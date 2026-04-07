import os
import traceback
from flask import current_app
from .. import db
from ..models import AudioInfo, Result, CetaceanInfo, Label

class InferenceService:
    @staticmethod
    def auto_label(upload_id, model_path, model_type='yolo', classes_str=''):
        """
        對 CetaceanInfo 進行自動標記。
        會更新 status 與 progress，支援前端動態監控。
        支援 YOLOv8, ResNet18, EfficientNet-B0
        """
        import torch
        
        # 解析使用者指定的類別
        user_specified_classes = []
        if classes_str and classes_str.strip():
            user_specified_classes = [int(c.strip()) for c in classes_str.split(',') if c.strip().isdigit()]
            if user_specified_classes:
                print(f"使用者指定的類別映射: {user_specified_classes}")
        
        if not os.path.exists(model_path): return
        
        audio_info = AudioInfo.query.get(upload_id)
        if not audio_info: return

        try:
            # 1. 更新狀態為 PROCESSING
            audio_info.status = 'PROCESSING'
            audio_info.progress = 0
            db.session.commit()

            # ---------------------------------------------------------
            # A. 模型初始化與載入
            # ---------------------------------------------------------
            model = None
            is_yolo = (model_type == 'yolo' or model_type.startswith('yolov8'))
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            cnn_labels_map = [] # Index -> ID

            if is_yolo:
                from ultralytics import YOLO
                try:
                    model = YOLO(model_path)
                except Exception as e:
                    print(f"YOLO 模型載入失敗: {e}。嘗試使用 PyTorch 載入...")
                    is_yolo = False # Fallback to generic PyTorch handling check

            if not is_yolo:
                # 載入 PyTorch 模型 (ResNet / EfficientNet)
                import torch.nn as nn
                from torchvision import transforms, models
                from PIL import Image as PILImage
                
                # 默認類別對應 (若無法從其他地方取得)
                all_labels = sorted(Label.query.all(), key=lambda x: x.name)
                cnn_labels_map = [l.id for l in all_labels]
                
                # 先載入 checkpoint 以確定 num_classes
                try:
                    checkpoint = torch.load(model_path, map_location=device)
                    state_dict = checkpoint
                    
                    # Checkpoint 格式判斷
                    if isinstance(checkpoint, dict) and 'classes' in checkpoint:
                        print("偵測到含 Metadata 的模型 checkpoint")
                        class_names = checkpoint['classes']
                        state_dict = checkpoint['model_state_dict']
                        
                        # 使用儲存的類別名稱來建立 Mapping
                        name_to_id = {l.name: l.id for l in Label.query.all()}
                        cnn_labels_map = [name_to_id.get(name, 0) for name in class_names]
                        num_classes = len(class_names)
                    
                    else:
                        # 舊版或是純 state_dict
                        # 嘗試從權重形狀推斷 num_classes
                        detected_classes = 0
                        if 'fc.weight' in state_dict: # ResNet
                            detected_classes = state_dict['fc.weight'].shape[0]
                        elif 'classifier.1.weight' in state_dict: # EfficientNet
                            detected_classes = state_dict['classifier.1.weight'].shape[0]
                        # 處理 module. 前綴的情況
                        elif 'module.fc.weight' in state_dict:
                            detected_classes = state_dict['module.fc.weight'].shape[0]
                        elif 'module.classifier.1.weight' in state_dict:
                            detected_classes = state_dict['module.classifier.1.weight'].shape[0]
                        
                        if detected_classes > 0:
                            print(f"從權重偵測到分類數量: {detected_classes}")
                            num_classes = detected_classes
                            
                            # 優先使用使用者指定的類別
                            if user_specified_classes and len(user_specified_classes) == num_classes:
                                cnn_labels_map = user_specified_classes
                                print(f"使用使用者指定的類別映射: {list(enumerate(cnn_labels_map))}")
                            elif user_specified_classes:
                                print(f"警告: 使用者指定的類別數量 ({len(user_specified_classes)}) 與模型輸出數 ({num_classes}) 不符")
                                if len(user_specified_classes) >= num_classes:
                                    cnn_labels_map = user_specified_classes[:num_classes]
                                else:
                                    cnn_labels_map = user_specified_classes
                                    for i in range(len(cnn_labels_map), num_classes):
                                        cnn_labels_map.append(0)
                                print(f"使用調整後的類別映射: {cnn_labels_map}")
                            else:
                                # 沒有使用者指定，嘗試從資料庫推斷
                                used_event_types_query = db.session.query(CetaceanInfo.event_type).filter(
                                    CetaceanInfo.event_type != 0,
                                    CetaceanInfo.event_type != None
                                ).distinct().all()
                                used_event_types = [str(et[0]) for et in used_event_types_query]
                                used_event_types_sorted = sorted(used_event_types)
                                
                                print(f"資料庫中已使用的 event_type: {used_event_types_sorted}")
                                
                                if len(used_event_types_sorted) == num_classes:
                                    cnn_labels_map = [int(et) for et in used_event_types_sorted]
                                    print(f"成功建立類別映射 (Index -> event_type): {list(enumerate(cnn_labels_map))}")
                                else:
                                    print(f"警告: 模型輸出數 ({num_classes}) 與資料庫 event_type 數 ({len(used_event_types_sorted)}) 不符。")
                                    print("請在表單中手動指定「訓練類別」(如：90,91)")
                                    if len(used_event_types_sorted) >= num_classes:
                                        cnn_labels_map = [int(et) for et in used_event_types_sorted[:num_classes]]
                                        print(f"使用前 {num_classes} 個 event_type 作為映射: {cnn_labels_map}")
                                    elif used_event_types_sorted:
                                        cnn_labels_map = [int(et) for et in used_event_types_sorted]
                                        for i in range(len(cnn_labels_map), num_classes):
                                            cnn_labels_map.append(0)
                                        print(f"警告: 使用部分 event_type 並補 0: {cnn_labels_map}")
                                    else:
                                        print("警告: 無法確定類別映射，預測結果可能不正確")
                                        cnn_labels_map = list(range(num_classes))
                        else:
                            # 無法偵測，使用 DB 數量
                            num_classes = len(cnn_labels_map)
                            if num_classes == 0:
                                print("警告: 無法決定分類數量，預設為 2 以避免崩潰")
                                num_classes = 2
                                cnn_labels_map = [90, 91]

                    # 初始化架構
                    if model_type == 'resnet18':
                        model = models.resnet18(weights=None)
                        num_ftrs = model.fc.in_features
                        model.fc = nn.Linear(num_ftrs, num_classes)
                    elif model_type == 'efficientnet_b0':
                        model = models.efficientnet_b0(weights=None)
                        num_ftrs = model.classifier[1].in_features
                        model.classifier[1] = nn.Linear(num_ftrs, num_classes)
                    else:
                        model = models.resnet18(weights=None)
                        num_ftrs = model.fc.in_features
                        model.fc = nn.Linear(num_ftrs, num_classes)
                    
                    # 載入權重
                    new_state_dict = {}
                    for k, v in state_dict.items():
                        if k.startswith('module.'):
                            new_state_dict[k[7:]] = v
                        else:
                            new_state_dict[k] = v
                            
                    model.load_state_dict(new_state_dict)
                    model.to(device)
                    model.eval()
                    
                    # 定義預處理
                    val_transforms = transforms.Compose([
                        transforms.Resize((224, 224)),
                        transforms.ToTensor(),
                        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
                    ])
                    
                except Exception as e:
                    print(f"PyTorch 模型載入失敗: {e}")
                    raise e

            # ---------------------------------------------------------
            # B. 執行推論
            # ---------------------------------------------------------
            
            # 準備 Label Mapping (Class Name -> ID) for YOLO
            all_labels_obj_map = {label.name: label.id for label in Label.query.all()}

            results_list = Result.query.filter_by(upload_id=upload_id).order_by(Result.id).all()
            cetaceans_list = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id).all()
            
            total_items = len(results_list)
            count = 0

            # 逐一預測
            for i, (res_item, cetacean_item) in enumerate(zip(results_list, cetaceans_list)):
                
                # 定期更新進度
                if total_items > 0 and i % max(1, int(total_items * 0.05)) == 0:
                    current_prog = int((i / total_items) * 100)
                    if current_prog != audio_info.progress:
                        audio_info.progress = current_prog
                        db.session.commit()

                image_path = os.path.join(current_app.root_path, 'static', res_item.audio_info.result_path, res_item.spectrogram_training_filename)
                
                if not os.path.exists(image_path): continue
                
                predicted_id = 0
                
                try:
                    if is_yolo:
                        preds = model(image_path, verbose=False)
                        if preds and preds[0].probs:
                            top1_index = preds[0].probs.top1
                            label_name = model.names[top1_index]
                            
                            if label_name in all_labels_obj_map:
                                predicted_id = all_labels_obj_map[label_name]
                            elif str(label_name).isdigit():
                                predicted_id = int(label_name)
                    else:
                        img = PILImage.open(image_path).convert('RGB')
                        img_t = val_transforms(img).unsqueeze(0).to(device)
                        
                        with torch.no_grad():
                            outputs = model(img_t)
                            _, predicted = torch.max(outputs, 1)
                            pred_idx = predicted.item()
                            
                            if pred_idx < len(cnn_labels_map):
                                predicted_id = cnn_labels_map[pred_idx]
                
                    # 寫入標記
                    if predicted_id != 0:
                        cetacean_item.event_type = predicted_id
                        cetacean_item.detect_type = 1 # 標記為 AI 辨識
                        count += 1
                        
                except Exception as e:
                    print(f"預測錯誤 (Index {i}): {e}")
                    continue

            # 完成後更新狀態
            audio_info.progress = 100
            audio_info.status = 'COMPLETED'
            db.session.commit()
            
            print(f"自動標記完成，更新了 {count} 筆資料。")

        except Exception as e:
            print(f"自動標記任務失敗: {e}")
            audio_info.status = 'COMPLETED'
            db.session.commit()
            traceback.print_exc()

        finally:
            # 清理暫存模型檔
            if os.path.exists(model_path):
                try:
                    os.remove(model_path)
                    parent = os.path.dirname(model_path)
                    if not os.listdir(parent):
                        os.rmdir(parent)
                except Exception:
                    pass  # 静默忽略目錄清理錯誤

    @staticmethod
    def auto_label_v2(upload_id, model_path, model_type='yolov8n-cls', classes_list=None):
        """
        對 CetaceanInfo 進行自動標記 (V2)。
        使用從 TrainingRun 取得的已知類別資訊，無需猜測映射。
        """
        import torch
        
        if not os.path.exists(model_path):
            print(f"模型檔案不存在: {model_path}")
            return
        
        audio_info = AudioInfo.query.get(upload_id)
        if not audio_info:
            print(f"找不到音檔記錄: {upload_id}")
            return

        try:
            # 更新狀態
            audio_info.status = 'PROCESSING'
            audio_info.progress = 0
            db.session.commit()
            
            # 建立類別映射 (class_name -> event_type ID)
            classes_list = classes_list or []
            cnn_labels_map = []
            for name in classes_list:
                try:
                    cnn_labels_map.append(int(name))
                except ValueError:
                    cnn_labels_map.append(0)
            
            print(f"類別映射 (Index -> event_type): {list(enumerate(cnn_labels_map))}")
            
            is_yolo = model_type.startswith('yolov8')
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model = None
            val_transforms = None
            
            if is_yolo:
                from ultralytics import YOLO
                model = YOLO(model_path)
            else:
                import torch.nn as nn
                from torchvision import transforms, models
                from PIL import Image as PILImage
                
                num_classes = len(cnn_labels_map) if cnn_labels_map else 2
                
                checkpoint = torch.load(model_path, map_location=device)
                state_dict = checkpoint
                
                if isinstance(checkpoint, dict) and 'model_state_dict' in checkpoint:
                    state_dict = checkpoint['model_state_dict']
                
                # 初始化模型
                if model_type == 'resnet18':
                    model = models.resnet18(weights=None)
                    model.fc = nn.Linear(model.fc.in_features, num_classes)
                elif model_type == 'efficientnet_b0':
                    model = models.efficientnet_b0(weights=None)
                    model.classifier[1] = nn.Linear(model.classifier[1].in_features, num_classes)
                else:
                    model = models.resnet18(weights=None)
                    model.fc = nn.Linear(model.fc.in_features, num_classes)
                
                new_state_dict = {}
                for k, v in state_dict.items():
                    new_state_dict[k[7:] if k.startswith('module.') else k] = v
                model.load_state_dict(new_state_dict)
                model.to(device)
                model.eval()
                
                val_transforms = transforms.Compose([
                    transforms.Resize((224, 224)),
                    transforms.ToTensor(),
                    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
                ])
            
            # 執行推論
            results_list = Result.query.filter_by(upload_id=upload_id).order_by(Result.id).all()
            cetaceans_list = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id).all()
            
            total_items = len(results_list)
            count = 0

            for i, (res_item, cetacean_item) in enumerate(zip(results_list, cetaceans_list)):
                if total_items > 0 and i % max(1, int(total_items * 0.05)) == 0:
                    audio_info.progress = int((i / total_items) * 100)
                    db.session.commit()

                image_path = os.path.join(
                    current_app.root_path, 'static', 
                    res_item.audio_info.result_path, 
                    res_item.spectrogram_training_filename
                )
                
                if not os.path.exists(image_path):
                    continue
                
                predicted_id = 0
                
                try:
                    if is_yolo:
                        preds = model(image_path, verbose=False)
                        if preds and preds[0].probs:
                            top1_index = preds[0].probs.top1
                            label_name = model.names[top1_index]
                            try:
                                predicted_id = int(label_name)
                            except ValueError:
                                predicted_id = 0
                    else:
                        from PIL import Image as PILImage
                        img = PILImage.open(image_path).convert('RGB')
                        img_t = val_transforms(img).unsqueeze(0).to(device)
                        
                        with torch.no_grad():
                            outputs = model(img_t)
                            _, predicted = torch.max(outputs, 1)
                            pred_idx = predicted.item()
                            
                            if pred_idx < len(cnn_labels_map):
                                predicted_id = cnn_labels_map[pred_idx]
                    
                    if predicted_id != 0:
                        cetacean_item.event_type = predicted_id
                        cetacean_item.detect_type = 1
                        count += 1
                        
                except Exception as e:
                    print(f"預測錯誤 (Index {i}): {e}")
                    continue

            audio_info.progress = 100
            audio_info.status = 'COMPLETED'
            db.session.commit()
            
            print(f"自動標記 V2 完成，更新了 {count} 筆資料。")

        except Exception as e:
            print(f"自動標記任務 V2 失敗: {e}")
            audio_info.status = 'COMPLETED'
            db.session.commit()
            traceback.print_exc()
