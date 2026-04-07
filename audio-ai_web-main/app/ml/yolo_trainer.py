import os
import shutil
import random
import json
import time
from datetime import datetime
from collections import defaultdict
import numpy as np

from ultralytics import YOLO
from flask import current_app
from .. import db
from ..models import Result, CetaceanInfo, Label, TrainingRun

class YoloTrainer:
    @staticmethod
    def train(upload_ids, training_run_id, model_name='yolov8n-cls.pt', train_params=None):
        """
        背景任務：使用已標記的資料來訓練 YOLOv8 分類模型。
        支援自訂訓練參數 (epochs, batch_size, learning_rate, image_size)
        """
        # 解析訓練參數
        if train_params is None:
            train_params = {}
        epochs = train_params.get('epochs', 50)
        batch_size = train_params.get('batch_size', 16)
        learning_rate = train_params.get('learning_rate', 0.001)
        image_size = train_params.get('image_size', 224)
        training_run = TrainingRun.query.get(training_run_id)
        if not training_run: return

        try:
            training_run.status = 'RUNNING'
            training_run.progress = 5
            db.session.commit()

            # 1. 準備路徑與資料
            base_dir = os.path.join(current_app.root_path, 'static', 'training_runs', str(training_run_id))
            dataset_dir = os.path.join(base_dir, 'dataset')
            
            # 找出有標記的 Result (透過 CetaceanInfo 的 event_type 映射，或者假設 Result 也有 label 關聯)
            # 為了訓練，我們需要將 CetaceanInfo 的標記對應到 Result 的圖片
            # 這裡採用更嚴謹的配對方式
            
            # 先撈取所有相關的 CetaceanInfo (有標記的)
            labeled_cetaceans = CetaceanInfo.query.filter(
                CetaceanInfo.audio_id.in_(upload_ids),
                CetaceanInfo.event_type != 0
            ).order_by(CetaceanInfo.audio_id, CetaceanInfo.id).all()

            if not labeled_cetaceans:
                raise ValueError("找不到任何已標記的資料來進行訓練。")

            # 建立對照表以加速查找 Result
            # Map: (audio_id, index) -> Result
            # 注意：這依賴於 Result 與 CetaceanInfo 的 ID 順序一致性
            results_map = defaultdict(list)
            all_results = Result.query.filter(Result.upload_id.in_(upload_ids)).order_by(Result.upload_id, Result.id).all()
            for res in all_results:
                results_map[res.upload_id].append(res)
            
            # 為了計算 Index，我們也需要該 audio 所有的 cetacean
            all_cetaceans_map = defaultdict(list)
            all_cetaceans = CetaceanInfo.query.filter(CetaceanInfo.audio_id.in_(upload_ids)).order_by(CetaceanInfo.audio_id, CetaceanInfo.id).all()
            for c in all_cetaceans:
                all_cetaceans_map[c.audio_id].append(c)

            # 準備 Label Mapping (ID -> Name)
            label_map = {l.id: l.name for l in Label.query.all()}

            data_by_label = defaultdict(list)
            
            for cetacean in labeled_cetaceans:
                aid = cetacean.audio_id
                try:
                    # 找出此 cetacean 在其 audio 序列中的 index
                    idx = all_cetaceans_map[aid].index(cetacean)
                    
                    if idx < len(results_map[aid]):
                        result_item = results_map[aid][idx]
                        
                        # 決定標籤名稱 (資料夾名稱)
                        # 優先使用 Label 表的名稱，若無則使用 ID
                        label_name = label_map.get(cetacean.event_type, str(cetacean.event_type))
                        
                        data_by_label[label_name].append(result_item)
                except (ValueError, IndexError):
                    continue

            total_val_images = 0
            
            # 建立資料集 (複製圖檔)
            for label_name, items in data_by_label.items():
                random.shuffle(items)
                if len(items) < 2:
                    train_items, val_items = items, []
                else:
                    split_index = int(len(items) * 0.8)
                    train_items, val_items = items[:split_index], items[split_index:]
                
                total_val_images += len(val_items)

                for item_list, target_dir in [(train_items, os.path.join(dataset_dir, 'train')), (val_items, os.path.join(dataset_dir, 'val'))]:
                    label_folder = os.path.join(target_dir, label_name)
                    os.makedirs(label_folder, exist_ok=True)
                    for item in item_list:
                        source_path = os.path.join(current_app.root_path, 'static', item.audio_info.result_path, item.spectrogram_training_filename)
                        if os.path.exists(source_path):
                            shutil.copy(source_path, label_folder)
            
            # 容錯：若無驗證集，複製訓練集充當
            if total_val_images == 0:
                src_train = os.path.join(dataset_dir, 'train')
                dst_val = os.path.join(dataset_dir, 'val')
                if os.path.exists(src_train):
                    if os.path.exists(dst_val): shutil.rmtree(dst_val)
                    shutil.copytree(src_train, dst_val)
            
            training_run.progress = 15
            db.session.commit()
            
            # 2. 開始訓練
            model = YOLO(model_name)
            
            def on_fit_epoch_end_callback(trainer):
                current_epoch = trainer.epoch + 1
                total_epochs = trainer.epochs
                progress = 15 + int((current_epoch / total_epochs) * 80)
                if progress > training_run.progress:
                    training_run.progress = progress
                    db.session.commit()

            model.add_callback("on_fit_epoch_end", on_fit_epoch_end_callback)
            
            # 執行訓練
            model.train(
                data=dataset_dir, 
                epochs=epochs, 
                batch=batch_size,
                lr0=learning_rate,
                imgsz=image_size,
                project=base_dir, 
                name='train_results',
                val=True
            )
            
            training_run.progress = 98
            db.session.commit()
            
            # --- 3. 取得詳細指標 ---
            accuracy_top1 = 0.0
            per_class_list = []
            
            train_results_dir = os.path.join(base_dir, 'train_results')
            best_model_path = os.path.join(train_results_dir, 'weights', 'best.pt')
            
            # 取得類別名稱 (從資料集目錄)
            train_dir = os.path.join(dataset_dir, 'train')
            val_dir = os.path.join(dataset_dir, 'val')
            class_names = []
            if os.path.exists(train_dir):
                class_names = sorted([d for d in os.listdir(train_dir) if os.path.isdir(os.path.join(train_dir, d))])
            
            print(f"[YOLO 訓練] 偵測到 {len(class_names)} 個類別: {class_names}")
            
            if os.path.exists(best_model_path):
                try:
                    # 載入最佳模型
                    val_model = YOLO(best_model_path)
                    
                    # 使用 val() 取得 Top-1 準確率
                    metrics = val_model.val(data=dataset_dir, verbose=False)
                    if hasattr(metrics, 'top1'):
                        accuracy_top1 = metrics.top1
                        print(f"[YOLO 訓練] Top-1 準確率: {accuracy_top1}")
                    
                    # 手動計算詳細指標：對驗證集進行預測
                    from sklearn.metrics import precision_recall_fscore_support, confusion_matrix as sklearn_cm
                    from PIL import Image as PILImage
                    import matplotlib.pyplot as plt
                    
                    all_preds = []
                    all_labels = []
                    
                    # 遍歷驗證集
                    if os.path.exists(val_dir):
                        for class_idx, class_name in enumerate(class_names):
                            class_dir = os.path.join(val_dir, class_name)
                            if os.path.exists(class_dir):
                                for img_file in os.listdir(class_dir):
                                    img_path = os.path.join(class_dir, img_file)
                                    if img_file.lower().endswith(('.jpg', '.png', '.jpeg')):
                                        try:
                                            # 進行預測
                                            result = val_model.predict(img_path, verbose=False)
                                            if result and len(result) > 0:
                                                pred_class = result[0].probs.top1
                                                all_preds.append(pred_class)
                                                all_labels.append(class_idx)
                                        except Exception as pred_e:
                                            print(f"[YOLO 訓練] 預測圖片失敗 {img_file}: {pred_e}")
                    
                    print(f"[YOLO 訓練] 完成 {len(all_preds)} 張驗證圖片預測")
                    
                    if all_preds and all_labels:
                        # 計算每類別指標
                        precision, recall, f1, support = precision_recall_fscore_support(
                            all_labels, all_preds, average=None, zero_division=0, labels=list(range(len(class_names)))
                        )
                        
                        for i, name in enumerate(class_names):
                            per_class_list.append({
                                'name': name,
                                'precision': round(float(precision[i]), 3),
                                'recall': round(float(recall[i]), 3),
                                'f1-score': round(float(f1[i]), 3),
                                'support': int(support[i])
                            })
                        
                        print(f"[YOLO 訓練] 成功計算 {len(per_class_list)} 個類別的詳細指標")
                        
                        # 生成混淆矩陣圖
                        cm = sklearn_cm(all_labels, all_preds, labels=list(range(len(class_names))))
                        fig_cm, ax_cm = plt.subplots(figsize=(8, 6))
                        
                        im = ax_cm.imshow(cm, interpolation='nearest', cmap='Blues')
                        fig_cm.colorbar(im, ax=ax_cm)
                        
                        ax_cm.set_xticks(np.arange(len(class_names)))
                        ax_cm.set_yticks(np.arange(len(class_names)))
                        ax_cm.set_xticklabels(class_names, rotation=45, ha='right')
                        ax_cm.set_yticklabels(class_names)
                        
                        thresh = cm.max() / 2.
                        for i in range(cm.shape[0]):
                            for j in range(cm.shape[1]):
                                ax_cm.text(j, i, format(cm[i, j], 'd'),
                                          ha="center", va="center",
                                          color="white" if cm[i, j] > thresh else "black")
                        
                        ax_cm.set_xlabel('Predicted')
                        ax_cm.set_ylabel('True')
                        ax_cm.set_title('Confusion Matrix (Manual)')
                        plt.tight_layout()
                        plt.savefig(os.path.join(train_results_dir, 'confusion_matrix_manual.png'))
                        plt.close()
                        
                except Exception as e:
                    print(f"[YOLO 訓練] 計算詳細指標時發生錯誤: {e}")
                    import traceback
                    traceback.print_exc()

            # 備案：如果 val() 失敗，嘗試從 CSV 讀取 Top-1
            if accuracy_top1 == 0.0:
                csv_path = os.path.join(train_results_dir, 'results.csv')
                if os.path.exists(csv_path):
                    try:
                        with open(csv_path, 'r') as f:
                            lines = f.readlines()
                            if len(lines) > 1:
                                last_line = lines[-1].strip()
                                keys = [k.strip() for k in lines[0].split(',')]
                                values = [float(v.strip()) for v in last_line.split(',')]
                                data_dict = dict(zip(keys, values))
                                if 'metrics/accuracy_top1' in data_dict:
                                    accuracy_top1 = data_dict['metrics/accuracy_top1']
                                    print(f"[YOLO 訓練] 從 CSV 讀取 Top-1: {accuracy_top1}")
                    except Exception as csv_e:
                        print(f"[YOLO 訓練] 讀取 CSV 失敗: {csv_e}")
            
            # 備案：如果沒有詳細指標，至少提供類別名稱
            if not per_class_list and class_names:
                print("[YOLO 訓練] 使用備案：僅提供類別名稱")
                per_class_list = [{'name': name, 'precision': 0, 'recall': 0, 'f1-score': 0} for name in class_names]

            # 5. 儲存
            now = datetime.now()
            duration_sec = (now - training_run.timestamp.replace(tzinfo=None)).total_seconds() if training_run.timestamp else 0
            metrics_dict = {
                'accuracy_top1': round(float(accuracy_top1), 4),
                'per_class_list': per_class_list,
                'end_time': now.strftime('%Y-%m-%d %H:%M:%S'),
                'duration_seconds': round(duration_sec, 1)
            }
            training_run.metrics = json.dumps(metrics_dict)
            
            # 6. 等待圖檔生成
            expected_images = ['results.png', 'confusion_matrix.png']
            for _ in range(5):
                if all(os.path.exists(os.path.join(train_results_dir, img)) for img in expected_images):
                    break
                time.sleep(1)

            training_run.results_path = os.path.join('training_runs', str(training_run_id), 'train_results')
            training_run.status = 'SUCCESS'
            training_run.progress = 100
            db.session.commit()
            print(f"--- [訓練任務 #{training_run_id}] 成功完成 ---")

        except Exception as e:
            print(f"!!! [訓練任務 #{training_run_id}] 失敗: {e}")
            if 'training_run' in locals() and training_run:
                training_run.status = 'FAILURE'
                training_run.progress = 100
                db.session.commit()
            raise
