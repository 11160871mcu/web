import os
import io
import csv
import zipfile
import soundfile as sf
from flask import send_file, current_app
from werkzeug.utils import secure_filename
from ..main_router import main_bp

from ..models import AudioInfo, Result, CetaceanInfo, Label, BBoxAnnotation

@main_bp.route('/download_dataset_zip/<int:upload_id>')
def download_dataset_zip(upload_id):
    """打包下載：結合 Result (檔案) 與 CetaceanInfo (標籤)。"""
    upload = AudioInfo.query.get_or_404(upload_id)
    
    results_all = Result.query.filter_by(upload_id=upload_id).order_by(Result.id).all()
    cetaceans_all = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id).all()
    
    folder_path = os.path.join(current_app.root_path, 'static', upload.result_path)
    if not os.path.exists(folder_path):
        return "找不到結果資料夾，無法下載。", 404

    params = upload.get_params()
    try:
        segment_duration = float(params.get('segment_duration', 2.0))
        overlap_percent = float(params.get('overlap', 50))
        hop_length = segment_duration * (1 - overlap_percent / 100.0)
    except Exception as e:
        print(f"參數解析錯誤: {e}")
        segment_duration = 2.0
        hop_length = 1.0

    def format_time(seconds):
        m = int(seconds // 60)
        s = seconds % 60
        return f"{m:02d}:{s:06.3f}"

    memory_file = io.BytesIO()
    try:
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            # A. 加入實體檔案 
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    filename_lower = file.lower()
                    arcname = file
                    should_include = False
                    
                    if filename_lower.endswith(('.wav', '.mp3')):
                        arcname = f"audio/{file}"
                        should_include = True
                    elif filename_lower.endswith(('.png', '.jpg')) and '_spec_training_' in filename_lower:
                        arcname = f"images/{file}"
                        should_include = True
                    
                    if should_include:
                        zf.write(file_path, arcname)
            
            # B. 生成 labels.csv 
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerow(['filename', 'event_type', 'label_name', 'time_segment', 'detect_type'])
            
            label_map = {l.id: l.name for l in Label.query.all()}
            
            for i, (res, cet) in enumerate(zip(results_all, cetaceans_all)):
                fname = res.spectrogram_training_filename
                csv_filename = f"images/{fname}"
                etype = cet.event_type
                label_name = label_map.get(etype, 'Unknown') if etype else 'Unknown'
                
                start_seconds = i * hop_length
                end_seconds = start_seconds + segment_duration
                time_str = f"{format_time(start_seconds)} - {format_time(end_seconds)}"
                
                csv_writer.writerow([
                    csv_filename, etype, label_name, time_str, 
                    'AI' if cet.detect_type == 1 else 'Manual'
                ])
            
            zf.writestr('labels.csv', csv_buffer.getvalue())

            # C. 生成 bbox_annotations.csv (框選標記資料)
            bbox_csv_buffer = io.StringIO()
            bbox_csv_writer = csv.writer(bbox_csv_buffer)
            bbox_csv_writer.writerow([
                'filename', 'segment_index', 'label', 
                'time_start_sec', 'time_end_sec', 
                'freq_min_hz', 'freq_max_hz'
            ])
            
            # 取得頻譜圖參數以計算頻率軸
            try:
                sample_rate = params.get('sample_rate', 'None')
                if sample_rate == 'None' or sample_rate is None:
                    # 如果沒有指定取樣率，嘗試從第一個音訊檔讀取
                    first_audio = results_all[0] if results_all else None
                    if first_audio and first_audio.audio_filename:
                        first_audio_path = os.path.join(folder_path, first_audio.audio_filename)
                        if os.path.exists(first_audio_path):
                            info = sf.info(first_audio_path)
                            sample_rate = info.samplerate
                        else:
                            sample_rate = 44100  # 預設值
                    else:
                        sample_rate = 44100
                else:
                    sample_rate = float(sample_rate)
                
                f_min = float(params.get('f_min', 0))
                f_max = float(params.get('f_max', 0))
                
                # 如果 f_max 為 0，使用 Nyquist 頻率
                if f_max <= 0:
                    f_max = sample_rate / 2
            except Exception as e:
                print(f"頻率參數解析錯誤: {e}")
                sample_rate = 44100
                f_min = 0
                f_max = sample_rate / 2
            
            # 查詢所有框選標記
            bbox_count = 0
            for i, res in enumerate(results_all):
                annotations = BBoxAnnotation.query.filter_by(result_id=res.id).all()
                
                if annotations:
                    fname = res.spectrogram_training_filename
                    csv_filename = f"images/{fname}"
                    segment_start_time = i * hop_length
                    
                    for bbox in annotations:
                        # 計算時間軸（秒）
                        time_start = segment_start_time + (bbox.x * segment_duration)
                        time_end = segment_start_time + ((bbox.x + bbox.width) * segment_duration)
                        
                        # 計算頻率軸（Hz）
                        # 注意：Y 軸需要反轉（Y=0 是圖片頂部=高頻，Y=1 是圖片底部=低頻）
                        freq_max_bbox = f_min + (1 - bbox.y) * (f_max - f_min)
                        freq_min_bbox = f_min + (1 - (bbox.y + bbox.height)) * (f_max - f_min)
                        
                        bbox_csv_writer.writerow([
                            csv_filename,
                            i,
                            bbox.label,
                            f"{time_start:.3f}",
                            f"{time_end:.3f}",
                            f"{freq_min_bbox:.2f}",
                            f"{freq_max_bbox:.2f}"
                        ])
                        bbox_count += 1
            
            # 只有在有框選標記時才加入檔案
            if bbox_count > 0:
                zf.writestr('bbox_annotations.csv', bbox_csv_buffer.getvalue())

        memory_file.seek(0)
        download_filename = f"dataset_{upload.id}_{secure_filename(upload.file_name)}.zip"
        return send_file(memory_file, mimetype='application/zip', as_attachment=True, download_name=download_filename)

    except Exception as e:
        print(f"打包 ZIP 時發生錯誤: {e}")
        return f"打包失敗: {e}", 500
