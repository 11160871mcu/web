import os
import json
from flask import current_app
from .. import db
from ..models import AudioInfo, Result, CetaceanInfo
from ..audio_utils import process_large_audio

class AudioService:
    @staticmethod
    def process_audio(audio_id):
        """
        處理上傳的音訊檔案，切割成片段並產生頻譜圖。
        由背景任務 (Celery) 呼叫。
        """
        # 重要：確保資料庫連接是最新的，避免 fork 進程共享連接問題
        db.session.remove()
        db.engine.dispose()
        
        try:
            # 使用原生 SQL 查詢必要資料，避免 ORM session 問題
            result = db.session.execute(
                db.text("SELECT file_path, result_path, params, fs FROM audio_info WHERE id = :id"),
                {"id": audio_id}
            ).fetchone()
            
            if not result:
                return
            
            upload_path = result[0]
            result_path = result[1]
            params_json = result[2]
            audio_fs = result[3]
            
            # 更新狀態為處理中
            db.session.execute(
                db.text("UPDATE audio_info SET status = 'PROCESSING', progress = 0 WHERE id = :id"),
                {"id": audio_id}
            )
            db.session.commit()

            result_dir = os.path.join(current_app.root_path, 'static', result_path)
            os.makedirs(result_dir, exist_ok=True)
            
            params = json.loads(params_json) if params_json else {}
            
            # 追蹤上次更新的進度，減少資料庫寫入頻率
            last_updated_progress = [0]

            def progress_callback(processed_count, total_count):
                """更新進度 - 使用原生 SQL 避免 session 衝突"""
                if total_count > 0:
                    progress = int((processed_count / total_count) * 100)
                    # 只在進度變化超過 10% 時才更新資料庫
                    if progress - last_updated_progress[0] >= 10 or progress == 100:
                        try:
                            db.session.execute(
                                db.text("UPDATE audio_info SET progress = :progress WHERE id = :id"),
                                {"progress": progress, "id": audio_id}
                            )
                            db.session.commit()
                            last_updated_progress[0] = progress
                        except Exception as e:
                            db.session.rollback()
                            print(f"進度更新失敗 (ID: {audio_id}): {e}")

            # 組裝頻譜圖參數字典
            n_fft = int(params.get('n_fft', 1024))
            window_overlap = float(params.get('window_overlap', 50)) / 100.0
            hop_length = int(n_fft * (1 - window_overlap))
            if hop_length < 1:
                hop_length = 1
            
            spec_params = {
                'n_fft': n_fft,
                'hop_length': hop_length,
                'window_overlap': window_overlap,
                'window_type': params.get('window_type', 'hann'),
                'n_mels': int(params.get('n_mels', 128)),
                'f_min': float(params.get('f_min', 0)),
                'f_max': float(params.get('f_max', 0)),
                'power': float(params.get('power', 2.0))
            }
            
            results_data = process_large_audio(
                filepath=upload_path,
                result_dir=result_dir,
                spec_type=params.get('spec_type', 'mel'),
                segment_duration=float(params.get('segment_duration', 2.0)),
                overlap_ratio=float(params.get('overlap', 50)) / 100.0,
                target_sr=int(params['sample_rate']) if params.get('sample_rate', 'None').isdigit() else None,
                is_mono=(params.get('channels', 'mono') == 'mono'),
                progress_callback=progress_callback,
                spec_params=spec_params
            )

            # 計算時間參數
            segment_duration = float(params.get('segment_duration', 2.0))
            overlap_ratio = float(params.get('overlap', 50)) / 100.0
            
            try:
                target_sr = int(params.get('sample_rate'))
            except (ValueError, TypeError):
                target_sr = audio_fs if audio_fs else 44100

            frame_length_samples = int(segment_duration * target_sr)
            hop_length_samples = int(frame_length_samples * (1 - overlap_ratio))

            # 雙表寫入迴圈
            for i, res_item in enumerate(results_data):
                new_result = Result(
                    upload_id=audio_id,
                    audio_filename=res_item['audio'],
                    spectrogram_filename=res_item['display_spectrogram'],
                    spectrogram_training_filename=res_item['training_spectrogram']
                )
                db.session.add(new_result)

                start_sample = i * hop_length_samples
                end_sample = start_sample + frame_length_samples
                
                new_cetacean = CetaceanInfo(
                    audio_id=audio_id,
                    start_sample=start_sample,
                    end_sample=end_sample,
                    event_duration=segment_duration,
                    event_type=0,
                    detect_type=2
                )
                db.session.add(new_cetacean)
            
            # 最終更新狀態
            db.session.execute(
                db.text("UPDATE audio_info SET status = 'COMPLETED', progress = 100 WHERE id = :id"),
                {"id": audio_id}
            )
            db.session.commit()

        except Exception as e:
            print(f"音訊處理任務 {audio_id} 失敗: {e}")
            db.session.rollback()
            try:
                db.session.execute(
                    db.text("UPDATE audio_info SET status = 'FAILED' WHERE id = :id"),
                    {"id": audio_id}
                )
                db.session.commit()
            except Exception as rollback_error:
                print(f"狀態更新失敗: {rollback_error}")
                db.session.rollback()
            raise
        finally:
            # 任務結束時清理 session
            db.session.remove()
