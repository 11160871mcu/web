import os
import json
import shutil
import re
from flask import request, redirect, url_for, current_app, jsonify
from werkzeug.utils import secure_filename
import pandas as pd
from ..main_router import main_bp
from .. import db, celery
from ..models import AudioInfo, PointInfo, CetaceanInfo

@main_bp.route('/upload', methods=['POST'])
def upload():
    """Web 上傳介面 - 支援多檔案上傳"""
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return redirect(url_for('main.index'))
    try:
        params_dict = {
            'spec_type': request.form['spec_type'],
            'segment_duration': float(request.form['segment_duration']),
            'overlap': float(request.form['overlap']),
            'sample_rate': request.form.get('sample_rate', 'None'),
            'channels': request.form.get('channels', 'mono'),
            'n_fft': int(request.form.get('n_fft', 1024)),
            'window_overlap': float(request.form.get('window_overlap', 50)),
            'window_type': request.form.get('window_type', 'hann'),
            'n_mels': int(request.form.get('n_mels', 128)),
            'f_min': float(request.form.get('f_min', 0)),
            'f_max': float(request.form.get('f_max', 0)),
            'power': float(request.form.get('power', 2.0))
        }
    except Exception as e:
        print(f"上傳參數解析錯誤: {e}")
        return "參數錯誤", 400
        
    params_json = json.dumps(params_dict)
    default_point = PointInfo.query.first()
    point_id = default_point.id if default_point else None
    uploaded_ids = []
    
    for file in files:
        if file and file.filename != '':
            filename = secure_filename(file.filename)
            file_ext = os.path.splitext(filename)[1].lower().replace('.', '')
            new_audio = AudioInfo(
                file_name=filename,
                file_path="pending",
                file_type=file_ext,
                result_path="pending",
                params=params_json,
                status='PENDING',
                point_id=point_id
            )
            db.session.add(new_audio)
            db.session.commit()
            
            upload_id = new_audio.id
            result_dir_relative = os.path.join('results', str(upload_id))
            result_dir_absolute = os.path.join(current_app.root_path, 'static', result_dir_relative)
            os.makedirs(result_dir_absolute, exist_ok=True)
            
            upload_filename = f"{upload_id}_{filename}"
            upload_path_absolute = os.path.join(current_app.root_path, current_app.config['UPLOAD_FOLDER'], upload_filename)
            file.save(upload_path_absolute)
            
            new_audio.file_path = upload_path_absolute
            new_audio.result_path = result_dir_relative
            db.session.commit()
            celery.send_task('app.tasks.process_audio_task', args=[upload_id])
            uploaded_ids.append(upload_id)
            
    if uploaded_ids:
        return redirect(url_for('main.history', new_upload_id=uploaded_ids[0]))
    return redirect(url_for('main.index'))

@main_bp.route('/history/delete_selected', methods=['POST'])
def delete_selected_uploads():
    """批次刪除分析紀錄"""
    upload_ids = request.form.getlist('upload_ids')
    if not upload_ids:
        return redirect(url_for('main.history'))
    uploads = AudioInfo.query.filter(AudioInfo.id.in_(upload_ids)).all()
    for u in uploads:
        path = os.path.join(current_app.root_path, 'static', u.result_path)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        if u.file_path and os.path.exists(u.file_path):
            os.remove(u.file_path)
        db.session.delete(u)
    db.session.commit()
    return redirect(url_for('main.history'))

@main_bp.route('/api/import_excel', methods=['POST'])
def import_excel():
    """ 匯入 Excel 標記資料 (Emily 專用：精準時間合併版) """
    import re
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': '沒有選擇檔案'}), 400

    # --- 1. 標籤對照表 (請根據你的系統調整數字) ---
    LABEL_TO_EVENT_TYPE = {
        'whale': 1, 'unknown': 0,
        'whale_upsweep': 10, 'whale_downsweep': 11, 'whale_concave': 12,
        'whale_convex': 13, 'whale_sine': 14, 'whale_click': 15,
        'whale_burst': 16, 'whale_constant': 17,
        'noise': 90, 'ship': 91, 'piling': 92
    }

    # --- 2. 優先權函數 (防止互蓋) ---
    def get_priority(etype):
        if etype is None: return 999
        if 1 <= etype <= 17: return 1  # 鯨豚最優先
        if etype == 0: return 2        # unknown 次之
        if etype >= 90: return 10      # 噪音最後
        return 5

    excel_rows_success = 0
    db_slice_updated = 0
    errors = []

    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls')):
            continue

        try:
            df = pd.read_excel(file)
            # 確保欄位名稱正確 (A~D 對應到 start_time, end_time, label, filename)
            df.columns = [str(c).strip().lower() for c in df.columns]
            
            pending_updates = {} # 暫存所有要更新的標籤
            touched_audio_ids = set() # 記錄這次改了哪些音檔

            for index, row in df.iterrows():
                # --- 3. 解析 Excel A~D 欄 ---
                # A欄: start_time (相對時間)
                # D欄: filename (完整檔名)
                try:
                    excel_start_time = float(row['start_time'])
                except:
                    continue

                raw_filename = str(row.get('filename', '')).strip()
                if not raw_filename or raw_filename.lower() == 'nan': 
                    continue

                # --- 4. 🔑 核心修正：解析檔名裡的「絕對時間」 ---
                # 使用正規表示法，從 D欄 檔名中挖出 "ID_時間" 的結構
                # 例如: 6826.230311030000_2888.3427 -> 抓出 ID 和 2888.3427
                match = re.search(r'^(.+?)_(\d+(?:\.\d+)?)', raw_filename)
                if not match:
                    errors.append(f"無法解析檔名時間: {raw_filename}")
                    continue
                    
                core_id = match.group(1) # 純 ID 部分
                filename_absolute_time = float(match.group(2)) # 檔名後面的秒數

                # --- 5. 🚀 關鍵計算：真實時間 = 檔名時間 + Excel時間 ---
                # 這就是為什麼之前 unknown 會消失的原因！
                true_absolute_time = filename_absolute_time + excel_start_time

                # --- 6. 尋找對應的音檔 ---
                # 在資料庫裡找包含這個 ID 的檔案
                target_audios = AudioInfo.query.filter(AudioInfo.file_name.ilike(f"%{core_id}%")).all()
                if not target_audios:
                    errors.append(f"找不到音檔: {raw_filename}")
                    continue

                # --- 7. 解析標籤 (C欄) ---
                # 在讀取標籤的那一行加上 .strip()
                label = str(row['label']).strip()  # 這會自動砍掉前後多餘的空格
                raw_label = row.get('label', '')
                event_type = 90 # 預設為 noise
                
                if isinstance(raw_label, (int, float)):
                    event_type = int(raw_label)
                else:
                    label_text = str(raw_label).strip().lower()
                    if label_text.isdigit():
                        event_type = int(label_text)
                    else:
                        if 'constant' in label_text and 'whale' not in label_text: 
                            event_type = 17
                        elif 'unknown' in label_text: 
                            event_type = 0
                        else: 
                            event_type = LABEL_TO_EVENT_TYPE.get(label_text, 90)

                excel_rows_success += 1

                # --- 8. 計算正確的切片 Index ---
                for target_audio in target_audios:
                    touched_audio_ids.add(target_audio.id)
                    params = target_audio.get_params()
                    segment_duration = float(params.get('segment_duration', 2.0))
                    
                    # 用「真實時間」去除以切片長度
                    calc_idx = int(round(true_absolute_time / segment_duration))

                    update_key = (target_audio.id, calc_idx)
                    
                    # --- 9. 優先權防護：誰優先誰留下 ---
                    if update_key not in pending_updates:
                        pending_updates[update_key] = event_type
                    else:
                        if get_priority(event_type) < get_priority(pending_updates[update_key]):
                            pending_updates[update_key] = event_type

            # --- 10. 一鍵洗底色 (把這次用到的音檔背景先刷成 noise 90) ---
            if touched_audio_ids:
                CetaceanInfo.query.filter(CetaceanInfo.audio_id.in_(touched_audio_ids)).update(
                    {"event_type": 90, "detect_type": 0}, synchronize_session=False
                )
                db.session.flush()

            # --- 11. 蓋上正確標籤 ---
            for (aid, idx), final_type in pending_updates.items():
                target_record = CetaceanInfo.query.filter_by(audio_id=aid).order_by(CetaceanInfo.id).offset(idx).first()
                if target_record:
                    target_record.event_type = final_type
                    target_record.detect_type = 0
                    db_slice_updated += 1

            db.session.commit()

        except Exception as e:
            db.session.rollback()
            errors.append(f"處理出錯: {str(e)}")

    return jsonify({
        'success': True, 
        'success_count': excel_rows_success, 
        'db_updated': db_slice_updated,
        'errors': errors
    })