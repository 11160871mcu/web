import os
import json
import shutil
import pandas as pd
from flask import request, redirect, url_for, current_app, jsonify
from werkzeug.utils import secure_filename
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
    """ 
    匯入 Excel/CSV 標記資料 (終極完美版 - 完整檔名精確比對)
    邏輯：
    1. 支援 .xlsx, .xls, .csv 檔案。
    2. 針對資料庫檔名，精準剝離前兩個底線 (例如 5_5_)，拿剩下的真實檔名去配對。
    3. 空白、NaN 或 0 直接跳過，不改動資料庫。
    4. 加入嚴格的 try-except 防卡死回傳機制。
    """
    files = request.files.getlist('files')
    if not files:
        return jsonify({'success': False, 'error': '沒有選擇檔案'}), 400

    LABEL_TO_EVENT_TYPE = {
        'whale': 1, 'unknown': 0,
        'whale_upsweep': 10, 'whale_downsweep': 11, 'whale_concave': 12,
        'whale_convex': 13, 'whale_sine': 14, 'whale_click': 15,
        'whale_burst': 16, 'whale_constant': 17,
        'noise': 90, 'ship': 91, 'piling': 92
    }

    def get_priority(etype):
        if etype is None: return 999
        if 1 <= etype <= 17: return 1  
        if etype == 0: return 2        
        if etype >= 90: return 10      
        return 5

    excel_rows_success = 0
    db_slice_updated = 0
    errors = []

    for file in files:
        # 1. 支援讀取 CSV 與 Excel
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file)
            else:
                continue 
        except Exception as e:
            errors.append(f"讀取檔案失敗 {file.filename}: {str(e)}")
            continue

        try:
            df.columns = [str(c).strip().lower() for c in df.columns]
            
            if 'filename' not in df.columns:
                return jsonify({'success': False, 'error': f"檔案 {file.filename} 缺少 filename 欄位"}), 400

            pending_updates = {} 
            grouped = df.groupby('filename', sort=False)

            # 預先撈出所有資料庫音檔，避免迴圈內重複查詢 (提升效能且最準確)
            potential_audios = AudioInfo.query.all()

            for raw_filename, group in grouped:
                # 取得 CSV 裡的「完整檔名」
                csv_filename = str(raw_filename).strip()
                if not csv_filename or csv_filename.lower() == 'nan':
                    continue
                
                target_audios = []
                for audio in potential_audios:
                    db_filename = audio.file_name
                    
                    # 2. 精準掠過資料庫檔名的前兩個底線 (例如 5_5_SM35955...wav)
                    first_underscore = db_filename.find('_')
                    if first_underscore != -1:
                        second_underscore = db_filename.find('_', first_underscore + 1)
                        if second_underscore != -1:
                            real_name_suffix = db_filename[second_underscore + 1:]
                        else:
                            real_name_suffix = db_filename
                    else:
                        real_name_suffix = db_filename

                    # 3. 比對檔名 (因為你說 CSV 是完整的，所以 == 或是 in 都能精準命中)
                    if csv_filename.lower() == real_name_suffix.lower() or csv_filename.lower() in db_filename.lower():
                        target_audios.append(audio)

                if not target_audios:
                    errors.append(f"找不到對應的音檔: {csv_filename}")
                    continue

                slice_idx = 0  
                for _, row in group.iterrows():
                    raw_label = row.get('label', None)

                    # 檢查空白 (NaN)
                    if pd.isna(raw_label) or str(raw_label).strip() == '':
                        slice_idx += 1
                        continue
                    
                    # 檢查 0
                    try:
                        if float(raw_label) == 0:
                            slice_idx += 1
                            continue
                    except (ValueError, TypeError):
                        pass 

                    # 解析標籤
                    event_type = None
                    label_text = str(raw_label).strip().lower()
                    
                    if label_text in LABEL_TO_EVENT_TYPE:
                        event_type = LABEL_TO_EVENT_TYPE[label_text]
                    else:
                        try:
                             val = int(float(label_text))
                             if val != 0:
                                 event_type = val
                        except:
                             event_type = None 

                    # 若標籤無效，直接跳過
                    if event_type is None:
                        slice_idx += 1
                        continue

                    # 紀錄有意義的更新
                    excel_rows_success += 1
                    for target_audio in target_audios:
                        update_key = (target_audio.id, slice_idx)
                        if update_key not in pending_updates:
                            pending_updates[update_key] = event_type
                        else:
                            if get_priority(event_type) < get_priority(pending_updates[update_key]):
                                pending_updates[update_key] = event_type

                    slice_idx += 1

            # 4. 寫入資料庫 (只更新有標籤的部分，空白處與 0 維持原狀)
            for (aid, idx), final_type in pending_updates.items():
                target_record = CetaceanInfo.query.filter_by(audio_id=aid).order_by(CetaceanInfo.id).offset(idx).first()
                if target_record:
                    target_record.event_type = final_type
                    target_record.detect_type = 0
                    db_slice_updated += 1

            db.session.commit()

        except Exception as e:
            db.session.rollback()
            return jsonify({'success': False, 'error': f"處理出錯: {str(e)}"}), 500

    return jsonify({
        'success': True, 
        'success_count': excel_rows_success, 
        'db_updated': db_slice_updated,
        'errors': errors
    })