import os
import json
import math
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

@main_bp.route('/batch_download_zip', methods=['POST'])
def batch_download_zip():
    """批次下載選取的專案 (壓縮為 ZIP)"""
    import zipfile
    import io
    import os
    from flask import send_file

    upload_ids = request.form.getlist('upload_ids')
    if not upload_ids:
        return redirect(url_for('main.history'))
    
    uploads = AudioInfo.query.filter(AudioInfo.id.in_(upload_ids)).all()
    if not uploads:
        return redirect(url_for('main.history'))

    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for upload in uploads:
            if upload.file_path and os.path.exists(upload.file_path):
                zf.write(upload.file_path, arcname=f"raw_audio/{upload.file_name}")
            
            if upload.result_path:
                result_dir_absolute = os.path.join(current_app.root_path, 'static', upload.result_path)
                if os.path.exists(result_dir_absolute):
                    for root, _, files in os.walk(result_dir_absolute):
                        for file in files:
                            file_path = os.path.join(root, file)
                            rel_path = os.path.relpath(file_path, result_dir_absolute)
                            arcname = f"results_{upload.id}/{rel_path}"
                            zf.write(file_path, arcname=arcname)
    
    memory_file.seek(0)
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name='batch_export.zip'
    )

@main_bp.route('/api/import_excel', methods=['POST'])
def import_excel():
    """
    匯入 Excel 標記資料

    【新格式說明】
    Excel (xlsx) 欄位格式：
        start_time  | end_time  | label         | filename
        132.9883    | 133.8762  | whale_upsweep | un5_6826.230311010000.wav

    - start_time / end_time：相對於該音檔起點的絕對秒數（非子片段偏移量）
    - filename：音檔完整名稱（含 .wav），直接對應資料庫 AudioInfo.file_name

    【修正說明】

    [Bug A] parse_filename 設計錯誤（已移除）：
        舊版 parse_filename() 假設 Excel 檔名中編碼了子片段偏移量
        （例如：un5_6826.230315150001_1873.2900_1），試圖從檔名尾端拆出 offset。
        新格式的 Excel 檔名是完整音檔名（un5_6826.230311010000.wav），
        時間欄位已是音檔內的絕對秒數，offset 永遠為 0。
        → 修正：移除 parse_filename()，改用 extract_core_id() 只負責去除
          副檔名與資料庫前綴（如 14_），取出核心 ID 用於比對，offset 固定為 0。

    [Bug B] 切片索引計算正確（保留）：
        step = segment_duration * (1 - overlap% / 100)
        slice_idx = abs_time // step
        此邏輯正確，不變動。

    [Bug C] reset 覆蓋手動標籤範圍過廣（已修正）：
        舊版在匯入前將所有 touched 音檔的全部切片 reset 為 90，
        導致 Excel 未覆蓋到的切片（如使用者手動在 UI 標記的）也被清除。
        → 修正：只 reset「本次 Excel 有對應切片索引」的 CetaceanInfo 紀錄，
          而非整個音檔的所有切片。

    [Bug D] 多批次匯入互相覆蓋（已保留修正）：
        pending_updates 與 touched_audio_ids 在所有 Excel 讀完後才執行寫入，
        避免多個 Excel 檔案的標籤互相覆蓋。

    [Bug E] 欄位名稱辨識邏輯（已強化）：
        新格式欄位固定為 start_time, end_time, label, filename，
        辨識邏輯更新以優先匹配這些名稱，並回退到模糊比對。

    [Bug F] 切片索引計算使用「時間點落點」而非「視窗覆蓋」（已修正）：
        舊版邏輯：
            start_idx = int(excel_start_time // step)
            end_idx   = int(excel_end_time   // step)
        這只計算「事件的起點/終點分別落在哪個切片」，
        會漏掉「視窗從左邊蓋住事件」的切片。
        例如 segment_duration=3, step=1.5, 事件在 18.48s–19.18s：
            舊版：切片 12–12（只算起點落在哪）
            正確：切片 11–12（切片11視窗 16.5–19.5s 也有覆蓋到事件）

        正確條件：切片 i 的視窗 [i*step, i*step+segment_duration] 與事件有交集
            ↔  i*step < event_end  AND  i*step + segment_duration > event_start
            → start_idx = max(0, ceil((event_start - segment_duration) / step + ε))
               end_idx   = int((event_end - ε) // step)
        → 修正：改用視窗覆蓋邏輯計算 start_idx 與 end_idx。
    """
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': '沒有選擇檔案'}), 400

    def get_priority(etype):
        if etype is None: return 999
        if 1 <= etype <= 17: return 1
        if etype == 0: return 2
        if etype >= 90: return 10
        return 5

    def extract_core_id(raw_filename):
        """
        從 Excel 的 filename 欄位取得核心 ID，用於比對資料庫 AudioInfo.file_name。

        新格式：Excel filename 就是完整音檔名，例如 un5_6826.230311010000.wav
        只需去除副檔名即可取得核心 ID。
        資料庫中音檔名可能帶前綴（如 14_un5_6826.230311010000.wav），
        使用 ilike('%core_id%') 進行模糊比對。

        回傳：core_id（去除副檔名後的字串）
        """
        # 去除副檔名（.wav / .flac / .mp3 等）
        stem, ext = os.path.splitext(raw_filename)
        if ext.lower() in ('.wav', '.flac', '.mp3', '.ogg', '.aiff', '.aif'):
            return stem
        # 若沒有已知副檔名，直接回傳原字串（相容舊格式）
        return raw_filename

    excel_rows_success = 0
    db_slice_updated = 0
    errors = []

    # pending_updates: {(audio_id, slice_idx): event_type}
    # touched_slice_keys: {(audio_id, slice_idx)} — 只 reset 本次有異動的切片
    pending_updates = {}
    touched_slice_keys = set()

    current_app.logger.info("--- 🚀 開始匯入 Excel 任務 ---")

    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
            continue
        try:
            # --- 1. 讀取與欄位標準化 ---
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            current_app.logger.info(f"📄 正在處理檔案: {file.filename} (共 {len(df)} 筆資料)")

            df.columns = [str(c).strip().lower().replace('\ufeff', '') for c in df.columns]

            # 欄位辨識：優先精確名稱，再模糊比對
            def find_col(df_cols, exact_names, fuzzy_keywords, default):
                for name in exact_names:
                    if name in df_cols:
                        return name
                return next((c for c in df_cols if any(k in c for k in fuzzy_keywords)), default)

            cols = df.columns.tolist()
            start_col = find_col(cols, ['start_time', 'start'],   ['start', 'begin', '起始'],  'start_time')
            end_col   = find_col(cols, ['end_time', 'end'],       ['end', '結束'],              'end_time')
            dur_col   = find_col(cols, ['duration'],              ['dur', 'delta', '持續'],     None)
            file_col  = find_col(cols, ['filename', 'file_name'], ['file', 'name', '檔'],       'filename')
            label_col = find_col(cols, ['label'],                 ['label', 'anno', 'type', '標'], 'label')

            for index, row in df.iterrows():
                # --- 2. 解析時間欄位（絕對秒數）---
                try:
                    excel_start_time = float(row[start_col])
                    if dur_col and dur_col in df.columns and pd.notna(row.get(dur_col)):
                        excel_end_time = excel_start_time + float(row[dur_col])
                    else:
                        excel_end_time = float(row.get(end_col, excel_start_time + 0.1))
                except Exception as e:
                    current_app.logger.warning(f"跳過第 {index + 1} 行：時間解析失敗 ({e})")
                    continue

                # --- 3. 取得核心 ID（新格式：直接從音檔名去除副檔名）---
                raw_filename = str(row.get(file_col, '')).strip()
                if not raw_filename or raw_filename.lower() == 'nan':
                    continue

                # [Bug A 修正] 不再嘗試從檔名拆解 offset；offset 固定為 0
                core_id = extract_core_id(raw_filename)

                # --- 4. 資料庫比對（忽略前綴如 14_ 或副檔名差異）---
                target_audios = AudioInfo.query.filter(
                    AudioInfo.file_name.ilike(f"%{core_id}%")
                ).all()

                if not target_audios:
                    current_app.logger.warning(
                        f"❌ [比對失敗] 找不到音檔! 核心ID: {core_id} (原始檔名: {raw_filename})"
                    )
                    continue

                # --- 5. 解析標籤 ---
                LABEL_TO_EVENT_TYPE = {
                    'whale': 1, 
                    'unknown': 0, 
                    'whale_unknown': 10,      # 新增：未知聲紋
                    'whale_upsweep': 11,      # 修改：10 -> 11
                    'whale_downsweep': 12,    # 修改：11 -> 12
                    'whale_concave': 13,      # 修改：12 -> 13
                    'whale_convex': 14,       # 修改：13 -> 14
                    'whale_sine': 15,         # 修改：14 -> 15
                    'whale_click': 16,        # 修改：15 -> 16
                    'whale_burst': 17,        # 修改：16 -> 17
                    'whale_constant': 18,     # 修改：17 -> 18
                    'noise': 90, 
                    'ship': 91, 
                    'piling': 92
                }
                raw_label = str(row.get(label_col, '')).strip().lower()
                if raw_label.isdigit():
                    event_type = int(raw_label)
                else:
                    event_type = LABEL_TO_EVENT_TYPE.get(raw_label, 90)

                excel_rows_success += 1

                # --- 6. 計算切片索引（offset 固定為 0，時間即絕對秒數）---
                for target_audio in target_audios:
                    try:
                        params = json.loads(target_audio.params) if target_audio.params else {}
                    except Exception:
                        params = {}

                    segment_duration = float(params.get('segment_duration', 3.0))
                    overlap_pct      = float(params.get('overlap', 50.0))
                    step             = segment_duration * (1.0 - overlap_pct / 100.0)
                    if step <= 0:
                        step = segment_duration

                    # [Bug A 修正] abs_time = 0 + excel_time（無子片段 offset）
                    # [Bug F 修正] 使用「視窗覆蓋」邏輯而非「時間點落點」邏輯
                    # 條件：切片視窗 [i*step, i*step+segment_duration] 與事件 [start, end] 有交集
                    #   即：i*step < end  AND  i*step + segment_duration > start
                    #   → i < end/step  AND  i > (start - segment_duration) / step
                    start_idx = max(0, math.ceil((excel_start_time - segment_duration) / step + 1e-9))
                    end_idx   = int((excel_end_time - 1e-9) // step)

                    current_app.logger.info(
                        f"✅ [配對成功] DB檔名: {target_audio.file_name} | "
                        f"start={excel_start_time:.3f}s end={excel_end_time:.3f}s | "
                        f"step={step}s | 切片: {start_idx}-{end_idx}"
                    )

                    for calc_idx in range(start_idx, end_idx + 1):
                        update_key = (target_audio.id, calc_idx)
                        # [Bug C 修正] 只記錄本次有異動的切片，不清整個音檔
                        touched_slice_keys.add(update_key)
                        if update_key not in pending_updates:
                            pending_updates[update_key] = event_type
                        else:
                            if get_priority(event_type) < get_priority(pending_updates[update_key]):
                                pending_updates[update_key] = event_type

        except Exception as e:
            current_app.logger.error(f"❌ 檔案處理出錯: {str(e)}")
            errors.append(f"檔案處理出錯: {str(e)}")

    # --- 7. 資料庫更新（所有 Excel 讀完後統一執行）---
    # [Bug D 保留] pending_updates 在所有檔案迴圈外，避免多 Excel 批次互相覆蓋
    # [Bug C 修正] 只 reset 本次有對應切片索引的 CetaceanInfo 記錄，
    #              不清除音檔中未被本次 Excel 覆蓋的手動標籤
    try:
        if pending_updates:
            # 依音檔 ID 分組，取得各音檔需要 reset 的切片索引集合
            audio_to_indices = {}
            for (aid, idx) in touched_slice_keys:
                audio_to_indices.setdefault(aid, set()).add(idx)

            # 對每個音檔，只 reset 本次有對應的切片
            for aid, indices in audio_to_indices.items():
                # 取得該音檔所有切片（依 id 排序）
                all_slices = (
                    CetaceanInfo.query
                    .filter_by(audio_id=aid)
                    .order_by(CetaceanInfo.id)
                    .all()
                )
                for idx in indices:
                    if idx < len(all_slices):
                        all_slices[idx].event_type = 90
                        all_slices[idx].detect_type = 0

            db.session.flush()

            # 寫入新標籤
            for (aid, idx), final_type in pending_updates.items():
                all_slices = (
                    CetaceanInfo.query
                    .filter_by(audio_id=aid)
                    .order_by(CetaceanInfo.id)
                    .all()
                )
                if idx < len(all_slices):
                    all_slices[idx].event_type = final_type
                    all_slices[idx].detect_type = 0
                    db_slice_updated += 1

            db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"❌ 資料庫寫入出錯: {str(e)}")
        errors.append(f"資料庫寫入出錯: {str(e)}")

    current_app.logger.info("--- 🏁 任務結束 ---")
    current_app.logger.info(f"成功處理 {excel_rows_success} 筆，更新 {db_slice_updated} 個切片標籤。")

    return jsonify({
        'success': True,
        'success_count': excel_rows_success,
        'db_updated': db_slice_updated,
        'errors': errors
    })