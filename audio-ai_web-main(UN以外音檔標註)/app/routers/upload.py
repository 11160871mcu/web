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
    匯入 Excel 標記資料（新格式）

    Excel 格式規範：
      - 檔名（或 Sheet 名稱）= 音檔核心 ID，例：PAM_20250622_020939
      - 第 0 行：大標題列（起始時間、結束時間、起始頻率...）
      - 第 1 行：子標題列（分、秒、分、秒...）
      - 第 2 行起：實際標記資料，每行一筆鯨魚事件
      - 有寫的行 → event_type=1（鯨魚）
      - 沒有寫的切片 → 全部洗成 event_type=90（環境噪音）

    安全機制：
      - 使用記憶體操作（SELECT → 修改 → commit），
        避免 bulk UPDATE 後程式崩潰導致資料全變 90。
    """
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': '沒有選擇檔案'}), 400

    excel_rows_success = 0
    db_slice_updated = 0
    errors = []

    current_app.logger.info("--- 🚀 開始匯入 Excel 任務（新格式）---")

    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls')):
            errors.append(f"不支援的檔案格式: {file.filename}，請上傳 .xlsx 或 .xls")
            continue
        try:
            # --- 1. 讀取所有 sheet（每個 sheet = 一個音檔）---
            all_sheets = pd.read_excel(file, sheet_name=None, header=None)

            for sheet_name, raw_df in all_sheets.items():
                current_app.logger.info(f"📄 處理 Sheet: [{sheet_name}]，共 {len(raw_df)} 行")

                # sheet 名稱即音檔核心 ID
                excel_core_id = sheet_name.strip()

                # --- 2. 配對資料庫中的音檔 ---
                # 優先：精確比對副檔名（避免 PAM_026 誤配到 PAM_0260）
                target_audios = AudioInfo.query.filter(
                    AudioInfo.file_name.in_([
                        f"{excel_core_id}.wav",
                        f"{excel_core_id}.mp3",
                        f"{excel_core_id}.flac",
                    ])
                ).all()

                if target_audios:
                    current_app.logger.info(f"✅ 精確比對成功: {excel_core_id}，共 {len(target_audios)} 筆")
                else:
                    # 次優：去掉末尾 _數字 後綴再精確比對
                    # 例：PAM_20250622_020939_784 → PAM_20250622_020939
                    trimmed_id = re.sub(r'_\d+$', '', excel_core_id)
                    if trimmed_id != excel_core_id:
                        target_audios = AudioInfo.query.filter(
                            AudioInfo.file_name.in_([
                                f"{trimmed_id}.wav",
                                f"{trimmed_id}.mp3",
                                f"{trimmed_id}.flac",
                            ])
                        ).all()
                        if target_audios:
                            current_app.logger.info(f"🔁 後綴修剪後精確比對成功: {excel_core_id} → {trimmed_id}，共 {len(target_audios)} 筆")

                if not target_audios:
                    msg = f"找不到音檔，核心ID: {excel_core_id}"
                    current_app.logger.warning(f"❌ {msg}")
                    errors.append(msg)
                    continue

                # --- 3. 解析欄位位置（從第 0 行大標題找「起始」「結束」）---
                header_row0 = raw_df.iloc[0].astype(str).str.strip()

                start_col_idx = None
                end_col_idx   = None
                for i, val in enumerate(header_row0):
                    if '起始' in val or 'start' in val.lower() or 'begin' in val.lower():
                        if start_col_idx is None:
                            start_col_idx = i
                    if '結束' in val or 'end' in val.lower():
                        if end_col_idx is None:
                            end_col_idx = i

                # 若找不到欄位，預設前四欄：[起始分, 起始秒, 結束分, 結束秒]
                if start_col_idx is None:
                    start_col_idx = 0
                if end_col_idx is None:
                    end_col_idx = 2

                start_min_col = start_col_idx
                start_sec_col = start_col_idx + 1
                end_min_col   = end_col_idx
                end_sec_col   = end_col_idx + 1

                # --- 4. 資料從第 2 行開始（跳過大標題行與分/秒子標題行）---
                data_df = raw_df.iloc[2:].reset_index(drop=True)

                # audio_updates: { audio_id: { slice_index: event_type } }
                # 收集所有要打鯨魚標籤的切片，最後再統一寫入
                audio_updates = {}

                for index, row in data_df.iterrows():
                    # --- 5. 解析分、秒欄位 → 轉成絕對秒數 ---
                    # 任一欄位為 NULL → 整行跳過（視為環境噪音，不標鯨魚）
                    if (pd.isna(row.iloc[start_min_col]) or pd.isna(row.iloc[start_sec_col]) or
                            pd.isna(row.iloc[end_min_col])   or pd.isna(row.iloc[end_sec_col])):
                        current_app.logger.info(f"跳過第 {index + 3} 行：含 NULL，視為環境噪音")
                        continue
                    try:
                        start_min = float(row.iloc[start_min_col])
                        start_sec = float(row.iloc[start_sec_col])
                        end_min   = float(row.iloc[end_min_col])
                        end_sec   = float(row.iloc[end_sec_col])
                    except Exception as e:
                        current_app.logger.warning(f"跳過第 {index + 3} 行：時間解析失敗 ({e})")
                        continue

                    calc_start_time = start_min * 60.0 + start_sec
                    calc_end_time   = end_min   * 60.0 + end_sec

                    if calc_end_time < calc_start_time:
                        current_app.logger.warning(
                            f"跳過第 {index + 3} 行：結束時間({calc_end_time}s) < 起始時間({calc_start_time}s)"
                        )
                        continue

                    # 開始時間 == 結束時間：視為瞬間事件，
                    # 標記「包含該時間點」的所有切片（通常只有 1 個）
                    if calc_end_time == calc_start_time:
                        current_app.logger.info(
                            f"第 {index + 3} 行：開始時間 == 結束時間 ({calc_start_time}s)，視為瞬間鯨魚事件"
                        )
                        # 讓結束時間加一個極小偏移，使下方切片計算能取到該點所在的切片
                        calc_end_time = calc_start_time + 1e-6

                    # 有寫記錄的行 = 鯨魚
                    event_type = 1
                    excel_rows_success += 1

                    # --- 6. 對每個配對音檔計算切片索引 ---
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

                        # ✅ 正確做法：找出所有「視窗有覆蓋到此事件時間段」的切片
                        # 切片 i 的視窗 = [i*step, i*step+segment_duration)
                        # 覆蓋條件：i*step < calc_end_time  AND  i*step+segment_duration > calc_start_time
                        # 整理後：i >= ceil((calc_start_time - segment_duration) / step)
                        #          i <= floor((calc_end_time - epsilon) / step)
                        start_idx = max(0, int((calc_start_time - segment_duration) // step) + 1)
                        end_idx   = int((calc_end_time - 1e-9) // step)  # 1e-9 避免剛好整除時多算一格

                        current_app.logger.info(
                            f"✅ 配對: {target_audio.file_name} | "
                            f"{start_min:.0f}分{start_sec}秒 ~ {end_min:.0f}分{end_sec}秒 "
                            f"= {calc_start_time:.2f}s~{calc_end_time:.2f}s | "
                            f"step={step}s | 切片: {start_idx}~{end_idx} | event=1(鯨魚)"
                        )

                        if target_audio.id not in audio_updates:
                            audio_updates[target_audio.id] = {}

                        for calc_idx in range(start_idx, end_idx + 1):
                            # 同一切片若已有鯨魚標記（1~17），不覆蓋；否則寫入
                            # end_idx 上限由寫入時的 total_slices 把關，這裡先存入，寫入時自動跳過超界
                            existing = audio_updates[target_audio.id].get(calc_idx)
                            if existing is None or not (1 <= existing <= 17):
                                audio_updates[target_audio.id][calc_idx] = event_type

                # --- 7. 安全寫入：逐音檔 SELECT → 記憶體修改 → commit ---
                # 確保所有配對到的音檔都被洗底色（即使 Excel 全空、沒有任何鯨魚標記）
                for target_audio in target_audios:
                    if target_audio.id not in audio_updates:
                        audio_updates[target_audio.id] = {}  # 空字典 = 全部洗 90，無鯨魚

                for audio_id, whale_indices in audio_updates.items():
                    try:
                        all_records = (
                            CetaceanInfo.query
                            .filter_by(audio_id=audio_id)
                            .order_by(CetaceanInfo.id)
                            .all()
                        )

                        if not all_records:
                            current_app.logger.warning(f"AudioID={audio_id} 無切片資料，跳過")
                            continue

                        total_slices = len(all_records)
                        current_app.logger.info(
                            f"洗底色: AudioID={audio_id}，共 {total_slices} 個切片，"
                            f"鯨魚切片數={len(whale_indices)}"
                        )

                        # 全部先設為環境噪音
                        for record in all_records:
                            record.event_type = 90
                            record.detect_type = 0

                        # 把 Excel 標記的切片改為鯨魚
                        for idx, etype in whale_indices.items():
                            if idx < total_slices:
                                all_records[idx].event_type = etype
                                all_records[idx].detect_type = 0
                                db_slice_updated += 1
                            else:
                                current_app.logger.debug(
                                    f"切片索引 {idx} 超出範圍（共 {total_slices} 個），跳過"
                                )

                        db.session.commit()
                        current_app.logger.info(f"✅ AudioID={audio_id} 寫入完成")

                    except Exception as e:
                        db.session.rollback()
                        msg = f"AudioID={audio_id} 寫入失敗: {str(e)}"
                        current_app.logger.error(f"❌ {msg}")
                        errors.append(msg)

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"❌ 檔案處理出錯: {str(e)}")
            errors.append(f"檔案 {file.filename} 處理出錯: {str(e)}")

    current_app.logger.info("--- 🏁 任務結束 ---")
    current_app.logger.info(f"成功處理 {excel_rows_success} 筆標記，更新 {db_slice_updated} 個切片標籤。")

    return jsonify({
        'success': True,
        'success_count': excel_rows_success,
        'db_updated': db_slice_updated,
        'errors': errors
    })