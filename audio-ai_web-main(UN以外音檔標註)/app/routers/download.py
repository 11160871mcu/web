import os
import io
import csv
import zipfile
import soundfile as sf
from flask import send_file, current_app, request
from werkzeug.utils import secure_filename
from ..main_router import main_bp

from ..models import AudioInfo, Result, CetaceanInfo, Label, BBoxAnnotation

# ─────────────────────────────────────────────────────────────
# 共用工具
# ─────────────────────────────────────────────────────────────
def _format_time(seconds):
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m:02d}:{s:06.3f}"

def _get_freq_params(params, folder_path, results_all):
    """從參數或音檔推算頻率軸範圍，回傳 (sample_rate, f_min, f_max)。"""
    try:
        sample_rate = params.get('sample_rate', 'None')
        if sample_rate == 'None' or sample_rate is None:
            first = results_all[0] if results_all else None
            if first and getattr(first, 'audio_filename', None) and folder_path:
                path = os.path.join(folder_path, first.audio_filename)
                sample_rate = sf.info(path).samplerate if os.path.exists(path) else 44100
            else:
                sample_rate = 44100
        else:
            sample_rate = float(sample_rate)
        f_min = float(params.get('f_min', 0))
        f_max = float(params.get('f_max', 0))
        if f_max <= 0:
            f_max = sample_rate / 2
    except Exception:
        sample_rate, f_min, f_max = 44100, 0, 22050
    return sample_rate, f_min, f_max


# ─────────────────────────────────────────────────────────────
# 路由 1：單筆下載（保留舊功能，向下相容）
# ─────────────────────────────────────────────────────────────
@main_bp.route('/download_dataset_zip/<int:upload_id>')
def download_dataset_zip(upload_id):
    """單筆音檔打包下載。"""
    upload = AudioInfo.query.get_or_404(upload_id)

    has_any_param = any(
        request.args.get(k) is not None
        for k in ('include_images', 'include_audio', 'include_labels', 'include_bbox')
    )
    if has_any_param:
        include_images = request.args.get('include_images') == '1'
        include_audio  = request.args.get('include_audio')  == '1'
        include_labels = request.args.get('include_labels') == '1'
        include_bbox   = request.args.get('include_bbox')   == '1'
    else:
        include_images = include_audio = include_labels = include_bbox = True

    if not any([include_images, include_audio, include_labels, include_bbox]):
        return "請至少選擇一項匯出內容。", 400

    results_all   = Result.query.filter_by(upload_id=upload_id).order_by(Result.id).all()
    cetaceans_all = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id).all()

    folder_path = os.path.join(current_app.root_path, 'static', upload.result_path)
    if (include_images or include_audio) and not os.path.exists(folder_path):
        return "找不到結果資料夾，無法下載頻譜圖或音檔。", 404
    if not os.path.exists(folder_path):
        folder_path = None

    params = upload.get_params()
    try:
        segment_duration = float(params.get('segment_duration', 2.0))
        hop_length = segment_duration * (1 - float(params.get('overlap', 50)) / 100.0)
    except Exception:
        segment_duration, hop_length = 2.0, 1.0

    memory_file = io.BytesIO()
    try:
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            if (include_images or include_audio) and folder_path:
                for root, _, files in os.walk(folder_path):
                    for file in files:
                        fp = os.path.join(root, file)
                        fl = file.lower()
                        if include_audio and fl.endswith(('.wav', '.mp3')):
                            zf.write(fp, f"audio/{file}")
                        elif include_images and fl.endswith(('.png', '.jpg')) and '_spec_training_' in fl:
                            zf.write(fp, f"images/{file}")

            if include_labels:
                label_map = {l.id: l.name for l in Label.query.all()}
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(['filename', 'event_type', 'label_name', 'time_segment', 'detect_type'])
                for i, (res, cet) in enumerate(zip(results_all, cetaceans_all)):
                    fname = f"images/{res.spectrogram_training_filename}" if include_images else res.spectrogram_training_filename
                    etype = cet.event_type
                    start = i * hop_length
                    w.writerow([fname, etype, label_map.get(etype, 'Unknown') if etype else 'Unknown',
                                 f"{_format_time(start)} - {_format_time(start + segment_duration)}",
                                 'AI' if cet.detect_type == 1 else 'Manual'])
                zf.writestr('labels.csv', buf.getvalue())

            if include_bbox:
                _, f_min, f_max = _get_freq_params(params, folder_path, results_all)
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(['filename', 'segment_index', 'label', 'time_start_sec', 'time_end_sec', 'freq_min_hz', 'freq_max_hz'])
                count = 0
                for i, res in enumerate(results_all):
                    anns = BBoxAnnotation.query.filter_by(result_id=res.id).all()
                    if not anns:
                        continue
                    fname = f"images/{res.spectrogram_training_filename}" if include_images else res.spectrogram_training_filename
                    seg_start = i * hop_length
                    for b in anns:
                        w.writerow([fname, i, b.label,
                                     f"{seg_start + b.x * segment_duration:.3f}",
                                     f"{seg_start + (b.x + b.width) * segment_duration:.3f}",
                                     f"{f_min + (1 - (b.y + b.height)) * (f_max - f_min):.2f}",
                                     f"{f_min + (1 - b.y) * (f_max - f_min):.2f}"])
                        count += 1
                if count > 0:
                    zf.writestr('bbox_annotations.csv', buf.getvalue())

        memory_file.seek(0)
        return send_file(memory_file, mimetype='application/zip', as_attachment=True,
                         download_name=f"dataset_{upload.id}_{secure_filename(upload.file_name)}.zip")
    except Exception as e:
        print(f"打包 ZIP 時發生錯誤: {e}")
        return f"打包失敗: {e}", 500


# ─────────────────────────────────────────────────────────────
# 路由 2：多筆合併下載（前端 confirmExport 呼叫此路由）
# ─────────────────────────────────────────────────────────────
@main_bp.route('/download_multiple_datasets_zip', methods=['POST'])
def download_multiple_datasets_zip():
    """多筆音檔合併打包，產出單一 ZIP 含合併後的 labels.csv。"""
    upload_ids = request.form.getlist('upload_ids')
    if not upload_ids:
        return "請至少選取一筆音檔", 400

    export_options = request.form.getlist('export_options')
    if not export_options:
        export_options = ['images', 'audio', 'csv', 'bbox']

    export_images = 'images' in export_options
    export_audio  = 'audio'  in export_options
    export_csv    = 'csv'    in export_options
    export_bbox   = 'bbox'   in export_options

    memory_file = io.BytesIO()
    try:
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:

            # 合併用的 CSV buffer
            labels_buf = io.StringIO()
            labels_w   = csv.writer(labels_buf)
            if export_csv:
                labels_w.writerow(['filename', 'event_type', 'label_name',
                                    'time_segment', 'detect_type', 'original_audio'])

            bbox_buf = io.StringIO()
            bbox_w   = csv.writer(bbox_buf)
            if export_bbox:
                bbox_w.writerow(['filename', 'segment_index', 'label',
                                  'time_start_sec', 'time_end_sec',
                                  'freq_min_hz', 'freq_max_hz', 'original_audio'])

            label_map = {l.id: l.name for l in Label.query.all()}
            bbox_total = 0

            for upload_id_str in upload_ids:
                upload = AudioInfo.query.get(int(upload_id_str))
                if not upload:
                    continue

                results_all   = Result.query.filter_by(upload_id=upload.id).order_by(Result.id).all()
                cetaceans_all = CetaceanInfo.query.filter_by(audio_id=upload.id).order_by(CetaceanInfo.id).all()

                folder_path = None
                if upload.result_path:
                    fp = os.path.join(current_app.root_path, 'static', upload.result_path)
                    if os.path.exists(fp):
                        folder_path = fp

                params = upload.get_params()
                try:
                    segment_duration = float(params.get('segment_duration', 2.0))
                    hop_length = segment_duration * (1 - float(params.get('overlap', 50)) / 100.0)
                except Exception:
                    segment_duration, hop_length = 2.0, 1.0

                spec_type = params.get('spec_type', 'unknown')

                # ── A. 實體檔案 ──
                if folder_path and (export_images or export_audio):
                    for root, _, files in os.walk(folder_path):
                        for file in files:
                            fp2 = os.path.join(root, file)
                            fl  = file.lower()
                            if export_audio and fl.endswith(('.wav', '.mp3')):
                                zf.write(fp2, f"audio/{spec_type}-{file}")
                            elif export_images and fl.endswith(('.png', '.jpg')) and '_spec_training_' in fl:
                                zf.write(fp2, f"images/{spec_type}-{file}")

                # ── B. labels.csv 行 ──
                if export_csv:
                    for i, (res, cet) in enumerate(zip(results_all, cetaceans_all)):
                        fname = f"images/{spec_type}-{res.spectrogram_training_filename}" if export_images else res.spectrogram_training_filename
                        etype = cet.event_type
                        start = i * hop_length
                        labels_w.writerow([
                            fname, etype,
                            label_map.get(etype, 'Unknown') if etype else 'Unknown',
                            f"{_format_time(start)} - {_format_time(start + segment_duration)}",
                            'AI' if cet.detect_type == 1 else 'Manual',
                            upload.file_name
                        ])

                # ── C. bbox_annotations.csv 行 ──
                if export_bbox:
                    _, f_min, f_max = _get_freq_params(params, folder_path, results_all)
                    for i, res in enumerate(results_all):
                        anns = BBoxAnnotation.query.filter_by(result_id=res.id).all()
                        if not anns:
                            continue
                        fname = f"images/{spec_type}-{res.spectrogram_training_filename}" if export_images else res.spectrogram_training_filename
                        seg_start = i * hop_length
                        for b in anns:
                            bbox_w.writerow([
                                fname, i, b.label,
                                f"{seg_start + b.x * segment_duration:.3f}",
                                f"{seg_start + (b.x + b.width) * segment_duration:.3f}",
                                f"{f_min + (1 - (b.y + b.height)) * (f_max - f_min):.2f}",
                                f"{f_min + (1 - b.y) * (f_max - f_min):.2f}",
                                upload.file_name
                            ])
                            bbox_total += 1

            # ── 寫入 CSV 檔案 ──
            if export_csv:
                zf.writestr('labels.csv', labels_buf.getvalue())
            if export_bbox and bbox_total > 0:
                zf.writestr('bbox_annotations.csv', bbox_buf.getvalue())

        memory_file.seek(0)
        return send_file(memory_file, mimetype='application/zip', as_attachment=True,
                         download_name='multi_dataset_export.zip')

    except Exception as e:
        print(f"打包 ZIP 時發生錯誤: {e}")
        return f"打包失敗: {e}", 500