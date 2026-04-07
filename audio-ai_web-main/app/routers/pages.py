import os
import json
from flask import render_template, request, current_app
from ..main_router import main_bp

# 引入相關模型
from ..models import AudioInfo, Result, Label, TrainingRun, CetaceanInfo

@main_bp.route('/')
def index():
    """渲染主上傳頁面。"""
    return render_template('index.html')

@main_bp.route('/history')
def history():
    """渲染分析歷史紀錄頁面。"""
    sort_order = request.args.get('sort', 'desc')
    query = AudioInfo.query
    
    if sort_order == 'asc':
        all_uploads = query.order_by(AudioInfo.record_time.asc()).all()
    else:
        all_uploads = query.order_by(AudioInfo.record_time.desc()).all()
        
    # 計算標記數據
    from ..models import CetaceanInfo, Label
    labels = Label.query.all()
    label_map = {l.id: l.name for l in labels}
    
    DEFAULT_LABEL_MAP = {
        1: '1. 鯨魚 (Whale)',
        10: '10. 上升型 (Upsweep)',
        11: '11. 下降型 (Downsweep)',
        12: '12. U型 (Concave)',
        13: '13. 倒U型 (Convex)',
        14: '14. sin型 (Sine)',
        15: '15. 嘎搭聲 (Click)',
        16: '16. 突發脈衝聲 (Burst)',
        17: '17. 常數型 (Constant)',
        90: '90. 環境噪音 (Noise)',
        91: '91. 船舶 (Ship)',
        92: '92. 風機打樁 (Piling)'
    }
    
    for upload in all_uploads:
        if upload.status == 'COMPLETED':
            cetaceans = CetaceanInfo.query.filter_by(audio_id=upload.id).filter(CetaceanInfo.event_type != 0).all()
            if cetaceans:
                counts_id = {}
                for c in cetaceans:
                    counts_id[c.event_type] = counts_id.get(c.event_type, 0) + 1
                
                sorted_counts = {}
                for eid in sorted(counts_id.keys()):
                    label_name = label_map.get(eid)
                    if not label_name:
                        label_name = DEFAULT_LABEL_MAP.get(eid, str(eid))
                    sorted_counts[label_name] = counts_id[eid]
                
                upload.label_counts = sorted_counts
            else:
                upload.label_counts = None
        else:
            upload.label_counts = None
            
    return render_template('history.html', uploads=all_uploads, current_sort=sort_order)

@main_bp.route('/results/<int:upload_id>')
def results(upload_id):
    """
    渲染單次分析結果頁面。
    在這裡進行動態配對，將 Result 的圖片 URL 塞給 CetaceanInfo 物件。
    """
    page = request.args.get('page', 1, type=int)
    upload_record = AudioInfo.query.get_or_404(upload_id)
    params = upload_record.get_params()
    
    # 計算顯示用的秒數
    try:
        segment_duration = float(params.get('segment_duration', 2.0))
        overlap_percent = float(params.get('overlap', 50))
        hop_length_seconds = segment_duration * (1 - overlap_percent / 100.0)
    except (ValueError, TypeError):
        hop_length_seconds = 1.0

    # 1. 查詢資料分頁 (CetaceanInfo)
    pagination = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id.asc()).paginate(
        page=page, per_page=10, error_out=False
    )
    
    # 2. 查詢對應的圖片 (Result)
    offset = (page - 1) * 10
    limit = 10
    results_slice = Result.query.filter_by(upload_id=upload_id).order_by(Result.id.asc()).offset(offset).limit(limit).all()
    
    # 3. 動態合併
    for cetacean, result in zip(pagination.items, results_slice):
        setattr(cetacean, 'spectrogram_url', result.spectrogram_url)
        setattr(cetacean, 'audio_url', result.audio_url)
        label_obj = Label.query.get(cetacean.event_type) if cetacean.event_type else None
        setattr(cetacean, 'label_name', label_obj.name if label_obj else 'Unknown')

    return render_template(
        'result.html',
        upload=upload_record,
        pagination=pagination,
        hop_length_seconds=hop_length_seconds,
        params=params
    )

@main_bp.route('/labeling/<int:upload_id>')
def labeling_page(upload_id):
    """渲染標記頁面。"""
    page = request.args.get('page', 1, type=int)
    upload_record = AudioInfo.query.get_or_404(upload_id)
    
    pagination = CetaceanInfo.query.filter_by(audio_id=upload_id).order_by(CetaceanInfo.id.asc()).paginate(
        page=page, per_page=50, error_out=False
    )
    
    offset = (page - 1) * 50
    results_slice = Result.query.filter_by(upload_id=upload_id).order_by(Result.id.asc()).offset(offset).limit(50).all()
    
    for cetacean, result in zip(pagination.items, results_slice):
        setattr(cetacean, 'spectrogram_url', result.spectrogram_url)
        setattr(cetacean, 'spectrogram_training_url', result.spectrogram_training_url)
        setattr(cetacean, 'label_id', cetacean.event_type)
    
    # 查詢已完成的訓練任務供自動標記使用
    training_runs = TrainingRun.query.filter_by(status='SUCCESS').order_by(TrainingRun.timestamp.desc()).all()

    return render_template('label.html', upload=upload_record, pagination=pagination, training_runs=training_runs)

@main_bp.route('/label-advanced/<int:upload_id>')
def label_advanced_page(upload_id):
    """渲染進階框選標記頁面 - 一張圖一頁"""
    upload_record = AudioInfo.query.get_or_404(upload_id)
    index = request.args.get('index', 0, type=int)

    results_all = Result.query.filter_by(upload_id=upload_id).order_by(Result.id.asc()).all()
    total = len(results_all)

    if total == 0:
        return "此分析尚無頻譜圖結果。", 404

    # 確保 index 在合法範圍
    index = max(0, min(index, total - 1))
    current_result = results_all[index]

    return render_template(
        'label_advanced.html',
        upload=upload_record,
        result=current_result,
        current_index=index,
        total=total
    )
