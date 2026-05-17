from flask import jsonify
from ..main_router import main_bp

from ..models import AudioInfo, TrainingRun

@main_bp.route('/api/upload/<int:upload_id>/status')
def upload_status(upload_id):
    """查詢分析任務狀態"""
    upload = AudioInfo.query.get_or_404(upload_id)
    return jsonify({
        'id': upload.id,
        'status': upload.status,
        'progress': upload.progress
    })

@main_bp.route('/api/training/<int:run_id>/status')
def run_status(run_id):
    """查詢訓練任務狀態"""
    run = TrainingRun.query.get_or_404(run_id)
    return jsonify({
        'id': run.id,
        'status': run.status,
        'progress': run.progress
    })
