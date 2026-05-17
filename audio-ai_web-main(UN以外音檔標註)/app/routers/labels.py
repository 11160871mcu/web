import os
import json
from flask import request, jsonify, redirect, url_for, current_app
from ..main_router import main_bp

from ..models import CetaceanInfo, TrainingRun, Label, Result, BBoxAnnotation
from .. import db, celery

@main_bp.route('/api/cetacean/<int:cetacean_id>/label', methods=['POST'])
def update_cetacean_label(cetacean_id):
    """API: 更新單筆鯨豚資訊的標籤"""
    cetacean = CetaceanInfo.query.get_or_404(cetacean_id)
    data = request.get_json()
    label_id = data.get('label_id') 
    
    if label_id is not None:
        cetacean.event_type = int(label_id)
        cetacean.detect_type = 0 # 0: 人工標記
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'error': 'Missing label_id'}), 400

@main_bp.route('/labeling/auto_label', methods=['POST'])
def auto_label():
    """啟動自動標記任務 - 使用已訓練的模型"""
    upload_id = request.form.get('upload_id')
    run_id = request.form.get('run_id')
    
    if not upload_id or run_id is None:
        return "缺少必要參數", 400
    
    # 從 TrainingRun 取得模型資訊
    training_run = TrainingRun.query.get(run_id)
    if not training_run or training_run.status != 'SUCCESS':
        return "找不到有效的訓練模型", 404
    
    # 取得模型路徑
    model_path = os.path.join(
        current_app.root_path, 'static', 
        training_run.results_path, 'weights', 'best.pt'
    )
    
    if not os.path.exists(model_path):
        return f"模型檔案不存在: {model_path}", 404
    
    # 從訓練參數取得模型類型
    params = training_run.get_params() if hasattr(training_run, 'get_params') else {}
    if isinstance(params, str):
        params = json.loads(params) if params else {}
    model_type = params.get('model_type', 'yolov8n-cls')
    
    # 從 metrics 取得類別資訊
    metrics = training_run.get_metrics() if hasattr(training_run, 'get_metrics') else {}
    if isinstance(metrics, str):
        metrics = json.loads(metrics) if metrics else {}
    
    classes_list = []
    if 'per_class_list' in metrics:
        classes_list = [item['name'] for item in metrics['per_class_list']]
    
    celery.send_task('app.tasks.auto_label_task_v2', args=[
        int(upload_id), 
        model_path, 
        model_type, 
        classes_list
    ])
    return redirect(url_for('main.labeling_page', upload_id=upload_id))

@main_bp.route('/api/labels', methods=['GET', 'POST'])
def handle_labels():
    if request.method == 'GET':
        return jsonify([{'id': l.id, 'name': l.name} for l in Label.query.all()])
    if request.method == 'POST':
        data = request.get_json()
        if Label.query.filter_by(name=data['name']).first(): return jsonify({'error': 'Exists'}), 409
        new_label = Label(name=data['name'])
        db.session.add(new_label)
        db.session.commit()
        return jsonify({'id': new_label.id, 'name': new_label.name}), 201

@main_bp.route('/api/bbox/<int:result_id>', methods=['GET'])
def get_bbox_annotations(result_id):
    """取得一張頻譜圖的所有框選標記"""
    Result.query.get_or_404(result_id)
    annotations = BBoxAnnotation.query.filter_by(result_id=result_id).all()
    return jsonify([
        {
            'id': a.id,
            'label': a.label,
            'x': a.x,
            'y': a.y,
            'width': a.width,
            'height': a.height
        }
        for a in annotations
    ])

@main_bp.route('/api/bbox/<int:result_id>', methods=['POST'])
def save_bbox_annotations(result_id):
    """儲存一張頻譜圖的所有框選標記 (先刪後寫)"""
    Result.query.get_or_404(result_id)
    data = request.get_json()
    boxes = data.get('boxes', [])

    # 刪除舊的標記
    BBoxAnnotation.query.filter_by(result_id=result_id).delete()

    # 寫入新的標記
    for box in boxes:
        annotation = BBoxAnnotation(
            result_id=result_id,
            label=box['label'],
            x=float(box['x']),
            y=float(box['y']),
            width=float(box['width']),
            height=float(box['height'])
        )
        db.session.add(annotation)

    db.session.commit()
    return jsonify({'success': True, 'count': len(boxes)})

@main_bp.route('/api/upload/<int:upload_id>/clear_labels', methods=['POST'])
def clear_all_labels(upload_id):
    """API: 清空一份音檔的所有標記"""
    try:
        # 清空 BBoxAnnotation
        results = Result.query.filter_by(upload_id=upload_id).all()
        result_ids = [r.id for r in results]
        if result_ids:
            BBoxAnnotation.query.filter(BBoxAnnotation.result_id.in_(result_ids)).delete(synchronize_session=False)

        # 清空 CetaceanInfo 事件
        CetaceanInfo.query.filter_by(audio_id=upload_id).update(
            {'event_type': 0, 'detect_type': 0},
            synchronize_session=False
        )

        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
