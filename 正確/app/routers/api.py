import os
from functools import wraps
from flask import request, jsonify
from werkzeug.utils import secure_filename
from ..main_router import main_bp

from ..models import AudioInfo, PointInfo
from .. import db

# API 權限驗證 (給洋聲使用) - 從環境變數取得 Token
API_TOKENS = {
    "yang_sheng_partner": os.environ.get('YANG_SHENG_API_TOKEN', 'sk_test_1234567890abcdef')
}

def require_api_token(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'success': False, 'error': '缺少 Authorization Header'}), 401
        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return jsonify({'success': False, 'error': 'Authorization 格式錯誤'}), 401
        token = parts[1]
        if token not in API_TOKENS.values():
            return jsonify({'success': False, 'error': '無效的 API Token'}), 403
        return f(*args, **kwargs)
    return decorated_function

@main_bp.route('/api/v1/upload', methods=['POST'])
@require_api_token
def api_upload_audio():
    """洋聲 API: 上傳並觸發分析"""
    if 'file' not in request.files: return jsonify({'error': 'No file'}), 400
    file = request.files['file']
    try:
        point_id = int(request.form.get('point_id'))
        if not PointInfo.query.get(point_id): return jsonify({'error': 'Invalid point_id'}), 404
    except Exception as e:
        print(f"point_id 解析錯誤: {e}")
        return jsonify({'error': 'Missing point_id'}), 400

    filename = secure_filename(file.filename)
    new_audio = AudioInfo(
        file_name=filename, file_path="pending", file_type="wav",
        point_id=point_id, result_path="pending", status='PENDING'
    )
    db.session.add(new_audio)
    db.session.commit()
    
    return jsonify({'success': True, 'upload_id': new_audio.id}), 201
