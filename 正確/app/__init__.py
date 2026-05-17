"""
Flask 應用程式初始化模組。

此模組負責：
1. 初始化 Flask 應用程式實例
2. 設定資料庫連線 (SQLAlchemy)
3. 配置 Celery 任務佇列
4. 註冊 Blueprint 路由
5. 建立必要的檔案目錄
6. 整合 Flask 上下文與 Celery 任務

架構模式：
- 使用應用程式工廠模式 (Application Factory Pattern)
- 支援環境變數配置
- 資料庫使用 MySQL 8.0
- 訊息佇列使用 Redis

環境變數：
- DATABASE_URL: MySQL 資料庫連線字串
- CELERY_BROKER_URL: Celery broker URL (Redis)
- CELERY_RESULT_BACKEND: Celery result backend URL (Redis)
"""

import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from celery import Celery, Task

# ============================================================================
# 全域元件初始化
# ============================================================================

# SQLAlchemy 資料庫實例
db = SQLAlchemy()

# Celery 任務佇列實例
# - broker: 訊息佇列位置 (Redis)
# - backend: 任務結果儲存位置 (Redis)
# - include: 自動載入的任務模組
celery = Celery(
    __name__,
    broker=os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    backend=os.environ.get('CELERY_RESULT_BACKEND', 'redis://redis:6379/0'),
    include=['app.tasks']
)


# ============================================================================
# 應用程式工廠函式
# ============================================================================

def create_app():
    """
    建立並配置 Flask 應用程式實例。
    
    使用工廠模式建立應用程式，便於測試和部署。
    
    配置流程：
    1. 載入資料庫和 Celery 設定
    2. 初始化 SQLAlchemy
    3. 建立必要的檔案目錄
    4. 註冊 Blueprint
    5. 建立資料庫表格
    6. 設定 Celery 與 Flask 上下文整合
    
    Returns:
        Flask: 配置完成的 Flask 應用程式實例
        
    環境變數：
        DATABASE_URL (str): MySQL 連線字串，格式：mysql+pymysql://user:password@host/database
        CELERY_BROKER_URL (str): Redis 訊息佇列 URL
        CELERY_RESULT_BACKEND (str): Redis 結果儲存 URL
    """
    app = Flask(__name__)

    # ------------------------------------------------------------------------
    # 資料庫配置
    # ------------------------------------------------------------------------
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
        'DATABASE_URL', 
        'mysql+pymysql://user:password@db/audio_db'
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # ------------------------------------------------------------------------
    # Celery 配置更新
    # ------------------------------------------------------------------------
    celery.conf.update(
        broker_url=os.environ.get('CELERY_BROKER_URL'),
        result_backend=os.environ.get('CELERY_RESULT_BACKEND')
    )

    # ------------------------------------------------------------------------
    # 初始化資料庫
    # ------------------------------------------------------------------------
    db.init_app(app)

    # ------------------------------------------------------------------------
    # 檔案路徑配置
    # ------------------------------------------------------------------------
    app.config['UPLOAD_FOLDER'] = 'static/uploads'      # 原始上傳音檔
    app.config['RESULT_FOLDER'] = 'static/results'      # 分析結果（頻譜圖、切割音檔）

    # 建立必要的目錄結構
    with app.app_context():
        os.makedirs(os.path.join(app.root_path, app.config['UPLOAD_FOLDER']), exist_ok=True)
        os.makedirs(os.path.join(app.root_path, app.config['RESULT_FOLDER']), exist_ok=True)
        os.makedirs(os.path.join(app.root_path, 'static', 'training_runs'), exist_ok=True)

    # ------------------------------------------------------------------------
    # 註冊 Blueprint 與建立資料庫表格
    # ------------------------------------------------------------------------
    with app.app_context():
        # 匯入並註冊主要路由 Blueprint
        from . import main_router as main
        app.register_blueprint(main.main_bp)
        
        # 建立所有資料表（如果不存在）
        try:
            db.create_all()
        except Exception as e:
            # 忽略並發建立表格的錯誤（例如多個 worker 同時啟動）
            print(f"資料庫表格建立檢查: {e}")

    # ------------------------------------------------------------------------
    # Celery 與 Flask 上下文整合
    # ------------------------------------------------------------------------
    # 自訂 Task 類別，使 Celery 任務能夠存取 Flask 應用程式上下文
    # 這對於在背景任務中使用 db.session 等 Flask 功能至關重要
    class FlaskTask(Task):
        """
        整合 Flask 上下文的 Celery Task 基礎類別。
        
        所有 Celery 任務將在 Flask 應用程式上下文中執行，
        確保可以正常使用資料庫、配置等 Flask 資源。
        """
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = FlaskTask

    return app


# ============================================================================
# 應用程式實例
# ============================================================================

# 建立全域應用程式實例
# 此實例將被 WSGI 伺服器（如 Gunicorn）使用
app = create_app()