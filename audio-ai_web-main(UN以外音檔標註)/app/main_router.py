from flask import Blueprint

# 建立 Blueprint 實例，作為所有端點的進入點
main_bp = Blueprint('main', __name__)

# 引入所有的子路由設定，這樣它們就會自動註冊到這個 main_bp 上
from .routers import pages, upload, status, training, labels, download, api
