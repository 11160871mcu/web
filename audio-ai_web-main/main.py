"""
應用程式的總進入點
此檔案用於啟動 Flask 伺服器
"""
from app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
