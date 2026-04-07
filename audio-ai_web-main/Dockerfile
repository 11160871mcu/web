# 修改 Dockerfile

# 使用輕量級的 Python 3.9 作為基礎映像檔
FROM python:3.9-slim

# 設定容器內的工作目錄
WORKDIR /code

# 安裝系統依賴
# 新增 ffmpeg 以支援更多音訊格式
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libsndfile1 \
    ffmpeg \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 複製依賴文件並安裝 Python 套件
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 將您本地的 app 資料夾，完整複製到容器的 /code/app 路徑下
COPY ./app ./app

# 預設啟動指令
CMD ["flask", "run", "--host=0.0.0.0"]