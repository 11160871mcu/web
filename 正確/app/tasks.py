from . import celery
from .services.audio_service import AudioService
from .ml.yolo_trainer import YoloTrainer
from .ml.cnn_trainer import CnnTrainer
from .ml.inference import InferenceService

# --- 任務 1: 音訊處理 ---
@celery.task(name='app.tasks.process_audio_task', bind=True)
def process_audio_task(self, audio_id):
    """
    背景任務：處理上傳的音訊檔案，切割成片段並產生頻譜圖。
    """
    AudioService.process_audio(audio_id)


# --- 任務 2: 模型訓練 ---
@celery.task(name='app.tasks.train_yolo_model')
def train_yolo_model(upload_ids, training_run_id, model_name='yolov8n-cls.pt', train_params=None):
    """
    背景任務：使用已標記的資料來訓練 YOLOv8 分類模型。
    """
    YoloTrainer.train(upload_ids, training_run_id, model_name, train_params)


@celery.task(name='app.tasks.train_cnn_model')
def train_cnn_model(upload_ids, training_run_id, model_name='resnet18', train_params=None):
    """
    背景任務：使用 PyTorch 訓練 CNN 分類模型 (ResNet18, EfficientNet-B0)。
    """
    CnnTrainer.train(upload_ids, training_run_id, model_name, train_params)


# --- 任務 3: AI 自動標記 ---
@celery.task(name='app.tasks.auto_label_task')
def auto_label_task(upload_id, model_path, model_type='yolo', classes_str=''):
    """
    背景任務：對 CetaceanInfo 進行自動標記。
    """
    InferenceService.auto_label(upload_id, model_path, model_type, classes_str)


@celery.task(name='app.tasks.auto_label_task_v2')
def auto_label_task_v2(upload_id, model_path, model_type='yolov8n-cls', classes_list=None):
    """
    背景任務：對 CetaceanInfo 進行自動標記 (V2)。
    """
    InferenceService.auto_label_v2(upload_id, model_path, model_type, classes_list)