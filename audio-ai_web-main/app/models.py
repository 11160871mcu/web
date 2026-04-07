"""
資料庫模型定義模組。

此模組包含所有 SQLAlchemy ORM 模型定義，用於：
1. 管理海洋生物聲學監測計畫的基礎資料
2. 儲存音檔與分析結果
3. 紀錄 AI 訓練任務與標記資料

資料庫架構：
- 基礎資訊層：ProjectInfo, PointInfo, RecoderInfo（監測計畫管理）
- 核心數據層：AudioInfo（音檔主表）
- 分析結果層：CetaceanInfo, ShipInfo, TurbineInfo（檢測結果）
- 系統支援層：Result, Label, TrainingRun, BBoxAnnotation（檔案與訓練管理）

關聯架構：
    ProjectInfo (1) --\u003e (N) PointInfo (1) --\u003e (N) AudioInfo
    RecoderInfo (1) --\u003e (N) PointInfo
    AudioInfo (1) --\u003e (N) CetaceanInfo/ShipInfo/TurbineInfo
    AudioInfo (1) --\u003e (N) Result (1) --\u003e (N) BBoxAnnotation

時區設定：
- 所有 DateTime 欄位使用台北時間 (Asia/Taipei)
 
資料庫：MySQL 8.0
引擎：InnoDB（支援外鍵約束與事務）
字元集：utf8mb4（支援繁體中文與 Emoji）
"""

from . import db
from datetime import datetime
from zoneinfo import ZoneInfo
import json

# ============================================================================
# 全域配置
# ============================================================================

# 台北時區（UTC+8）
# 用於所有 DateTime 欄位的預設值
TAIPEI_TZ = ZoneInfo("Asia/Taipei")


# ============================================================================
# 基礎資訊模型（監測計畫管理）
# ============================================================================

class ProjectInfo(db.Model):
    """
    監測計畫資訊表（表二）。
    
    紀錄海洋生物聲學監測計畫的基本資訊，一個計畫可包含多個監測點位。
    
    Attributes:
        id (int): 主鍵
        name (str): 計畫名稱
        area (str): 執行區域（如「台灣東部海域」）
        start_time (datetime): 計畫開始時間
        end_time (datetime): 計畫結束時間
        points (list[PointInfo]): 關聯的點位列表（一對多）
    """
    __tablename__ = 'project_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    name = db.Column(db.String(255), nullable=False, comment='計畫名稱')
    area = db.Column(db.String(255), nullable=True, comment='執行區域')
    start_time = db.Column(db.DateTime, nullable=True, comment='開始時間')
    end_time = db.Column(db.DateTime, nullable=True, comment='結束時間')
    
    # 關聯：一個計畫擁有多個點位
    points = db.relationship('PointInfo', backref='project', lazy=True)


class RecoderInfo(db.Model):
    """
    水下錄音機資訊表（表四）。
    
    管理水下錄音機（Hydrophone）的儀器規格與狀態，用於追蹤監測設備。
    
    Attributes:
        id (int): 主鍵
        brand (str): 儀器廠牌（如「Ocean Instruments」）
        recoder (str): 儀器型號
        sn (str): 儀器序號（唯一識別碼）
        sen (float): 靈敏度（dB re 1V/μPa）
        high_gain (float): 高增益值（dB）
        low_gain (float): 低增益值（dB）
        status (int): 儀器狀態（1=正常, 0=故障）
        belong (str): 儀器產權單位
        points (list[PointInfo]): 使用此儀器的點位列表
    """
    __tablename__ = 'recoder_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    brand = db.Column(db.String(100), nullable=False, comment='廠牌')
    recoder = db.Column(db.String(100), nullable=False, comment='型號')
    sn = db.Column(db.String(100), nullable=False, comment='序號')
    sen = db.Column(db.Float, nullable=True, comment='靈敏度')
    high_gain = db.Column(db.Float, nullable=True, comment='高增益')
    low_gain = db.Column(db.Float, nullable=True, comment='低增益')
    status = db.Column(db.Integer, default=1, comment='狀態(1=正常)')
    belong = db.Column(db.String(100), nullable=True, comment='產權單位')
    
    # 關聯：一台錄音機可用於多個點位
    points = db.relationship('PointInfo', backref='recoder', lazy=True)


class PointInfo(db.Model):
    """
    監測點位資訊表（表三）。
    
    紀錄單一監測點位的地理資訊、執行時間與使用設備。
    
    Attributes:
        id (int): 主鍵
        name (str): 點位名稱（如「東部外海 ST01」）
        phase (int): 點位階段（0=規劃中, 1=執行中, 2=已回收）
        start_time (datetime): 佈放時間
        end_time (datetime): 回收時間
        gps_lat (float): GPS 緯度（十進位度數）
        gps_lon (float): GPS 經度（十進位度數）
        depth (float): 水深（公尺）
        fs (int): 錄音取樣頻率（Hz）
        return_success (bool): 儀器回收成功（True=成功, False=遺失）
        project_id (int): 所屬計畫 ID
        recoder_id (int): 使用的錄音機 ID
        audios (list[AudioInfo]): 此點位的音檔列表
    """
    __tablename__ = 'point_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    name = db.Column(db.String(255), nullable=False, comment='點位名稱')
    phase = db.Column(db.Integer, default=0, nullable=False, comment='階段')
    start_time = db.Column(db.DateTime, nullable=True, comment='佈放時間')
    end_time = db.Column(db.DateTime, nullable=True, comment='回收時間')
    gps_lat = db.Column(db.Float, nullable=True, comment='緯度')
    gps_lon = db.Column(db.Float, nullable=True, comment='經度')
    depth = db.Column(db.Float, nullable=True, comment='水深(m)')
    fs = db.Column(db.Integer, nullable=True, comment='取樣頻率(Hz)')
    return_success = db.Column(db.Boolean, nullable=True, comment='回收成功')
    
    # 外鍵
    project_id = db.Column(db.Integer, db.ForeignKey('project_info.id'), nullable=True, comment='所屬計畫')
    recoder_id = db.Column(db.Integer, db.ForeignKey('recoder_info.id'), nullable=True, comment='使用錄音機')
    
    # 關聯：一個點位擁有多個音檔
    audios = db.relationship('AudioInfo', backref='point', lazy=True)


# ============================================================================
# 核心數據模型（音檔管理）
# ============================================================================

class AudioInfo(db.Model):
    """
    音檔資訊主表（表六）。
    
    系統的核心表格，儲存上傳音檔的基本資訊與處理狀態。
    每個音檔會被切割成多個片段並產生頻譜圖，透過 results 關聯存取。
    
    Attributes:
        id (int): 主鍵（音檔 ID）
        file_name (str): 原始檔案名稱
        file_path (str): 儲存路徑（相對於 UPLOAD_FOLDER）
        file_type (str): 檔案類型（wav, mp3）
        record_time (datetime): 錄製時間（預設為上傳時間）
        record_duration (float): 錄製長度（秒）
        target (str): 監測目標物（如「鯨豚」）
        target_type (int): 目標物類型代碼
        fs (int): 取樣頻率（Hz）
        point_id (int): 所屬點位 ID
        
        # 系統運作欄位
        result_path (str): 分析結果資料夾路徑（相對於 RESULT_FOLDER）
        params (str): JSON 格式的處理參數
        status (str): 處理狀態（PENDING, RUNNING, SUCCESS, FAILURE）
        progress (int): 處理進度（0-100%）
        
        # 關聯
        cetaceans (list[CetaceanInfo]): 鯨豚檢測結果
        ships (list[ShipInfo]): 船舶檢測結果
        turbines (list[TurbineInfo]): 風機檢測結果
        results (list[Result]): 分析產生的檔案列表
        
    Examples:
        >>> audio = AudioInfo.query.get(1)
        >>> params = audio.get_params()
        >>> print(params['segment_duration'])  # 取得切割片段長度
        2.0
        >>> for result in audio.results:
        ...     print(result.spectrogram_url)  # 顯示所有頻譜圖
    """
    __tablename__ = 'audio_info'
    
    # 基本欄位
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    file_name = db.Column(db.String(255), nullable=False, comment='檔案名稱')
    file_path = db.Column(db.String(255), nullable=False, comment='檔案路徑')
    file_type = db.Column(db.String(50), nullable=False, comment='檔案類型')
    record_time = db.Column(db.DateTime, default=lambda: datetime.now(TAIPEI_TZ), comment='錄製時間')
    record_duration = db.Column(db.Float, nullable=True, comment='長度(s)')
    target = db.Column(db.String(100), nullable=True, comment='監測目標')
    target_type = db.Column(db.Integer, nullable=True, comment='目標類型')
    fs = db.Column(db.Integer, nullable=True, comment='取樣頻率')
    
    # 外鍵
    point_id = db.Column(db.Integer, db.ForeignKey('point_info.id'), nullable=True, comment='所屬點位')

    # 系統運作欄位
    result_path = db.Column(db.String(255), nullable=True, comment='結果路徑')
    params = db.Column(db.Text, nullable=True, comment='處理參數(JSON)')
    status = db.Column(db.String(50), default='PENDING', nullable=False, comment='處理狀態')
    progress = db.Column(db.Integer, default=0, nullable=False, comment='進度(%)')

    # 關聯（cascade 確保刪除音檔時一併刪除相關記錄）
    cetaceans = db.relationship('CetaceanInfo', backref='audio_info', lazy=True, cascade="all, delete-orphan")
    ships = db.relationship('ShipInfo', backref='audio_info', lazy=True, cascade="all, delete-orphan")
    turbines = db.relationship('TurbineInfo', backref='audio_info', lazy=True, cascade="all, delete-orphan")
    results = db.relationship('Result', backref='audio_info', lazy=True, cascade="all, delete-orphan")

    def get_params(self):
        """
        解析 JSON 格式的處理參數。
        
        Returns:
            dict: 參數字典，解析失敗返回空字典
            
        Example:
            >>> audio.params = '{"segment_duration": 2.0, "overlap": 50}'
            >>> audio.get_params()
            {'segment_duration': 2.0, 'overlap': 50}
        """
        try:
            return json.loads(self.params) if self.params else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    # 屬性別名（向後相容舊版程式碼）
    @property
    def original_filename(self):
        """別名：原始檔案名稱"""
        return self.file_name
    
    @property
    def upload_timestamp(self):
        """別名：上傳時間戳記"""
        return self.record_time


# ============================================================================
# 分析結果模型（檢測資料）
# ============================================================================

class CetaceanInfo(db.Model):
    """
    鯨豚檢測資訊表（表七）。
    
    儲存鯨豚聲音事件的檢測結果，包含時間範圍、類型與辨識方式。
    
    Attributes:
        id (int): 主鍵
        audio_id (int): 所屬音檔 ID
        start_sample (int): 起始取樣點位置
        end_sample (int): 結束取樣點位置
        event_duration (int): 持續時間（秒）
        event_type (int): 事件類型（對應表八：0=未知, 1=上升型, 2=下降型...）
        detect_type (int): 辨識方式（0=人工標記, 1=AI辨識, 2=系統自動切割）
        
    Note:
        - event_type 對應專案定義的鯨豚聲紋分類表（表八）
        - detect_type=2 表示初始自動切割，尚未經人工或 AI 確認
    """
    __tablename__ = 'cetacean_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    audio_id = db.Column(db.Integer, db.ForeignKey('audio_info.id'), nullable=False, comment='所屬音檔')
    start_sample = db.Column(db.Integer, nullable=True, comment='起始位置')
    end_sample = db.Column(db.Integer, nullable=True, comment='結束位置')
    event_duration = db.Column(db.Integer, nullable=True, comment='持續時間(s)')
    event_type = db.Column(db.Integer, default=0, comment='類型代碼')
    detect_type = db.Column(db.Integer, default=2, comment='辨識方式(0=人工,1=AI,2=自動)')


class ShipInfo(db.Model):
    """
    船舶噪音檢測資訊表（表九）。
    
    紀錄船舶噪音事件，用於環境噪音分析。
    
    Attributes:
        id (int): 主鍵
        audio_id (int): 所屬音檔 ID
        start_sample (int): 起始取樣點
        end_sample (int): 結束取樣點
        event_duration (int): 持續時間（秒）
        event_type (int): 船舶類型代碼
    """
    __tablename__ = 'ship_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    audio_id = db.Column(db.Integer, db.ForeignKey('audio_info.id'), nullable=False, comment='所屬音檔')
    start_sample = db.Column(db.Integer, nullable=True, comment='起始位置')
    end_sample = db.Column(db.Integer, nullable=True, comment='結束位置')
    event_duration = db.Column(db.Integer, nullable=True, comment='持續時間(s)')
    event_type = db.Column(db.Integer, nullable=True, comment='類型代碼')


class TurbineInfo(db.Model):
    """
    風機噪音檢測資訊表（表十一）。
    
    紀錄離岸風機產生的噪音事件。
    
    Attributes:
        id (int): 主鍵
        audio_id (int): 所屬音檔 ID
        start_sample (int): 起始取樣點
        end_sample (int): 結束取樣點
        event_duration (int): 持續時間（秒）
        event_type (int): 風機類型代碼
    """
    __tablename__ = 'turbine_info'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    audio_id = db.Column(db.Integer, db.ForeignKey('audio_info.id'), nullable=False, comment='所屬音檔')
    start_sample = db.Column(db.Integer, nullable=True, comment='起始位置')
    end_sample = db.Column(db.Integer, nullable=True, comment='結束位置')
    event_duration = db.Column(db.Integer, nullable=True, comment='持續時間(s)')
    event_type = db.Column(db.Integer, nullable=True, comment='類型代碼')


# ============================================================================
# 系統檔案模型（分析結果儲存）
# ============================================================================

class Result(db.Model):
    """
    分析結果檔案表。
    
    儲存系統處理音檔後產生的實體檔案資訊（頻譜圖、切割音檔）。
    每個 AudioInfo 會產生多個 Result（對應每個切割片段）。
    
    Attributes:
        id (int): 主鍵
        upload_id (int): 所屬音檔 ID
        audio_filename (str): 切割音檔檔名（可選）
        spectrogram_filename (str): 顯示用頻譜圖檔名（帶座標軸）
        spectrogram_training_filename (str): 訓練用頻譜圖檔名（無座標軸）
        label_id (int): 關聯的標籤 ID
        bbox_annotations (list[BBoxAnnotation]): 此頻譜圖的框選標記
        
    Properties:
        audio_url: 音檔完整 URL
        spectrogram_url: 顯示用頻譜圖完整 URL
        spectrogram_training_url: 訓練用頻譜圖完整 URL
    """
    __tablename__ = 'results'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    upload_id = db.Column(db.Integer, db.ForeignKey('audio_info.id'), nullable=False, comment='所屬音檔')
    audio_filename = db.Column(db.String(255), nullable=True, comment='音檔檔名')
    spectrogram_filename = db.Column(db.String(255), nullable=False, comment='頻譜圖(帶軸)')
    spectrogram_training_filename = db.Column(db.String(255), nullable=False, comment='頻譜圖(無軸)')
    label_id = db.Column(db.Integer, db.ForeignKey('labels.id'), nullable=True, comment='標籤ID')
    
    # 關聯
    label = db.relationship('Label', backref='results')

    @property
    def audio_url(self):
        """
        取得切割音檔的完整相對路徑。
        
        Returns:
            str: 音檔 URL，若無音檔返回 None
        """
        if self.audio_filename:
            return f"{self.audio_info.result_path}/{self.audio_filename}"
        return None

    @property
    def spectrogram_url(self):
        """取得顯示用頻譜圖的完整相對路徑（帶座標軸）"""
        return f"{self.audio_info.result_path}/{self.spectrogram_filename}"

    @property
    def spectrogram_training_url(self):
        """取得訓練用頻譜圖的完整相對路徑（無座標軸）"""
        return f"{self.audio_info.result_path}/{self.spectrogram_training_filename}"


# ============================================================================
# 輔助系統模型（標籤與訓練）
# ============================================================================

class Label(db.Model):
    """
    標籤管理表。
    
    對應表八的事件類型定義，用於分類訓練與查詢。
    
    Attributes:
        id (int): 主鍵
        name (str): 標籤名稱（唯一，如「whale_upsweep」）
        description (str): 標籤描述
    """
    __tablename__ = 'labels'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    name = db.Column(db.String(100), nullable=False, unique=True, comment='標籤名稱')
    description = db.Column(db.String(255), nullable=True, comment='標籤描述')


class TrainingRun(db.Model):
    """
    AI 模型訓練任務記錄表。
    
    紀錄每次模型訓練的參數、狀態與結果。
    
    Attributes:
        id (int): 主鍵（任務 ID）
        timestamp (datetime): 任務建立時間
        status (str): 訓練狀態（PENDING, RUNNING, SUCCESS, FAILURE）
        results_path (str): 訓練結果資料夾路徑
        params (str): JSON 格式的訓練參數
        metrics (str): JSON 格式的訓練指標
        progress (int): 訓練進度（0-100%）
        
    Methods:
        get_params(): 解析訓練參數
        get_metrics(): 解析訓練指標
        get_model_display_name(): 取得模型友善名稱
        
    Example:
        >>> run = TrainingRun.query.get(1)
        >>> params = run.get_params()
        >>> print(f"Model:: {run.get_model_display_name()}, Epochs: {params['epochs']}")
    """
    __tablename__ = 'training_runs'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    timestamp = db.Column(db.DateTime, default=lambda: datetime.now(TAIPEI_TZ), comment='建立時間')
    status = db.Column(db.String(50), default='PENDING', comment='狀態')
    results_path = db.Column(db.String(255), nullable=True, comment='結果路徑')
    params = db.Column(db.Text, nullable=True, comment='參數(JSON)')
    metrics = db.Column(db.Text, nullable=True, comment='指標(JSON)')
    progress = db.Column(db.Integer, default=0, nullable=False, comment='進度(%)')

    def get_params(self):
        """
        解析訓練參數 JSON。
        
        Returns:
            dict: 參數字典，包含 model_type, epochs, batch_size 等
        """
        if self.params:
            try:
                return json.loads(self.params)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}

    def get_metrics(self):
        """
        解析訓練指標 JSON。
        
        Returns:
            dict: 指標字典，包含 accuracy, loss 等
        """
        if self.metrics:
            try:
                return json.loads(self.metrics)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    def get_model_display_name(self):
        """
        取得模型的友善顯示名稱。
        
        Returns:
            str: 模型顯示名稱（如「YOLOv8n」）
        """
        model_names = {
            'yolov8n-cls': 'YOLOv8n',
            'yolov8s-cls': 'YOLOv8s',
            'resnet18': 'ResNet18',
            'efficientnet_b0': 'EfficientNet-B0'
        }
        params = self.get_params()
        model_type = params.get('model_type', 'yolov8n-cls')
        return model_names.get(model_type, model_type)


# ============================================================================
# 進階標記模型（框選標註）
# ============================================================================

class BBoxAnnotation(db.Model):
    """
    框選標記資料表。
    
    儲存進階標記頁面在頻譜圖上繪製的矩形框標記。
    座標使用百分比（0-1）表示，不受圖片尺寸影響。
    
    Attributes:
        id (int): 主鍵
        result_id (int): 關聯的頻譜圖 ID
        label (str): 標籤名稱（如「whale_upsweep」）
        x (float): 左上角 X 座標（百分比，0-1）
        y (float): 左上角 Y 座標（百分比，0-1）
        width (float): 寬度（百分比，0-1）
        height (float): 高度（百分比，0-1）
        
    Note:
        - 座標系統：左上角為原點 (0, 0)
        - Y 軸方向：往下為正（頻譜圖中 Y=0 是高頻，Y=1 是低頻）
        - 百分比座標確保標記在不同螢幕尺寸下保持一致
        
    Example:
        >>> # 標記一個位於頻譜圖中央的框
        >>> bbox = BBoxAnnotation(
        ...     result_id=1,
        ...     label='dolphin_click',
        ...     x=0.3, y=0.2,  # 左上角
        ...     width=0.4, height=0.3  # 寬高
        ... )
    """
    __tablename__ = 'bbox_annotations'
    
    id = db.Column(db.Integer, primary_key=True, comment='主鍵')
    result_id = db.Column(db.Integer, db.ForeignKey('results.id'), nullable=False, comment='頻譜圖ID')
    label = db.Column(db.String(100), nullable=False, comment='標籤')
    x = db.Column(db.Float, nullable=False, comment='X座標(%)')
    y = db.Column(db.Float, nullable=False, comment='Y座標(%)')
    width = db.Column(db.Float, nullable=False, comment='寬度(%)')
    height = db.Column(db.Float, nullable=False, comment='高度(%)')

    # 關聯（刪除頻譜圖時一併刪除框選標記）
    result = db.relationship('Result', backref=db.backref('bbox_annotations', cascade='all, delete-orphan'))