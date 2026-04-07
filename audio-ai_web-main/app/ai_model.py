"""
AI 模型推論模組。

此模組負責：
1. 載入訓練好的 YOLOv8 分類模型
2. 對頻譜圖影像執行推論
3. 管理模型的隨需載入（Lazy Loading）

設計模式：
- Singleton 模式：全域共用單一模型實例
- 隨需載入：第一次呼叫時才載入模型，避免啟動時的資源消耗
- 容錯機制：模型不存在時不會中斷程式執行

模型部署流程：
1. 使用平台訓練功能產生 best.pt 模型檔案
2. 將 best.pt 複製到 app/models/ 目錄
3. 系統自動在首次推論時載入模型
4. 後續推論重用已載入的模型實例

技術規格：
- 模型格式：YOLOv8 Classification (.pt)
- 輸入：頻譜圖影像 (PNG/JPG)
- 輸出：預測類別與信心度
- 運算裝置：自動偵測 CUDA GPU，否則使用 CPU
"""

from ultralytics import YOLO
import torch
import os

# ============================================================================
# 全域配置
# ============================================================================

# 自動偵測可用的運算裝置（GPU 優先，否則使用 CPU）
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print(f"AI 模型將使用運算裝置: {DEVICE}")

# 模型檔案路徑
# 注意：需要手動將訓練好的 best.pt 檔案放在此路徑
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "best.pt")

# 全域模型實例（初始為 None，採用隨需載入）
model = None


# ============================================================================
# 推論函式
# ============================================================================

def run_inference(image_path):
    """
    對指定的頻譜圖影像執行 YOLOv8 分類推論。
    
    採用隨需載入機制：
    - 首次呼叫時載入模型到記憶體
    - 後續呼叫重用已載入的模型
    - 模型檔案不存在時返回空結果（不中斷程式）
    
    Args:
        image_path (str): 頻譜圖影像的完整檔案路徑（支援 PNG/JPG 格式）
        
    Returns:
        list: 預測結果列表
            - 成功: [{'label': '預測類別名稱', 'confidence': 0.99}]
            - 失敗或無模型: []
            
    Example:
        >>> result = run_inference('/path/to/spectrogram.png')
        >>> if result:
        ...     print(f"預測類別: {result[0]['label']}")
        ...     print(f"信心度: {result[0]['confidence']}")
    
    Note:
        - 函式設計為容錯，即使模型不存在也不會拋出異常
        - verbose=False 避免在 console 印出過多日誌
        - 只返回 Top-1 預測結果（信心度最高的類別）
    """
    global model

    # ------------------------------------------------------------------------
    # 模型隨需載入
    # ------------------------------------------------------------------------
    if model is None and os.path.exists(MODEL_PATH):
        try:
            print(f"正在載入 AI 模型: {MODEL_PATH}")
            model = YOLO(MODEL_PATH)
            model.to(DEVICE)
            print("AI 模型載入成功")
        except Exception as e:
            print(f"AI 模型載入失敗: {e}")
            return []

    # ------------------------------------------------------------------------
    # 檢查模型可用性
    # ------------------------------------------------------------------------
    if model is None:
        # 模型檔案不存在，靜默返回空結果
        # 註解此行以避免日誌過多
        # print("AI 模型不可用，跳過推論")
        return []

    # ------------------------------------------------------------------------
    # 執行推論
    # ------------------------------------------------------------------------
    try:
        # 執行預測（verbose=False 避免 console 輸出）
        results = model(image_path, verbose=False)
        
        # 處理分類模型的預測結果
        if results:
            result = results[0]  # 取得第一張圖片的結果
            
            if result.probs is not None:
                # 提取 Top-1 預測
                top1_index = result.probs.top1          # 最高信心度的類別索引
                top1_confidence = result.probs.top1conf.item()  # 信心度值
                label = model.names[top1_index]         # 類別名稱
                
                # 返回格式化的結果
                return [{
                    "label": label,
                    "confidence": round(top1_confidence, 4)
                }]
        
        return []
        
    except Exception as e:
        print(f"推論過程發生錯誤 (影像: '{image_path}'): {e}")
        return []


# ============================================================================
# 啟動檢查
# ============================================================================

# 應用程式啟動時檢查模型檔案是否存在
if not os.path.exists(MODEL_PATH):
    print(f"警告: 找不到模型檔案 '{MODEL_PATH}'")
    print("推論功能將被停用。請訓練模型後將 best.pt 放置於 app/models/ 目錄。")
