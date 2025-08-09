# Cloud Run 版本的 Dockerfile

# 使用官方 Python 精簡版映像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 設定環境變數 (提前設定)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 複製 requirements.txt 並安裝 Python 依賴 (利用 Docker 快取層)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 建立非 root 用戶以提升安全性 (在複製檔案前建立)
RUN useradd --create-home --shell /bin/bash --uid 1000 appuser

# 複製應用程式碼
COPY . .

# 設定檔案權限並切換用戶
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

# 切換到非 root 用戶
USER appuser

# 暴露端口 (Cloud Run 會自動設定 PORT 環境變數)
EXPOSE 8080

# 健康檢查 (Cloud Run 實際上不需要，但有助於本地測試)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

# 啟動指令 (Cloud Run 使用 PORT 環境變數)
CMD exec uvicorn fastapi_app_cloud:app --host 0.0.0.0 --port ${PORT:-8080}