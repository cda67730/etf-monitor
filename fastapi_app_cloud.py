# fastapi_app_cloud.py - 真正的修正版本
import os
import logging
import traceback
import hashlib
import secrets
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Request, Form, Depends, HTTPException, Cookie, Query
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

# ============ 環境配置 ============
class Settings:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.debug = os.getenv("DEBUG", "true").lower() == "true"
        self.allowed_hosts = os.getenv("ALLOWED_HOSTS", "*").split(",")
        self.port = int(os.getenv("PORT", 8080))
        self.scheduler_token = os.getenv("SCHEDULER_TOKEN", "default-secret-token")
        
        # 安全設定
        self.web_password = os.getenv("WEB_PASSWORD", "etf2024")
        self.session_secret = os.getenv("SESSION_SECRET", secrets.token_hex(32))
        self.session_timeout = int(os.getenv("SESSION_TIMEOUT", "28800"))
        
        # 流量限制
        self.rate_limit_requests = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
        self.api_daily_limit = int(os.getenv("API_DAILY_LIMIT", "1000"))

settings = Settings()

# ============ 日誌配置 ============
logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============ 數據庫初始化 ============
# 替換 fastapi_app_cloud.py 中的數據庫初始化部分（第 41-49行）

# ============ 數據庫初始化 ============
logger.info("開始初始化數據庫配置...")

# 檢查環境變數
database_url = os.getenv("DATABASE_URL")
logger.info(f"DATABASE_URL 環境變數: {database_url[:50] if database_url else 'None'}...")

try:
    from database_config import db_config
    from improved_etf_scraper_cloud import ETFHoldingsScraper
    
    logger.info(f"database_config 模組導入成功")
    logger.info(f"初始檢測數據庫類型: {db_config.db_type}")
    logger.info(f"使用的數據庫 URL: {db_config.database_url[:50] if hasattr(db_config, 'database_url') else 'Unknown'}...")
    
    # 測試數據庫連接
    try:
        with db_config.get_connection() as conn:
            logger.info("數據庫連接測試成功")
    except Exception as e:
        logger.error(f"數據庫連接測試失敗: {e}")
    
    logger.info(f"✅ 成功初始化數據庫配置 - 最終類型: {db_config.db_type}")
    
except Exception as e:
    logger.error(f"❌ 數據庫初始化失敗: {e}")
    logger.error(traceback.format_exc())
    db_config = None

# ============ FastAPI 應用初始化 ============
def get_app_title():
    """安全獲取應用標題"""
    try:
        if db_config:
            return f"ETF持股明細監控系統 (Cloud Run版本 - {db_config.db_type.upper()})"
        else:
            return "ETF持股明細監控系統 (Cloud Run版本)"
    except:
        return "ETF持股明細監控系統 (Cloud Run版本)"

app = FastAPI(
    title=get_app_title(),
    debug=settings.debug,
    version="cloud-run-db-1.0"
)

# ============ 模板和靜態文件 ============
try:
    templates = Jinja2Templates(directory="templates")
    logger.info("模板目錄初始化成功")
except Exception as e:
    logger.warning(f"模板目錄初始化失敗: {e}")
    templates = None

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
    logger.info("靜態文件目錄掛載成功")

# ============ 中間件配置 ============
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.allowed_hosts if settings.environment == "production" else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ============ 初始化爬蟲 ============
try:
    scraper = ETFHoldingsScraper() if db_config else None
    if scraper:
        logger.info("ETF爬蟲初始化成功")
except Exception as e:
    logger.warning(f"ETF爬蟲初始化失敗: {e}")
    scraper = None

# ============ 會話管理 ============
class SessionManager:
    def __init__(self):
        self.sessions = {}
        
    def create_session(self, request: Request) -> str:
        """創建新會話"""
        session_id = secrets.token_urlsafe(32)
        client_ip = self.get_client_ip(request)
        
        self.sessions[session_id] = {
            "created_at": datetime.now(),
            "last_access": datetime.now(),
            "ip": client_ip,
            "authenticated": True
        }
        
        self.cleanup_expired_sessions()
        logger.info(f"✅ 創建會話: {session_id[:8]}... IP: {client_ip}")
        return session_id
    
    def validate_session(self, session_id: str, request: Request) -> bool:
        """驗證會話"""
        client_ip = self.get_client_ip(request)
        
        if not session_id:
            logger.debug(f"❌ 會話驗證失敗: 無session_id, IP: {client_ip}")
            return False
            
        if session_id not in self.sessions:
            logger.debug(f"❌ 會話不存在: {session_id[:8] if session_id else 'None'}..., IP: {client_ip}")
            return False
            
        session = self.sessions[session_id]
        
        # 檢查過期
        if datetime.now() - session["created_at"] > timedelta(seconds=settings.session_timeout):
            logger.info(f"⏰ 會話過期: {session_id[:8]}...")
            del self.sessions[session_id]
            return False
        
        # 更新訪問時間
        session["last_access"] = datetime.now()
        logger.debug(f"✅ 會話有效: {session_id[:8]}... IP: {client_ip}")
        return True
    
    def cleanup_expired_sessions(self):
        """清理過期會話"""
        now = datetime.now()
        expired = [
            sid for sid, session in self.sessions.items()
            if now - session["created_at"] > timedelta(seconds=settings.session_timeout)
        ]
        
        for sid in expired:
            logger.info(f"🧹 清理過期會話: {sid[:8]}...")
            del self.sessions[sid]
    
    def get_client_ip(self, request: Request) -> str:
        """獲取客戶端IP"""
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
            
        return getattr(request.client, 'host', 'unknown')

# ============ 流量限制 ============
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
        self.api_requests = defaultdict(int)
        self.api_reset_time = defaultdict(datetime)
    
    def check_rate_limit(self, request: Request, limit_type: str = "web") -> bool:
        """檢查流量限制"""
        client_ip = session_manager.get_client_ip(request)
        now = datetime.now()
        
        if limit_type == "web":
            return self._check_hourly_limit(client_ip, now)
        elif limit_type == "api":
            return self._check_daily_api_limit(client_ip, now)
        
        return True
    
    def _check_hourly_limit(self, client_ip: str, now: datetime) -> bool:
        """檢查每小時限制"""
        cutoff_time = now - timedelta(hours=1)
        self.requests[client_ip] = [
            timestamp for timestamp in self.requests[client_ip]
            if timestamp > cutoff_time
        ]
        
        if len(self.requests[client_ip]) >= settings.rate_limit_requests:
            return False
        
        self.requests[client_ip].append(now)
        return True
    
    def _check_daily_api_limit(self, client_ip: str, now: datetime) -> bool:
        """檢查每日API限制"""
        if client_ip not in self.api_reset_time or now.date() > self.api_reset_time[client_ip].date():
            self.api_requests[client_ip] = 0
            self.api_reset_time[client_ip] = now
        
        if self.api_requests[client_ip] >= settings.api_daily_limit:
            return False
        
        self.api_requests[client_ip] += 1
        return True
    
    def get_remaining_requests(self, request: Request, limit_type: str = "web") -> dict:
        """獲取剩餘請求次數"""
        client_ip = session_manager.get_client_ip(request)
        
        if limit_type == "web":
            used = len(self.requests[client_ip])
            return {
                "limit": settings.rate_limit_requests,
                "used": used,
                "remaining": max(0, settings.rate_limit_requests - used),
                "reset_time": datetime.now() + timedelta(hours=1)
            }
        elif limit_type == "api":
            used = self.api_requests[client_ip]
            return {
                "limit": settings.api_daily_limit,
                "used": used,
                "remaining": max(0, settings.api_daily_limit - used),
                "reset_time": datetime.now().replace(hour=0, minute=0, second=0) + timedelta(days=1)
            }

# 初始化管理器
session_manager = SessionManager()
rate_limiter = RateLimiter()

# ============ 安全檢查函數 ============
def verify_password(input_password: str) -> bool:
    """驗證密碼"""
    return input_password == settings.web_password

async def check_authentication(request: Request) -> bool:
    """檢查認證"""
    session_id = request.cookies.get("session_id")
    client_ip = session_manager.get_client_ip(request)
    
    logger.debug(f"🔐 認證檢查: {request.url.path}, session_id: {session_id[:8] if session_id else 'None'}..., IP: {client_ip}")
    
    if not session_manager.validate_session(session_id, request):
        logger.warning(f"❌ 認證失敗: {request.url.path}, IP: {client_ip}")
        return False
    
    logger.debug(f"✅ 認證成功: {request.url.path}, IP: {client_ip}")
    return True

async def check_rate_limit_middleware(request: Request):
    """檢查流量限制"""
    if request.url.path in ["/health", "/login", "/logout", "/favicon.ico", "/trigger-scrape", "/debug/session", "/static"]:
        return True
    
    limit_type = "api" if request.url.path.startswith("/api/") else "web"
    
    if not rate_limiter.check_rate_limit(request, limit_type):
        remaining = rate_limiter.get_remaining_requests(request, limit_type)
        raise HTTPException(
            status_code=429, 
            detail=f"Rate limit exceeded. Try again after {remaining['reset_time']}"
        )
    
    return True

# ============ 登錄頁面模板 ============
def get_login_html_template() -> str:
    """安全獲取登錄頁面模板"""
    try:
        db_type = db_config.db_type.upper() if db_config else "UNKNOWN"
    except Exception as e:
        logger.warning(f"獲取數據庫類型失敗: {e}")
        db_type = "UNKNOWN"
    
    return """<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>登錄 - ETF監控系統</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        body { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
        }
        .login-container { 
            max-width: 400px; 
            margin: 0 auto; 
            padding: 40px;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        }
        .brand { 
            text-align: center; 
            margin-bottom: 30px; 
            color: #667eea;
        }
        .debug-info {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 10px;
            margin-top: 15px;
            font-size: 0.8em;
            border: 1px solid #dee2e6;
        }
        .db-info {
            background: #e8f5e8;
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 15px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="login-container">
            <div class="brand">
                <h3><i class="fas fa-shield-alt"></i> ETF監控系統</h3>
                <p class="text-muted">安全登錄驗證</p>
            </div>
            
            <div class="db-info">
                <small><i class="fas fa-database"></i> 使用數據庫: <strong>""" + db_type + """</strong></small>
            </div>
            
            <!-- ERROR_PLACEHOLDER -->
            
            <form method="post" action="/login" id="login-form">
                <div class="mb-3">
                    <label for="password" class="form-label">
                        <i class="fas fa-key"></i> 訪問密碼
                    </label>
                    <input type="password" class="form-control" id="password" name="password" required autofocus placeholder="請輸入訪問密碼">
                </div>
                <button type="submit" class="btn btn-primary w-100">
                    <i class="fas fa-sign-in-alt"></i> 登錄系統
                </button>
            </form>
            
            <div class="debug-info">
                <strong>調試信息:</strong><br>
                <small>
                    URL: <span id="current-url"></span><br>
                    Cookies: <span id="cookies-display">檢查中...</span><br>
                    狀態: <span id="status-display">準備中</span>
                </small>
            </div>
        </div>
    </div>
    
    <script>
        function updateDebugInfo() {
            document.getElementById('current-url').textContent = window.location.href;
            document.getElementById('cookies-display').textContent = document.cookie || '無 Cookies';
        }
        
        document.getElementById('login-form').addEventListener('submit', function(e) {
            document.getElementById('status-display').textContent = '提交中...';
            
            setTimeout(function() {
                updateDebugInfo();
                document.getElementById('status-display').textContent = '檢查 Cookie 設置...';
            }, 1000);
        });
        
        updateDebugInfo();
        setInterval(updateDebugInfo, 2000);
    </script>
</body>
</html>"""

# ============ 中間件 ============
@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """統一安全中間件"""
    start_time = time.time()
    
    try:
        client_ip = session_manager.get_client_ip(request)
        logger.debug(f"📥 請求: {request.method} {request.url.path} from {client_ip}")
        
        # 1. 檢查流量限制
        await check_rate_limit_middleware(request)
        
        # 2. 公開路由，跳過認證
        public_paths = ["/health", "/login", "/logout", "/favicon.ico", "/trigger-scrape", "/debug/session", "/static"]
        
        # 檢查是否為靜態文件路徑
        is_public = any(request.url.path.startswith(path) for path in public_paths)
        
        if is_public:
            logger.debug(f"🚪 公開路由: {request.url.path}")
            response = await call_next(request)
        else:
            # 3. 需要認證的路由
            if await check_authentication(request):
                response = await call_next(request)
                logger.debug(f"✅ 認證通過，處理請求: {request.url.path}")
            else:
                # 認證失敗，重定向到登錄
                logger.info(f"🔄 重定向到登錄: {request.url.path} from {client_ip}")
                response = RedirectResponse(url="/login", status_code=302)
        
        # 4. 添加安全頭部
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        
        # 5. 添加流量限制信息
        remaining = rate_limiter.get_remaining_requests(request)
        response.headers["X-RateLimit-Remaining"] = str(remaining["remaining"])
        
        # 6. 記錄日誌
        process_time = time.time() - start_time
        logger.info(f"📊 {request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s - {client_ip}")
        
        return response
        
    except HTTPException as e:
        if e.status_code == 429:
            return HTMLResponse(
                content="<h1>請求過於頻繁</h1><p><a href='/login'>返回登錄</a></p>",
                status_code=429
            )
        else:
            logger.error(f"HTTP異常: {e}")
            return RedirectResponse(url="/login", status_code=302)
    
    except Exception as e:
        logger.error(f"中間件錯誤: {e}")
        logger.error(traceback.format_exc())
        return HTMLResponse(
            content="<h1>系統錯誤</h1><p><a href='/login'>返回登錄</a></p>",
            status_code=500
        )

# ============ 認證路由 ============
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    """登錄頁面"""
    client_ip = session_manager.get_client_ip(request)
    logger.info(f"🔑 顯示登錄頁面, IP: {client_ip}")
    
    html_content = get_login_html_template()
    
    if error:
        error_html = f'<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> {error}</div>'
        html_content = html_content.replace("<!-- ERROR_PLACEHOLDER -->", error_html)
    else:
        html_content = html_content.replace("<!-- ERROR_PLACEHOLDER -->", "")
    
    return HTMLResponse(content=html_content)

@app.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    """處理登錄提交"""
    try:
        client_ip = session_manager.get_client_ip(request)
        logger.info(f"🔐 登錄嘗試: IP={client_ip}")
        
        # 檢查密碼
        if not verify_password(password):
            logger.warning(f"❌ 密碼錯誤, IP: {client_ip}")
            return RedirectResponse(url="/login?error=密碼錯誤", status_code=302)
        
        # 創建會話
        session_id = session_manager.create_session(request)
        logger.info(f"✅ 登錄成功: IP={client_ip}, Session={session_id[:8]}...")
        
        # 創建響應並設置Cookie
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(
            key="session_id",
            value=session_id,
            max_age=settings.session_timeout,
            httponly=False,
            secure=False,
            samesite="lax",
            path="/"
        )
        
        # 防止緩存
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        
        logger.info(f"🍪 Cookie 設置完成: session_id={session_id[:8]}...")
        return response
        
    except Exception as e:
        logger.error(f"登錄處理錯誤: {e}")
        logger.error(traceback.format_exc())
        return RedirectResponse(url="/login?error=系統錯誤", status_code=302)

@app.post("/logout")
async def logout(request: Request):
    """登出"""
    session_id = request.cookies.get("session_id")
    client_ip = session_manager.get_client_ip(request)
    
    if session_id and session_id in session_manager.sessions:
        del session_manager.sessions[session_id]
        logger.info(f"🚪 用戶登出: IP={client_ip}, Session={session_id[:8]}...")
    
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_id", path="/")
    return response

# ============ 調試端點 ============
@app.get("/debug/session")
async def debug_session_info(request: Request):
    """調試會話信息"""
    session_id = request.cookies.get("session_id")
    client_ip = session_manager.get_client_ip(request)
    
    db_info = {}
    try:
        if db_config:
            db_info = {
                "database_type": db_config.db_type,
                "database_url": db_config.database_url[:50] + "..." if len(db_config.database_url) > 50 else db_config.database_url
            }
    except Exception as e:
        db_info = {"database_error": str(e)}
    
    return {
        "session_id": session_id[:8] if session_id else None,
        "session_exists": session_id in session_manager.sessions if session_id else False,
        "client_ip": client_ip,
        "cookies": list(request.cookies.keys()),
        "total_sessions": len(session_manager.sessions),
        "current_time": datetime.now().isoformat(),
        "session_timeout": settings.session_timeout,
        "scraper_status": "available" if scraper else "unavailable",
        "templates_status": "available" if templates else "unavailable",
        **db_info
    }

# ============ Cloud Run 端點 ============
@app.get("/health")
async def health_check():
    """健康檢查"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "sessions": len(session_manager.sessions),
        "version": "cloud-run-db-1.0",
        "components": {
            "database": "connected" if db_config else "unavailable",
            "scraper": "available" if scraper else "unavailable", 
            "templates": "available" if templates else "unavailable"
        }
    }
    
    try:
        if db_config:
            health_status["database_type"] = db_config.db_type
    except:
        health_status["components"]["database"] = "error"
    
    return health_status

# ============ 完整的數據庫查詢類別 ============
class DatabaseQuery:
    """最終完善版本 - 解決所有發現的問題"""
    
    def __init__(self):
        self.etf_names = {
            '00981A': '統一台股增長主動式ETF',
            '00982A': '群益台灣精選強棒主動式ETF', 
            '00983A': '中信ARK創新主動式ETF',
            '00984A': '安聯台灣高息成長主動式ETF',
            '00985A': '野村台灣增強50主動式ETF'
        }
        self.db_available = db_config is not None
        if self.db_available:
            self.ensure_tables_exist()
    
    def ensure_tables_exist(self):
        """確保表存在"""
        if not self.db_available:
            logger.warning("數據庫不可用，跳過表創建")
            return
        
        try:
            holdings_table_sql = '''
                CREATE TABLE IF NOT EXISTS holdings_changes (
                    id SERIAL PRIMARY KEY,
                    etf_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    old_shares INTEGER DEFAULT 0,
                    new_shares INTEGER DEFAULT 0,
                    old_weight REAL DEFAULT 0.0,
                    new_weight REAL DEFAULT 0.0,
                    change_date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
            
            etf_holdings_sql = '''
                CREATE TABLE IF NOT EXISTS etf_holdings (
                    id SERIAL PRIMARY KEY,
                    etf_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    weight REAL NOT NULL,
                    shares INTEGER NOT NULL,
                    unit TEXT DEFAULT '股',
                    update_date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
            
            db_config.execute_query(holdings_table_sql)
            db_config.execute_query(etf_holdings_sql)
            
            logger.info("數據庫表檢查完成")
            
        except Exception as e:
            logger.error(f"數據庫初始化錯誤: {e}")
    
    def execute_query(self, query: str, params: tuple = (), fetch: str = "none"):
        """統一的查詢執行方法"""
        if not self.db_available:
            logger.warning("數據庫不可用")
            return [] if fetch == "all" else None
        
        try:
            return db_config.execute_query(query, params, fetch)
        except Exception as e:
            logger.error(f"執行查詢失敗: {e}")
            logger.error(f"查詢: {query[:200]}...")  # 限制日誌長度
            logger.error(f"參數: {params}")
            return [] if fetch == "all" else None
    
    def get_etf_name(self, etf_code: str) -> str:
        return self.etf_names.get(etf_code, etf_code)
    
    def get_available_dates(self):
        if not self.db_available:
            return []
        
        try:
            query = 'SELECT DISTINCT update_date FROM etf_holdings ORDER BY update_date DESC'
            results = self.execute_query(query, fetch="all")
            return [row['update_date'] for row in results] if results else []
        except Exception as e:
            logger.error(f"獲取日期錯誤: {e}")
            return []
    
    def get_etf_codes(self):
        return ['00981A', '00982A', '00983A', '00984A', '00985A']
    
    def get_etf_codes_with_names(self):
        return [{'code': code, 'name': self.get_etf_name(code)} for code in self.get_etf_codes()]
    
    def get_holdings_by_date(self, date: str) -> List[Dict[str, Any]]:
        """根據日期獲取所有ETF的持股"""
        if not self.db_available:
            return []
        
        try:
            query = '''
                SELECT etf_code, stock_code, stock_name, weight, shares, unit
                FROM etf_holdings 
                WHERE update_date = %s 
                ORDER BY etf_code, weight DESC
            '''
            results = self.execute_query(query, (date,), fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取日期持股錯誤: {e}")
            return []
    
    def get_holdings_by_etf(self, etf_code: str, date: str = None) -> List[Dict[str, Any]]:
        """根據ETF代碼獲取持股"""
        if not self.db_available:
            return []
        
        try:
            if date:
                query = '''
                    SELECT stock_code, stock_name, weight, shares, unit, update_date
                    FROM etf_holdings 
                    WHERE etf_code = %s AND update_date = %s 
                    ORDER BY weight DESC
                '''
                params = (etf_code, date)
            else:
                query = '''
                    SELECT stock_code, stock_name, weight, shares, unit, update_date
                    FROM etf_holdings 
                    WHERE etf_code = %s 
                    ORDER BY update_date DESC, weight DESC
                '''
                params = (etf_code,)
            
            results = self.execute_query(query, params, fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取ETF持股錯誤: {e}")
            return []
    
    def get_holdings_changes(self, etf_code: str = None, date: str = None) -> List[Dict[str, Any]]:
        """獲取持股變化"""
        if not self.db_available:
            return []
        
        try:
            base_query = '''
                SELECT etf_code, stock_code, stock_name, change_type, 
                       old_shares, new_shares, old_weight, new_weight, change_date
                FROM holdings_changes 
            '''
            
            conditions = []
            params = []
            
            if etf_code:
                conditions.append("etf_code = %s")
                params.append(etf_code)
            
            if date:
                conditions.append("change_date = %s")
                params.append(date)
            
            if conditions:
                query = base_query + "WHERE " + " AND ".join(conditions)
            else:
                query = base_query
            
            query += " ORDER BY change_date DESC, etf_code"
            
            results = self.execute_query(query, tuple(params), fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取持股變化錯誤: {e}")
            return []

    def get_new_holdings(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取新增持股 - 最終完善版本"""
        if not self.db_available:
            logger.warning("數據庫不可用，返回空列表")
            return []
        
        try:
            logger.info(f"🔍 查詢新增持股: date={date}, etf_code={etf_code}")
            
            # 如果沒有指定日期，使用最新日期
            if not date:
                dates = self.get_available_dates()
                if not dates:
                    logger.warning("沒有可用日期")
                    return []
                date = dates[0]
                logger.info(f"使用最新日期: {date}")
            
            # 構建查詢條件
            conditions = ["hc.change_type = %s", "hc.change_date = %s"]
            params = ['NEW', date]
            
            if etf_code:
                conditions.append("hc.etf_code = %s")
                params.append(etf_code)
            
            where_clause = " AND ".join(conditions)
            
            # 嘗試 JOIN 查詢 - 使用參數化查詢而非 f-string
            join_query = f'''
                SELECT h.etf_code, h.stock_code, h.stock_name, h.weight, h.shares, h.unit,
                       hc.change_type, hc.new_shares
                FROM holdings_changes hc
                JOIN etf_holdings h ON (
                    hc.etf_code = h.etf_code 
                    AND hc.stock_code = h.stock_code 
                    AND hc.change_date = h.update_date
                )
                WHERE {where_clause}
                ORDER BY hc.etf_code, h.weight DESC
            '''
            
            logger.info(f"執行 JOIN 查詢...")
            results = self.execute_query(join_query, tuple(params), fetch="all")
            
            if results:
                logger.info(f"✅ JOIN 查詢成功，找到 {len(results)} 筆新增持股")
                # 添加 ETF 名稱
                for result in results:
                    result['etf_name'] = self.get_etf_name(result['etf_code'])
                return results
            
            # 如果 JOIN 查詢沒有結果，嘗試分別查詢
            logger.info("JOIN 查詢無結果，嘗試分別查詢...")
            
            # 先從 holdings_changes 獲取 NEW 類型的記錄
            change_query = f'''
                SELECT etf_code, stock_code, stock_name, new_shares, change_date
                FROM holdings_changes
                WHERE {where_clause}
                ORDER BY etf_code, stock_code
            '''
            
            changes = self.execute_query(change_query, tuple(params), fetch="all")
            logger.info(f"變化記錄查詢結果: {len(changes) if changes else 0} 筆")
            
            if not changes:
                logger.warning("沒有找到新增類型的變化記錄")
                return []
            
            # 為每個變化記錄查找對應的持股信息
            new_holdings = []
            for change in changes:
                if not change:  # 額外的安全檢查
                    continue
                    
                holding_query = '''
                    SELECT etf_code, stock_code, stock_name, weight, shares, unit
                    FROM etf_holdings
                    WHERE etf_code = %s AND stock_code = %s AND update_date = %s
                '''
                
                holding = self.execute_query(
                    holding_query, 
                    (change['etf_code'], change['stock_code'], date), 
                    fetch="one"
                )
                
                if holding:
                    # 合併數據
                    combined = dict(holding)
                    combined['change_type'] = 'NEW'
                    combined['etf_name'] = self.get_etf_name(combined['etf_code'])
                    new_holdings.append(combined)
                else:
                    # 如果沒有對應的持股記錄，使用變化記錄的數據
                    fallback = {
                        'etf_code': change['etf_code'],
                        'etf_name': self.get_etf_name(change['etf_code']),
                        'stock_code': change['stock_code'],
                        'stock_name': change['stock_name'],
                        'weight': 0.0,  # 默認值
                        'shares': change['new_shares'],
                        'unit': '股',  # 默認值
                        'change_type': 'NEW'
                    }
                    new_holdings.append(fallback)
            
            logger.info(f"✅ 分別查詢成功，找到 {len(new_holdings)} 筆新增持股")
            return new_holdings
            
        except Exception as e:
            logger.error(f"❌ 查詢新增持股錯誤: {e}")
            logger.error(f"查詢參數: date={date}, etf_code={etf_code}")
            # ✅ 修正：確保 traceback 在文件頂部導入
            try:
                import traceback
                logger.error(f"錯誤堆棧: {traceback.format_exc()}")
            except ImportError:
                logger.error("無法導入 traceback 模組")
            return []

    def diagnose_new_holdings_data(self, date: str = None) -> Dict[str, Any]:
        """診斷新增持股數據的完整性 - 簡化版本"""
        if not self.db_available:
            return {"status": "database_unavailable"}
        
        diagnosis = {
            "status": "checking",
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # 如果沒有指定日期，使用最新日期
            if not date:
                dates = self.get_available_dates()
                if dates:
                    date = dates[0]
                    diagnosis["used_date"] = date
                else:
                    diagnosis["error"] = "no_available_dates"
                    return diagnosis
            
            # 檢查 holdings_changes 表
            changes_query = "SELECT COUNT(*) as count FROM holdings_changes WHERE change_date = %s"
            changes_result = self.execute_query(changes_query, (date,), fetch="one")
            diagnosis["total_changes"] = changes_result["count"] if changes_result else 0
            
            # 檢查 NEW 類型的變化
            new_changes_query = "SELECT COUNT(*) as count FROM holdings_changes WHERE change_date = %s AND change_type = 'NEW'"
            new_changes_result = self.execute_query(new_changes_query, (date,), fetch="one")
            diagnosis["new_changes"] = new_changes_result["count"] if new_changes_result else 0
            
            # 檢查 etf_holdings 表
            holdings_query = "SELECT COUNT(*) as count FROM etf_holdings WHERE update_date = %s"
            holdings_result = self.execute_query(holdings_query, (date,), fetch="one")
            diagnosis["total_holdings"] = holdings_result["count"] if holdings_result else 0
            
            diagnosis["status"] = "completed"
            
        except Exception as e:
            diagnosis["status"] = "error"
            diagnosis["error"] = str(e)
        
        return diagnosis

    def get_holdings_with_changes(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取持股明細並包含變化資料 - 優化版本"""
        if not self.db_available:
            return []
        
        try:
            # 構建查詢條件
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append("h.update_date = %s")
                params.append(date)
            else:
                # 如果沒有指定日期，使用最新日期
                where_conditions.append("h.update_date = (SELECT MAX(update_date) FROM etf_holdings)")
            
            if etf_code:
                where_conditions.append("h.etf_code = %s")
                params.append(etf_code)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # 使用 LEFT JOIN 一次性獲取持股和變化資料
            query = f'''
                SELECT 
                    h.etf_code,
                    h.stock_code,
                    h.stock_name,
                    h.weight,
                    h.shares,
                    h.unit,
                    h.update_date,
                    hc.change_type,
                    hc.old_shares,
                    hc.new_shares,
                    hc.old_weight,
                    hc.new_weight
                FROM etf_holdings h
                LEFT JOIN holdings_changes hc ON (
                    h.etf_code = hc.etf_code 
                    AND h.stock_code = hc.stock_code 
                    AND h.update_date = hc.change_date
                )
                {where_clause}
                ORDER BY h.etf_code, h.weight DESC
            '''
            
            results = self.execute_query(query, tuple(params), fetch="all")
            
            # 處理結果，計算變化數據
            holdings = []
            if results:
                for result in results:
                    holding = dict(result)
                    
                    # 計算股數變化
                    if holding.get('change_type'):
                        old_shares = holding.get('old_shares') or 0
                        new_shares = holding.get('new_shares') or 0
                        
                        if holding['change_type'] == 'NEW':
                            holding['shares_increase'] = new_shares
                            holding['shares_decrease'] = 0
                        elif holding['change_type'] == 'INCREASED':
                            holding['shares_increase'] = max(0, new_shares - old_shares)
                            holding['shares_decrease'] = 0
                        elif holding['change_type'] == 'DECREASED':
                            holding['shares_increase'] = 0
                            holding['shares_decrease'] = max(0, old_shares - new_shares)
                        elif holding['change_type'] == 'REMOVED':
                            holding['shares_increase'] = 0
                            holding['shares_decrease'] = old_shares
                        else:
                            holding['shares_increase'] = 0
                            holding['shares_decrease'] = 0
                    else:
                        # 無變化記錄
                        holding['change_type'] = None
                        holding['old_shares'] = holding.get('shares', 0)
                        holding['new_shares'] = holding.get('shares', 0)
                        holding['shares_increase'] = 0
                        holding['shares_decrease'] = 0
                    
                    holdings.append(holding)
            
            return holdings
            
        except Exception as e:
            logger.error(f"獲取持股變化資料錯誤: {e}")
            logger.error(f"查詢參數: date={date}, etf_code={etf_code}")
            return []
        
    def get_holdings_change_stats(self, holdings: List[Dict[str, Any]]) -> Dict[str, int]:
        """計算持股變化統計"""
        stats = {
            'total': len(holdings),
            'new_count': 0,
            'increased_count': 0, 
            'decreased_count': 0,
            'removed_count': 0,
            'no_change_count': 0
        }
        
        for holding in holdings:
            change_type = holding.get('change_type')
            if change_type == 'NEW':
                stats['new_count'] += 1
            elif change_type == 'INCREASED':
                stats['increased_count'] += 1
            elif change_type == 'DECREASED':
                stats['decreased_count'] += 1
            elif change_type == 'REMOVED':
                stats['removed_count'] += 1
            else:
                stats['no_change_count'] += 1
        
        return stats

    def get_decreased_holdings(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取減持股票"""
        if not self.db_available:
            return []
        
        try:
            base_query = '''
                SELECT etf_code, stock_code, stock_name, change_type,
                       old_shares, new_shares, old_weight, new_weight, change_date
                FROM holdings_changes
                WHERE change_type IN ('DECREASED', 'REMOVED')
            '''
            
            conditions = []
            params = []
            
            if date:
                conditions.append("change_date = %s")
                params.append(date)
            
            if etf_code:
                conditions.append("etf_code = %s")
                params.append(etf_code)
            
            if conditions:
                query = base_query + " AND " + " AND ".join(conditions)
            else:
                query = base_query
            
            query += " ORDER BY change_date DESC, etf_code, (old_shares - new_shares) DESC"
            
            results = self.execute_query(query, tuple(params), fetch="all")
            
            # 處理數據格式
            if results:
                for result in results:
                    if result:  # 額外安全檢查
                        result['etf_name'] = self.get_etf_name(result['etf_code'])
                        result['change_amount'] = max(0, result['old_shares'] - result['new_shares'])
                        # 轉換變化類型名稱
                        if result['change_type'] == 'REMOVED':
                            result['change_type'] = '完全移除'
                        elif result['change_type'] == 'DECREASED':
                            result['change_type'] = '減持'
            
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取減持股票錯誤: {e}")
            return []
    
    def get_cross_holdings(self, date: str = None) -> List[Dict[str, Any]]:
        """獲取跨ETF重複持股"""
        if not self.db_available:
            return []
        
        try:
            # 查找在同一日期被多個ETF持有的股票
            query = '''
                SELECT 
                    stock_code,
                    stock_name,
                    COUNT(DISTINCT etf_code) as etf_count,
                    SUM(shares) as total_shares
                FROM etf_holdings
                WHERE update_date = %s
                GROUP BY stock_code, stock_name
                HAVING COUNT(DISTINCT etf_code) > 1
                ORDER BY total_shares DESC
            '''
            
            if not date:
                # 如果沒有指定日期，使用最新日期
                dates = self.get_available_dates()
                if not dates:
                    return []
                date = dates[0]
            
            results = self.execute_query(query, (date,), fetch="all")
            
            # 為每個重複持股獲取詳細信息
            cross_holdings = []
            if results:
                for result in results:
                    if not result:  # 安全檢查
                        continue
                        
                    stock_code = result['stock_code']
                    
                    # 獲取當前持股詳情
                    detail_query = '''
                        SELECT etf_code, shares, weight
                        FROM etf_holdings
                        WHERE stock_code = %s AND update_date = %s
                        ORDER BY shares DESC
                    '''
                    details = self.execute_query(detail_query, (stock_code, date), fetch="all")
                    
                    # 獲取前一日持股（用於計算變化）
                    prev_query = '''
                        SELECT etf_code, shares
                        FROM etf_holdings
                        WHERE stock_code = %s AND update_date < %s
                        ORDER BY update_date DESC
                        LIMIT 10
                    '''
                    prev_holdings = self.execute_query(prev_query, (stock_code, date), fetch="all")
                    prev_dict = {h['etf_code']: h['shares'] for h in prev_holdings} if prev_holdings else {}
                    
                    # 計算變化
                    etf_details = []
                    total_increase = 0
                    total_decrease = 0
                    
                    if details:
                        for detail in details:
                            if not detail:  # 安全檢查
                                continue
                                
                            etf_code = detail['etf_code']
                            current_shares = detail['shares']
                            previous_shares = prev_dict.get(etf_code, 0)
                            change = current_shares - previous_shares
                            
                            if change > 0:
                                total_increase += change
                            elif change < 0:
                                total_decrease += abs(change)
                            
                            etf_details.append({
                                'etf_code': etf_code,
                                'etf_name': self.get_etf_name(etf_code),
                                'shares': current_shares,
                                'previous_shares': previous_shares,
                                'change': change,
                                'weight': detail['weight']
                            })
                    
                    cross_holdings.append({
                        'stock_code': result['stock_code'],
                        'stock_name': result['stock_name'],
                        'etf_count': result['etf_count'],
                        'total_shares': result['total_shares'],
                        'total_increase': total_increase,
                        'total_decrease': total_decrease,
                        'etf_details': etf_details
                    })
            
            return cross_holdings
            
        except Exception as e:
            logger.error(f"獲取跨ETF重複持股錯誤: {e}")
            return []
    
    def get_latest_holdings(self, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取最新持股"""
        if not self.db_available:
            return []
        
        try:
            if etf_code:
                query = '''
                    SELECT stock_code, stock_name, weight, shares, unit, update_date
                    FROM etf_holdings 
                    WHERE etf_code = %s AND update_date = (
                        SELECT MAX(update_date) FROM etf_holdings WHERE etf_code = %s
                    )
                    ORDER BY weight DESC
                '''
                params = (etf_code, etf_code)
            else:
                query = '''
                    SELECT etf_code, stock_code, stock_name, weight, shares, unit, update_date
                    FROM etf_holdings 
                    WHERE update_date = (SELECT MAX(update_date) FROM etf_holdings)
                    ORDER BY etf_code, weight DESC
                '''
                params = ()
            
            results = self.execute_query(query, params, fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取最新持股錯誤: {e}")
            return []
# 初始化數據庫查詢對象
db_query = DatabaseQuery()

def get_sort_icon(field: str, current_sort: str) -> str:
    """獲取排序圖標後綴"""
    if current_sort == f"{field}_desc":
        return "-down"  # fa-sort-down
    elif current_sort == f"{field}_asc":
        return "-up"    # fa-sort-up
    else:
        return ""       # fa-sort

def get_sort_display(sort_by: str) -> str:
    """獲取排序顯示文字"""
    sort_names = {
        'weight_desc': '權重(高→低)',
        'weight_asc': '權重(低→高)', 
        'increase_desc': '新增股數(多→少)',
        'increase_asc': '新增股數(少→多)',
        'decrease_desc': '減少股數(多→少)',
        'decrease_asc': '減少股數(少→多)',
        'etf_stock': 'ETF+股票代碼'
    }
    return sort_names.get(sort_by, '權重(高→低)')

def apply_holdings_sorting(holdings: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
    """應用持股排序邏輯 - 修正版"""
    if not holdings:
        return holdings
    
    try:
        # 確保所有記錄都有必要的排序欄位，設置默認值
        for holding in holdings:
            # 確保必要欄位存在
            holding.setdefault('shares_increase', 0)
            holding.setdefault('shares_decrease', 0)
            holding.setdefault('weight', 0.0)
            holding.setdefault('etf_code', '')
            holding.setdefault('stock_code', '')
            
            # 轉換None為0
            if holding['shares_increase'] is None:
                holding['shares_increase'] = 0
            if holding['shares_decrease'] is None:
                holding['shares_decrease'] = 0
            if holding['weight'] is None:
                holding['weight'] = 0.0
        
        # 應用排序
        if sort_by == "weight_desc":
            return sorted(holdings, key=lambda x: x['weight'], reverse=True)
        
        elif sort_by == "weight_asc":
            return sorted(holdings, key=lambda x: x['weight'], reverse=False)
        
        elif sort_by == "increase_desc":
            # 按新增股數降序，然後按權重降序
            return sorted(holdings, 
                         key=lambda x: (x['shares_increase'], x['weight']), 
                         reverse=True)
        
        elif sort_by == "increase_asc":
            # 按新增股數升序，然後按權重降序
            return sorted(holdings, 
                         key=lambda x: (x['shares_increase'], -x['weight']), 
                         reverse=False)
        
        elif sort_by == "decrease_desc":
            # 按減少股數降序，然後按權重降序
            return sorted(holdings, 
                         key=lambda x: (x['shares_decrease'], x['weight']), 
                         reverse=True)
        
        elif sort_by == "decrease_asc":
            # 按減少股數升序，然後按權重降序
            return sorted(holdings, 
                         key=lambda x: (x['shares_decrease'], -x['weight']), 
                         reverse=False)
        
        elif sort_by == "etf_stock":
            # 按ETF代碼，然後按股票代碼排序
            return sorted(holdings, 
                         key=lambda x: (x['etf_code'], x['stock_code']))
        
        else:
            # 默認按權重降序
            return sorted(holdings, key=lambda x: x['weight'], reverse=True)
            
    except Exception as e:
        logger.error(f"排序應用錯誤: {e}")
        logger.error(f"排序參數: {sort_by}")
        logger.error(f"資料筆數: {len(holdings)}")
        # 如果排序失敗，返回原始數據
        return holdings


# ============ API 路由 ============
@app.get("/api/holdings")
async def api_get_holdings(
    request: Request,
    etf_code: str = Query(None, description="ETF代碼"),
    date: str = Query(None, description="日期 (YYYY-MM-DD)")
):
    """API: 獲取持股明細"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not db_query.db_available:
            raise HTTPException(status_code=503, detail="Database unavailable")
        
        if etf_code and date:
            data = db_query.get_holdings_by_etf(etf_code, date)
        elif etf_code:
            data = db_query.get_holdings_by_etf(etf_code)
        elif date:
            data = db_query.get_holdings_by_date(date)
        else:
            data = db_query.get_latest_holdings()
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "etf_code": etf_code,
            "date": date,
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API持股查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/changes")
async def api_get_changes(
    request: Request,
    etf_code: str = Query(None, description="ETF代碼"),
    date: str = Query(None, description="日期 (YYYY-MM-DD)")
):
    """API: 獲取持股變化"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not db_query.db_available:
            raise HTTPException(status_code=503, detail="Database unavailable")
        
        data = db_query.get_holdings_changes(etf_code, date)
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "etf_code": etf_code,
            "date": date,
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API變化查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etfs")
async def api_get_etfs(request: Request):
    """API: 獲取ETF列表"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        etf_info = db_query.get_etf_codes_with_names()
        dates = db_query.get_available_dates()
        
        return {
            "status": "success",
            "etfs": etf_info,
            "available_dates": dates,
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API ETF列表錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 主要頁面路由 ============
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """首頁"""
    try:
        if not templates:
            return HTMLResponse(
                content="<h1>ETF監控系統</h1><p>模板引擎未初始化，請聯繫管理員</p>",
                status_code=500
            )
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        etf_info = db_query.get_etf_codes_with_names()
        
        return templates.TemplateResponse("index.html", {
            "request": request,
            "dates": dates,
            "etf_codes": etf_codes,
            "etf_info": etf_info,
            "current_date": dates[0] if dates else None,
            "database_type": db_config.db_type if db_config else "unavailable"
        })
    except Exception as e:
        logger.error(f"首頁錯誤: {e}")
        return HTMLResponse(
            content=f"<h1>系統錯誤</h1><p>錯誤詳情: {str(e)}</p><p><a href='/login'>返回登錄</a></p>",
            status_code=500
        )

@app.get("/holdings/{etf_code}")
async def holdings_detail(request: Request, etf_code: str, date: str = Query(None)):
    """持股明細頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        if etf_code not in db_query.get_etf_codes():
            raise HTTPException(status_code=404, detail="ETF not found")
        
        if not date:
            dates = db_query.get_available_dates()
            date = dates[0] if dates else None
        
        holdings = db_query.get_holdings_by_etf(etf_code, date)
        etf_name = db_query.get_etf_name(etf_code)
        available_dates = db_query.get_available_dates()
        
        return templates.TemplateResponse("holdings.html", {
            "request": request,
            "etf_code": etf_code,
            "etf_name": etf_name,
            "holdings": holdings,
            "current_date": date,
            "available_dates": available_dates,
            "database_type": db_config.db_type if db_config else "unavailable"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"持股明細頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/changes")
async def changes_page(request: Request, etf_code: str = Query(None), date: str = Query(None)):
    """持股變化頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        changes = db_query.get_holdings_changes(etf_code, date)
        etf_info = db_query.get_etf_codes_with_names()
        available_dates = db_query.get_available_dates()
        
        return templates.TemplateResponse("changes.html", {
            "request": request,
            "changes": changes,
            "etf_info": etf_info,
            "available_dates": available_dates,
            "selected_etf": etf_code,
            "selected_date": date,
            "database_type": db_config.db_type if db_config else "unavailable"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"變化頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/new-holdings", response_class=HTMLResponse)
async def new_holdings_page(request: Request, date: str = Query(None), etf_code: str = Query(None)):
    """新增持股頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        new_holdings = []
        if date:
            new_holdings = db_query.get_new_holdings(date, etf_code)
        
        return templates.TemplateResponse("new_holdings.html", {
            "request": request,
            "new_holdings": new_holdings,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"新增持股頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/decreased-holdings", response_class=HTMLResponse)
async def decreased_holdings_page(request: Request, date: str = Query(None), etf_code: str = Query(None)):
    """減持表頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        decreased_holdings = []
        if date:
            decreased_holdings = db_query.get_decreased_holdings(date, etf_code)
        
        return templates.TemplateResponse("decreased_holdings.html", {
            "request": request,
            "decreased_holdings": decreased_holdings,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"減持表頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cross-holdings", response_class=HTMLResponse)
async def cross_holdings_page(request: Request, date: str = Query(None)):
    """跨ETF重複持股頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        dates = db_query.get_available_dates()
        
        cross_holdings = []
        if date:
            cross_holdings = db_query.get_cross_holdings(date)
        
        return templates.TemplateResponse("cross_holdings.html", {
            "request": request,
            "cross_holdings": cross_holdings,
            "dates": dates,
            "selected_date": date
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"跨ETF重複持股頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/holdings", response_class=HTMLResponse)
async def holdings_page(
    request: Request, 
    date: str = Query(None), 
    etf_code: str = Query(None),
    sort_by: str = Query("weight_desc", description="排序方式")  # ⭐ 新增排序參數
):
    """每日持股頁面 - 修正版本帶排序功能"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        logger.info(f"持股頁面請求: date={date}, etf_code={etf_code}, sort_by={sort_by}")
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        holdings = []
        change_stats = {}
        
        if date:
            # ⭐ 關鍵修正：使用正確的方法
            logger.info(f"獲取持股資料: date={date}, etf_code={etf_code}")
            holdings = db_query.get_holdings_with_changes(date, etf_code)  # ⭐ 這裡是關鍵
            
            if holdings:
                logger.info(f"原始資料筆數: {len(holdings)}")
                
                # 應用排序
                holdings = apply_holdings_sorting(holdings, sort_by)  # ⭐ 使用排序函數
                logger.info(f"排序後資料筆數: {len(holdings)}, 排序方式: {sort_by}")
                
                # 計算變化統計
                change_stats = db_query.get_holdings_change_stats(holdings)
                logger.info(f"變化統計: {change_stats}")
            else:
                logger.warning(f"沒有找到日期 {date} 的持股資料")
        
        # ⭐ 重要：將函數添加到模板上下文
        template_context = {
            "request": request,
            "holdings": holdings,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code,
            "sort_by": sort_by,
            "change_stats": change_stats,
            # ⭐ 將函數添加到模板上下文
            "get_sort_icon": get_sort_icon,
            "get_sort_display": get_sort_display
        }
        
        logger.info(f"返回模板，資料筆數: {len(holdings)}")
        return templates.TemplateResponse("holdings.html", template_context)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"每日持股頁面錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/manual-scrape")
async def manual_scrape(request: Request):
    """手動爬取功能"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not scraper:
            raise HTTPException(status_code=503, detail="Scraper unavailable")
        
        # 執行爬蟲
        success_count = scraper.scrape_all_etfs()
        
        return {
            "status": "success",
            "message": f"成功爬取 {success_count} 個ETF的數據",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手動爬取錯誤: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ============ 爬蟲觸發端點 ============
@app.post("/trigger-scrape")
async def trigger_scrape(request: Request):
    """觸發爬蟲（由調度器調用）"""
    try:
        # 檢查調度器令牌
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if token != settings.scheduler_token:
            raise HTTPException(status_code=401, detail="Invalid scheduler token")
        
        if not scraper:
            raise HTTPException(status_code=503, detail="Scraper unavailable")
        
        # 執行爬蟲
        success_count = scraper.scrape_all_etfs()
        
        return {
            "status": "success",
            "message": f"爬蟲執行完成，成功處理 {success_count} 個ETF",
            "timestamp": datetime.now().isoformat(),
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"觸發爬蟲錯誤: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ============ 手動測試端點 ============
@app.post("/test-scrape")
async def test_scrape(request: Request, etf_code: str = Form(...)):
    """測試單個ETF爬蟲（需要認證）"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not scraper:
            raise HTTPException(status_code=503, detail="Scraper unavailable")
        
        if etf_code not in scraper.etf_codes:
            raise HTTPException(status_code=400, detail="Invalid ETF code")
        
        # 執行單個ETF爬蟲
        success = scraper.scrape_single_etf(etf_code)
        
        return {
            "status": "success" if success else "failed",
            "message": f"ETF {etf_code} 爬蟲{'成功' if success else '失敗'}",
            "timestamp": datetime.now().isoformat(),
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"測試爬蟲錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 應用程式關閉處理 ============
@app.on_event("shutdown")
async def shutdown_event():
    """應用程式關閉時的清理工作"""
    try:
        if db_config:
            db_config.close()
            logger.info("應用程式關閉，數據庫連接已清理")
    except Exception as e:
        logger.error(f"關閉應用程式時出錯: {e}")


# 添加到您的 fastapi_app_cloud.py 中

@app.get("/diagnostic")
async def diagnostic_database(request: Request):
    """線上數據庫診斷端點"""
    try:
        # 檢查認證（可選，診斷時可以暫時註釋掉）
        # if not await check_authentication(request):
        #     raise HTTPException(status_code=401, detail="Unauthorized")
        
        diagnostic_info = {
            "timestamp": datetime.now().isoformat(),
            "environment": "production",
            "database_status": {},
            "environment_variables": {},
            "connection_test": {},
            "railway_info": {}
        }
        
        # 1. 檢查環境變數
        database_url = os.getenv("DATABASE_URL")
        diagnostic_info["environment_variables"] = {
            "DATABASE_URL_exists": database_url is not None,
            "DATABASE_URL_length": len(database_url) if database_url else 0,
            "DATABASE_URL_prefix": database_url[:50] if database_url else None,
            "DATABASE_URL_scheme": database_url.split("://")[0] if database_url and "://" in database_url else None
        }
        
        # 2. Railway 環境檢查
        railway_vars = {
            "RAILWAY_ENVIRONMENT": os.getenv("RAILWAY_ENVIRONMENT"),
            "RAILWAY_PROJECT_ID": os.getenv("RAILWAY_PROJECT_ID"),
            "RAILWAY_SERVICE_ID": os.getenv("RAILWAY_SERVICE_ID"),
            "PORT": os.getenv("PORT"),
        }
        diagnostic_info["railway_info"] = railway_vars
        
        # 3. 數據庫配置狀態
        if db_config:
            diagnostic_info["database_status"] = {
                "db_config_available": True,
                "detected_type": db_config.db_type,
                "connection_status": getattr(db_config, 'connection_status', 'unknown'),
                "has_pg_pool": db_config.pg_pool is not None if hasattr(db_config, 'pg_pool') else False
            }
            
            # 如果有 get_status 方法，調用它
            if hasattr(db_config, 'get_status'):
                diagnostic_info["database_status"].update(db_config.get_status())
        else:
            diagnostic_info["database_status"] = {
                "db_config_available": False,
                "error": "db_config 未初始化"
            }
        
        # 4. 連接測試
        try:
            if db_config and db_config.db_type == "postgresql":
                # 測試 PostgreSQL 連接
                with db_config.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT version();")
                    version_info = cursor.fetchone()
                    
                    cursor.execute("SELECT current_database(), current_user;")
                    db_info = cursor.fetchone()
                    
                    diagnostic_info["connection_test"] = {
                        "status": "success",
                        "database_type": "postgresql",
                        "version": str(version_info) if version_info else "unknown",
                        "current_database": str(db_info) if db_info else "unknown"
                    }
            elif db_config and db_config.db_type == "sqlite":
                # 測試 SQLite 連接
                with db_config.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT sqlite_version();")
                    version_info = cursor.fetchone()
                    
                    diagnostic_info["connection_test"] = {
                        "status": "success", 
                        "database_type": "sqlite",
                        "version": str(version_info) if version_info else "unknown",
                        "file_path": db_config.sqlite_path if hasattr(db_config, 'sqlite_path') else "unknown"
                    }
            else:
                diagnostic_info["connection_test"] = {
                    "status": "failed",
                    "error": "無法識別數據庫類型或 db_config 不可用"
                }
                
        except Exception as e:
            diagnostic_info["connection_test"] = {
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__
            }
        
        # 5. 表檢查
        try:
            if db_config:
                # 檢查表是否存在
                if db_config.db_type == "postgresql":
                    query = """
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name IN ('etf_holdings', 'holdings_changes')
                    """
                else:
                    query = """
                        SELECT name FROM sqlite_master 
                        WHERE type='table' 
                        AND name IN ('etf_holdings', 'holdings_changes')
                    """
                
                results = db_config.execute_query(query, fetch="all")
                diagnostic_info["tables"] = {
                    "existing_tables": [row['table_name'] if 'table_name' in row else row['name'] for row in results] if results else [],
                    "expected_tables": ['etf_holdings', 'holdings_changes']
                }
        except Exception as e:
            diagnostic_info["tables"] = {
                "error": str(e)
            }
        
        return diagnostic_info
        
    except Exception as e:
        logger.error(f"診斷端點錯誤: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }




# 簡化版診斷端點（無需認證）
@app.get("/debug/db-status")
async def simple_db_status():
    """簡單的數據庫狀態檢查（無需認證）"""
    return {
        "database_url_exists": os.getenv("DATABASE_URL") is not None,
        "database_url_prefix": os.getenv("DATABASE_URL", "")[:50],
        "db_config_available": db_config is not None,
        "db_type": db_config.db_type if db_config else "unknown",
        "railway_env": os.getenv("RAILWAY_ENVIRONMENT"),
        "timestamp": datetime.now().isoformat()
    }


# database_config_improved.py - 改進版本，增強調試和錯誤處理
import os
import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import logging
from typing import Optional, Dict, Any, Union
from contextlib import contextmanager
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DatabaseConfig:
    """數據庫配置管理 - 改進版本"""
    
    def __init__(self):
        # 詳細記錄初始化過程
        logger.info("開始初始化數據庫配置...")
        
        # 檢查所有可能的環境變數
        self.database_url = self._get_database_url()
        
        logger.info(f"最終使用的 DATABASE_URL: {self.database_url[:50] if self.database_url else 'None'}...")
        
        self.db_type = self._detect_db_type()
        logger.info(f"檢測到數據庫類型: {self.db_type}")
        
        # PostgreSQL 連接池
        self.pg_pool: Optional[SimpleConnectionPool] = None
        
        # SQLite 路徑
        self.sqlite_path = None
        
        # 連接狀態
        self.connection_status = "unknown"
        
        self._initialize_database()
    
    def _get_database_url(self) -> str:
        """獲取數據庫 URL，檢查多個可能的環境變數"""
        
        # 檢查常見的環境變數
        env_vars_to_check = [
            "DATABASE_URL",
            "POSTGRES_URL", 
            "POSTGRESQL_URL",
            "DB_URL"
        ]
        
        logger.info("檢查環境變數...")
        for var_name in env_vars_to_check:
            value = os.getenv(var_name)
            if value:
                logger.info(f"找到環境變數 {var_name}: {value[:50]}...")
                return value
            else:
                logger.debug(f"環境變數 {var_name} 未設置")
        
        # 檢查 Railway 特定環境變數
        railway_vars = [
            "RAILWAY_ENVIRONMENT",
            "RAILWAY_PROJECT_ID", 
            "RAILWAY_SERVICE_ID"
        ]
        
        railway_detected = False
        for var in railway_vars:
            if os.getenv(var):
                railway_detected = True
                logger.info(f"檢測到 Railway 環境變數: {var}={os.getenv(var)}")
        
        if railway_detected:
            logger.warning("檢測到 Railway 環境但未找到 DATABASE_URL，請檢查環境變數設置")
        
        # 如果在 Railway 環境但沒有找到 DATABASE_URL，這是個問題
        if railway_detected and not any(os.getenv(var) for var in env_vars_to_check):
            logger.error("❌ 在 Railway 環境中但未找到數據庫 URL！")
            logger.error("請確保在 Railway 項目中正確設置了 DATABASE_URL 環境變數")
        
        # 默認使用 SQLite
        default_sqlite = "sqlite:///etf_holdings.db"
        logger.warning(f"未找到數據庫 URL 環境變數，使用默認 SQLite: {default_sqlite}")
        return default_sqlite
    
    def _detect_db_type(self) -> str:
        """檢測數據庫類型，增強錯誤處理"""
        if not self.database_url:
            logger.warning("database_url 為空，默認使用 SQLite")
            return "sqlite"
        
        logger.info(f"分析數據庫 URL: {self.database_url[:50]}...")
        
        if self.database_url.startswith(("postgresql://", "postgres://")):
            logger.info("檢測到 PostgreSQL URL")
            return "postgresql"
        elif self.database_url.startswith("sqlite://"):
            logger.info("檢測到 SQLite URL") 
            return "sqlite"
        elif self.database_url.endswith(".db"):
            logger.info("檢測到 SQLite 文件路徑")
            return "sqlite"
        else:
            logger.warning(f"未知數據庫 URL 格式: {self.database_url[:50]}..., 默認使用 SQLite")
            return "sqlite"
    
    def _initialize_database(self):
        """初始化數據庫連接，增強錯誤處理"""
        logger.info(f"初始化數據庫連接，類型: {self.db_type}")
        
        if self.db_type == "postgresql":
            success = self._initialize_postgresql()
            if success:
                self.connection_status = "postgresql_connected"
                logger.info("✅ PostgreSQL 初始化成功")
            else:
                logger.error("❌ PostgreSQL 初始化失敗，降級到 SQLite")
                self.db_type = "sqlite"
                self._initialize_sqlite()
                self.connection_status = "sqlite_fallback"
        else:
            self._initialize_sqlite()
            self.connection_status = "sqlite_only"
        
        logger.info(f"最終數據庫狀態: {self.connection_status}")
    
    def _initialize_postgresql(self) -> bool:
        """初始化 PostgreSQL，詳細錯誤處理"""
        try:
            logger.info("開始 PostgreSQL 初始化...")
            
            # 處理 Railway 的 DATABASE_URL 格式
            database_url = self.database_url
            if database_url.startswith("postgres://"):
                logger.info("轉換 postgres:// 為 postgresql://")
                database_url = database_url.replace("postgres://", "postgresql://", 1)
            
            # 解析 DATABASE_URL
            logger.info("解析數據庫 URL...")
            parsed = urlparse(database_url)
            
            if not parsed.hostname:
                logger.error("❌ PostgreSQL URL 缺少 hostname")
                return False
            
            if not parsed.username:
                logger.error("❌ PostgreSQL URL 缺少 username") 
                return False
            
            logger.info(f"PostgreSQL 連接信息:")
            logger.info(f"  Host: {parsed.hostname}")
            logger.info(f"  Port: {parsed.port or 5432}")
            logger.info(f"  Database: {parsed.path[1:] if parsed.path else 'Unknown'}")
            logger.info(f"  Username: {parsed.username}")
            logger.info(f"  Password: {'設置' if parsed.password else '未設置'}")
            
            # 先進行連接測試
            logger.info("執行連接測試...")
            test_conn = None
            try:
                test_conn = psycopg2.connect(
                    database_url,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=15
                )
                test_conn.autocommit = True
                
                with test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    logger.info(f"✅ PostgreSQL 版本: {version['version'][:80]}...")
                    
                    # 測試基本操作
                    cur.execute("SELECT current_database();")
                    db_name = cur.fetchone()
                    logger.info(f"✅ 當前數據庫: {db_name['current_database']}")
                
                logger.info("✅ PostgreSQL 連接測試成功")
                
            except psycopg2.OperationalError as e:
                logger.error(f"❌ PostgreSQL 連接測試失敗 (OperationalError): {e}")
                return False
            except psycopg2.Error as e:
                logger.error(f"❌ PostgreSQL 錯誤: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ 連接測試異常: {e}")
                return False
            finally:
                if test_conn:
                    test_conn.close()
            
            # 連接測試成功，創建連接池
            logger.info("創建 PostgreSQL 連接池...")
            try:
                self.pg_pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,  # 減少最大連接數
                    host=parsed.hostname,
                    port=parsed.port or 5432,
                    database=parsed.path[1:] if parsed.path else 'railway',
                    user=parsed.username,
                    password=parsed.password,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=30
                )
                
                # 測試從連接池獲取連接
                test_pool_conn = self.pg_pool.getconn()
                self.pg_pool.putconn(test_pool_conn)
                
                logger.info("✅ PostgreSQL 連接池創建成功")
                return True
                
            except Exception as e:
                logger.error(f"❌ PostgreSQL 連接池創建失敗: {e}")
                return False
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL 初始化失敗: {e}")
            logger.error(f"錯誤詳情: {type(e).__name__}: {str(e)}")
            return False
    
    def _initialize_sqlite(self):
        """初始化 SQLite"""
        if self.database_url.startswith("sqlite://"):
            self.sqlite_path = self.database_url[9:]  # 移除 'sqlite://'
        elif self.database_url.startswith("sqlite:///"):
            self.sqlite_path = self.database_url[10:]  # 移除 'sqlite:///'
        else:
            self.sqlite_path = self.database_url
        
        logger.info(f"✅ 使用 SQLite 數據庫: {self.sqlite_path}")
        
        # 測試 SQLite 連接
        try:
            conn = sqlite3.connect(self.sqlite_path)
            conn.close()
            logger.info("✅ SQLite 連接測試成功")
        except Exception as e:
            logger.error(f"❌ SQLite 連接測試失敗: {e}")
    
    @contextmanager
    def get_connection(self):
        """獲取數據庫連接的上下文管理器"""
        if self.db_type == "postgresql":
            if not self.pg_pool:
                raise Exception("PostgreSQL 連接池未初始化")
                
            conn = None
            try:
                conn = self.pg_pool.getconn()
                conn.autocommit = True
                yield conn
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"PostgreSQL 連接錯誤: {e}")
                raise e
            finally:
                if conn:
                    self.pg_pool.putconn(conn)
        else:
            conn = sqlite3.connect(self.sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = (), fetch: str = "none") -> Union[list, dict, None]:
        """執行查詢"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 轉換查詢語法
                converted_query = self._convert_query_syntax(query)
                cursor.execute(converted_query, params)
                
                if fetch == "all":
                    result = cursor.fetchall()
                    return [dict(row) for row in result]
                elif fetch == "one":
                    result = cursor.fetchone()
                    if result:
                        return dict(result)
                    return None
                else:
                    if self.db_type == "sqlite":
                        conn.commit()
                    return None
        except Exception as e:
            logger.error(f"查詢執行錯誤: {e}")
            logger.error(f"查詢: {query}")
            logger.error(f"參數: {params}")
            raise e
    
    def _convert_query_syntax(self, query: str) -> str:
        """轉換查詢語法以適應不同數據庫"""
        if self.db_type == "postgresql":
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            query = query.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        return query
    
    def get_status(self) -> Dict[str, Any]:
        """獲取數據庫狀態信息"""
        return {
            "db_type": self.db_type,
            "connection_status": self.connection_status,
            "database_url_prefix": self.database_url[:50] if self.database_url else None,
            "has_pool": self.pg_pool is not None,
            "sqlite_path": self.sqlite_path if self.db_type == "sqlite" else None
        }
    
    def close(self):
        """關閉數據庫連接"""
        if self.db_type == "postgresql" and self.pg_pool:
            self.pg_pool.closeall()
            logger.info("PostgreSQL 連接池已關閉")

# 全局數據庫實例
db_config = DatabaseConfig()


# ============ 主程式入口 ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)