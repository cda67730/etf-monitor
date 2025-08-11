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
    
    def get_etf_name(self, etf_code: str) -> str:
        return self.etf_names.get(etf_code, etf_code)
    
    def get_available_dates(self):
        if not self.db_available:
            return []
        
        try:
            query = 'SELECT DISTINCT update_date FROM etf_holdings ORDER BY update_date DESC'
            results = db_config.execute_query(query, fetch="all")
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
            results = db_config.execute_query(query, (date,), fetch="all")
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
            
            results = db_config.execute_query(query, params, fetch="all")
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
            
            results = db_config.execute_query(query, tuple(params), fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取持股變化錯誤: {e}")
            return []

    def get_new_holdings(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取新增持股"""
        if not self.db_available:
            return []
        
        try:
            base_query = '''
                SELECT h.etf_code, h.stock_code, h.stock_name, h.weight, h.shares,
                       hc.change_type
                FROM holdings_changes hc
                JOIN etf_holdings h ON hc.etf_code = h.etf_code 
                    AND hc.stock_code = h.stock_code 
                    AND hc.change_date = h.update_date
                WHERE hc.change_type = 'NEW'
            '''
            
            conditions = []
            params = []
            
            if date:
                conditions.append("hc.change_date = %s")
                params.append(date)
            
            if etf_code:
                conditions.append("hc.etf_code = %s")
                params.append(etf_code)
            
            if conditions:
                query = base_query + " AND " + " AND ".join(conditions)
            else:
                query = base_query
            
            query += " ORDER BY hc.change_date DESC, hc.etf_code, h.weight DESC"
            
            results = db_config.execute_query(query, tuple(params), fetch="all")
            
            # 添加 ETF 名稱
            for result in results:
                result['etf_name'] = self.get_etf_name(result['etf_code'])
            
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取新增持股錯誤: {e}")
            return []
    
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
            
            results = db_config.execute_query(query, tuple(params), fetch="all")
            
            # 處理數據格式
            for result in results:
                result['etf_name'] = self.get_etf_name(result['etf_code'])
                result['change_amount'] = result['old_shares'] - result['new_shares']
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
            
            results = db_config.execute_query(query, (date,), fetch="all")
            
            # 為每個重複持股獲取詳細信息
            cross_holdings = []
            for result in results:
                stock_code = result['stock_code']
                
                # 獲取當前持股詳情
                detail_query = '''
                    SELECT etf_code, shares, weight
                    FROM etf_holdings
                    WHERE stock_code = %s AND update_date = %s
                    ORDER BY shares DESC
                '''
                details = db_config.execute_query(detail_query, (stock_code, date), fetch="all")
                
                # 獲取前一日持股（用於計算變化）
                prev_query = '''
                    SELECT etf_code, shares
                    FROM etf_holdings
                    WHERE stock_code = %s AND update_date < %s
                    ORDER BY update_date DESC
                    LIMIT 10
                '''
                prev_holdings = db_config.execute_query(prev_query, (stock_code, date), fetch="all")
                prev_dict = {h['etf_code']: h['shares'] for h in prev_holdings}
                
                # 計算變化
                etf_details = []
                total_increase = 0
                total_decrease = 0
                
                for detail in details:
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
            
            results = db_config.execute_query(query, params, fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取最新持股錯誤: {e}")
            return []

# 初始化數據庫查詢對象
db_query = DatabaseQuery()

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
async def holdings_page(request: Request, date: str = Query(None), etf_code: str = Query(None)):
    """每日持股頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        holdings = []
        if date:
            if etf_code:
                holdings = db_query.get_holdings_by_etf(etf_code, date)
                # 添加 etf_code 到每個記錄
                for holding in holdings:
                    holding['etf_code'] = etf_code
            else:
                holdings = db_query.get_holdings_by_date(date)
        
        return templates.TemplateResponse("holdings.html", {
            "request": request,
            "holdings": holdings,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"每日持股頁面錯誤: {e}")
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

# ============ 主程式入口 ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)