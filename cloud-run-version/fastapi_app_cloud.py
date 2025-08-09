# 完整的修復版本 - 替換整個文件
import os
import sqlite3
import logging
import traceback
import hashlib
import secrets
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import FastAPI, Request, Form, Depends, HTTPException, Cookie
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

# 導入 Cloud Run 版本的爬蟲
from improved_etf_scraper_cloud import ETFHoldingsScraper

# ============ 環境配置 ============
class Settings:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.debug = os.getenv("DEBUG", "true").lower() == "true"  # 開啟調試
        self.allowed_hosts = os.getenv("ALLOWED_HOSTS", "*").split(",")  # Cloud Run 友善
        self.database_url = os.getenv("DATABASE_URL", "sqlite:///etf_holdings.db")
        self.port = int(os.getenv("PORT", 8080))
        self.scheduler_token = os.getenv("SCHEDULER_TOKEN", "default-secret-token")
        
        # 安全設定
        self.web_password = os.getenv("WEB_PASSWORD", "etf2024")
        self.session_secret = os.getenv("SESSION_SECRET", secrets.token_hex(32))
        self.session_timeout = int(os.getenv("SESSION_TIMEOUT", "28800"))  # 8小時
        
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

# ============ FastAPI 應用初始化 ============
app = FastAPI(
    title="ETF持股明細監控系統 (Cloud Run版本)",
    debug=settings.debug,
    version="cloud-run-1.0"
)

templates = Jinja2Templates(directory="templates")

# 安全中間件
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.allowed_hosts if settings.environment == "production" else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cloud Run 友善設置
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# 初始化爬蟲
scraper = ETFHoldingsScraper()

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
        # Cloud Run 頭部檢查
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
    """檢查認證 - 簡化版本"""
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
    if request.url.path in ["/health", "/login", "/favicon.ico", "/trigger-scrape"]:
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
LOGIN_HTML_TEMPLATE = """
<!DOCTYPE html>
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
    </style>
</head>
<body>
    <div class="container">
        <div class="login-container">
            <div class="brand">
                <h3><i class="fas fa-shield-alt"></i> ETF監控系統</h3>
                <p class="text-muted">安全登錄驗證</p>
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
            
            <!-- 調試信息 -->
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
        // 更新調試信息
        function updateDebugInfo() {
            document.getElementById('current-url').textContent = window.location.href;
            document.getElementById('cookies-display').textContent = document.cookie || '無 Cookies';
        }
        
        // 表單提交處理
        document.getElementById('login-form').addEventListener('submit', function(e) {
            document.getElementById('status-display').textContent = '提交中...';
            
            setTimeout(function() {
                updateDebugInfo();
                document.getElementById('status-display').textContent = '檢查 Cookie 設置...';
            }, 1000);
        });
        
        // 頁面載入時更新信息
        updateDebugInfo();
        setInterval(updateDebugInfo, 2000);
    </script>
</body>
</html>
"""

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
        public_paths = ["/health", "/login", "/logout", "/favicon.ico", "/trigger-scrape", "/debug/session"]
        if request.url.path in public_paths:
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
    
    html_content = LOGIN_HTML_TEMPLATE
    
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
            httponly=False,  # 調試時設為 False
            secure=False,    # HTTP 環境設為 False
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
    
    return {
        "session_id": session_id[:8] if session_id else None,
        "session_exists": session_id in session_manager.sessions if session_id else False,
        "client_ip": client_ip,
        "cookies": list(request.cookies.keys()),
        "total_sessions": len(session_manager.sessions),
        "current_time": datetime.now().isoformat(),
        "session_timeout": settings.session_timeout
    }

# ============ Cloud Run 端點 ============
@app.get("/health")
async def health_check():
    """健康檢查"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "sessions": len(session_manager.sessions),
        "version": "cloud-run-1.0"
    }

# ============ 數據庫查詢類別 ============
class DatabaseQuery:
    def __init__(self, db_path='etf_holdings.db'):
        self.db_path = db_path
        self.etf_names = {
            '00981A': '統一台股增長主動式ETF',
            '00982A': '群益台灣精選強棒主動式ETF', 
            '00983A': '中信ARK創新主動式ETF',
            '00984A': '安聯台灣高息成長主動式ETF',
            '00985A': '野村台灣增強50主動式ETF'
        }
        self.ensure_tables_exist()
    
    def ensure_tables_exist(self):
        """確保表存在"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS holdings_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etf_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    etf_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    weight REAL NOT NULL,
                    shares INTEGER NOT NULL,
                    unit TEXT DEFAULT '股',
                    update_date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(etf_code, stock_code, update_date)
                )
            """)
            
            conn.commit()
            logger.info("數據庫表檢查完成")
            
        except Exception as e:
            logger.error(f"數據庫初始化錯誤: {e}")
        finally:
            conn.close()
    
    def get_etf_name(self, etf_code: str) -> str:
        return self.etf_names.get(etf_code, etf_code)
    
    def get_available_dates(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT DISTINCT update_date FROM etf_holdings ORDER BY update_date DESC')
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"獲取日期錯誤: {e}")
            return []
        finally:
            conn.close()
    
    def get_etf_codes(self):
        return ['00981A', '00982A', '00983A', '00984A', '00985A']
    
    def get_etf_codes_with_names(self):
        return [{'code': code, 'name': self.get_etf_name(code)} for code in self.get_etf_codes()]

# 初始化數據庫查詢對象
db_query = DatabaseQuery()

# ============ 主要頁面路由 ============
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """首頁"""
    try:
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        etf_info = db_query.get_etf_codes_with_names()
        
        return templates.TemplateResponse("index.html", {
            "request": request,
            "dates": dates,
            "etf_codes": etf_codes,
            "etf_info": etf_info,
            "current_date": dates[0] if dates else None
        })
    except Exception as e:
        logger.error(f"首頁錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 主程式入口 ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)