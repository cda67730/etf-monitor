# fastapi_app_cloud.py - 添加權證功能，保持所有原有端點不變
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
logger.info("開始初始化數據庫配置...")

database_url = os.getenv("DATABASE_URL")
logger.info(f"DATABASE_URL 環境變數: {database_url[:50] if database_url else 'None'}...")

try:
    from database_config import db_config
    from improved_etf_scraper_cloud import ETFHoldingsScraper
    # 新增：導入權證爬蟲
    from warrant_scraper import WarrantScraper
    
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
    
    # 新增：初始化權證爬蟲
    warrant_scraper = WarrantScraper() if db_config else None
    if warrant_scraper:
        logger.info("權證爬蟲初始化成功")
    
except Exception as e:
    logger.warning(f"爬蟲初始化失敗: {e}")
    scraper = None
    warrant_scraper = None

# ============ 會話管理（保持原有代碼不變）============
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

# ============ 流量限制（保持原有代碼不變）============
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


def apply_holdings_sorting(holdings: List[Dict], sort_by: str) -> List[Dict]:
    """應用持股排序"""
    if not holdings:
        return holdings
    
    if sort_by == 'weight_desc':
        return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=True)
    elif sort_by == 'weight_asc':
        return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=False)
    elif sort_by == 'shares_desc':
        return sorted(holdings, key=lambda x: x.get('shares', 0), reverse=True)
    elif sort_by == 'shares_asc':
        return sorted(holdings, key=lambda x: x.get('shares', 0), reverse=False)
    elif sort_by == 'stock_code_asc':
        return sorted(holdings, key=lambda x: x.get('stock_code', ''), reverse=False)
    elif sort_by == 'stock_name_asc':
        return sorted(holdings, key=lambda x: x.get('stock_name', ''), reverse=False)
    else:
        # 默認按權重降序
        return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=True)

def get_sort_icon(current_sort: str, field: str) -> str:
    """獲取排序圖標"""
    if current_sort == f"{field}_desc":
        return "↓"
    elif current_sort == f"{field}_asc":
        return "↑"
    return ""

def get_sort_display(sort_by: str) -> str:
    """獲取排序顯示名稱"""
    sort_names = {
        'weight_desc': '權重降序',
        'weight_asc': '權重升序',
        'shares_desc': '股數降序',
        'shares_asc': '股數升序',
        'stock_code_asc': '股票代碼升序',
        'stock_name_asc': '股票名稱升序'
    }
    return sort_names.get(sort_by, sort_by)



# ============ 安全檢查函數（保持原有代碼不變）============
def verify_password(input_password: str) -> bool:
    """驗證密碼"""
    return input_password == settings.web_password

async def check_authentication(request: Request) -> bool:
    """檢查認證"""
    session_id = request.cookies.get("session_id")
    client_ip = session_manager.get_client_ip(request)
    
    logger.debug(f"🔍 認證檢查: {request.url.path}, session_id: {session_id[:8] if session_id else 'None'}..., IP: {client_ip}")
    
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

# ============ 完整的數據庫查詢類 ============
class DatabaseQuery:
    """最終完善版本 - 包含所有原有ETF功能和新增權證功能"""
    
    def __init__(self):
        self.etf_names = {
            '00980A': '主動野村臺灣優選ETF',
            '00981A': '統一台股增長主動式ETF',
            '00982A': '群益台灣精選強棒主動式ETF', 
            '00983A': '中信ARK創新主動式ETF',
            '00984A': '安聯台灣高股息成長主動式ETF',
            '00985A': '野村台灣增強50主動式ETF'
        }
        self.db_available = db_config is not None
        if self.db_available:
            self.ensure_tables_exist()
    
    
    
    

    def get_new_holdings(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取新增持股 - 完整版本"""
        if not self.db_available:
            logger.warning("數據庫不可用，返回空列表")
            return []
        
        try:
            logger.info(f"查詢新增持股: date={date}, etf_code={etf_code}")
            
            if not date:
                dates = self.get_available_dates()
                if not dates:
                    logger.warning("沒有可用日期")
                    return []
                date = dates[0]
                logger.info(f"使用最新日期: {date}")
            
            ph = self._get_placeholder()
            conditions = [f"hc.change_type = {ph}", f"hc.change_date = {ph}"]
            params = ['NEW', date]
            
            if etf_code:
                conditions.append(f"hc.etf_code = {ph}")
                params.append(etf_code)
            
            where_clause = " AND ".join(conditions)
            
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
            
            results = self.execute_query(join_query, tuple(params), fetch="all")
            
            if results:
                logger.info(f"JOIN 查詢成功，找到 {len(results)} 筆新增持股")
                for result in results:
                    result['etf_name'] = self.get_etf_name(result['etf_code'])
                return results
            
            logger.info("JOIN 查詢無結果，嘗試分別查詢...")
            
            change_query = f'''
                SELECT etf_code, stock_code, stock_name, new_shares, change_date
                FROM holdings_changes
                WHERE {where_clause}
                ORDER BY etf_code, stock_code
            '''
            
            changes = self.execute_query(change_query, tuple(params), fetch="all")
            
            if not changes:
                logger.warning("沒有找到新增類型的變化記錄")
                return []
            
            new_holdings = []
            for change in changes:
                if not change:
                    continue
                    
                holding_query = f'''
                    SELECT etf_code, stock_code, stock_name, weight, shares, unit
                    FROM etf_holdings
                    WHERE etf_code = {ph} AND stock_code = {ph} AND update_date = {ph}
                '''
                
                holding = self.execute_query(
                    holding_query, 
                    (change['etf_code'], change['stock_code'], date), 
                    fetch="one"
                )
                
                if holding:
                    combined = dict(holding)
                    combined['change_type'] = 'NEW'
                    combined['etf_name'] = self.get_etf_name(combined['etf_code'])
                    new_holdings.append(combined)
                else:
                    fallback = {
                        'etf_code': change['etf_code'],
                        'etf_name': self.get_etf_name(change['etf_code']),
                        'stock_code': change['stock_code'],
                        'stock_name': change['stock_name'],
                        'weight': 0.0,
                        'shares': change['new_shares'],
                        'unit': '股',
                        'change_type': 'NEW'
                    }
                    new_holdings.append(fallback)
            
            logger.info(f"分別查詢成功，找到 {len(new_holdings)} 筆新增持股")
            return new_holdings
            
        except Exception as e:
            logger.error(f"查詢新增持股錯誤: {e}")
            return []

    def get_holdings_with_changes(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取持股明細並包含變化資料 - 修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append(f"h.update_date = {ph}")
                params.append(date)
            else:
                where_conditions.append(f"h.update_date = (SELECT MAX(update_date) FROM etf_holdings)")
            
            if etf_code:
                where_conditions.append(f"h.etf_code = {ph}")
                params.append(etf_code)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
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
            
            holdings = []
            if results:
                for result in results:
                    holding = dict(result)
                    
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
                        holding['change_type'] = None
                        holding['old_shares'] = holding.get('shares', 0)
                        holding['new_shares'] = holding.get('shares', 0)
                        holding['shares_increase'] = 0
                        holding['shares_decrease'] = 0
                    
                    holdings.append(holding)
            
            return holdings
            
        except Exception as e:
            logger.error(f"獲取持股變化資料錯誤: {e}")
            return []

    def get_decreased_holdings(self, date: str = None, etf_code: str = None) -> List[Dict[str, Any]]:
        """獲取減持股票 - 修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            base_query = '''
                SELECT etf_code, stock_code, stock_name, change_type,
                       old_shares, new_shares, old_weight, new_weight, change_date
                FROM holdings_changes
                WHERE change_type IN ('DECREASED', 'REMOVED')
            '''
            
            conditions = []
            params = []
            
            if date:
                conditions.append(f"change_date = {ph}")
                params.append(date)
            
            if etf_code:
                conditions.append(f"etf_code = {ph}")
                params.append(etf_code)
            
            if conditions:
                query = base_query + " AND " + " AND ".join(conditions)
            else:
                query = base_query
            
            query += " ORDER BY change_date DESC, etf_code, (old_shares - new_shares) DESC"
            
            results = self.execute_query(query, tuple(params), fetch="all")
            
            if results:
                for result in results:
                    if result:
                        result['etf_name'] = self.get_etf_name(result['etf_code'])
                        result['change_amount'] = max(0, result['old_shares'] - result['new_shares'])
                        if result['change_type'] == 'REMOVED':
                            result['change_type'] = '完全移除'
                        elif result['change_type'] == 'DECREASED':
                            result['change_type'] = '減持'
            
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取減持股票錯誤: {e}")
            return []
    
    def get_cross_holdings(self, date: str = None) -> List[Dict[str, Any]]:
        """獲取跨ETF重複持股 - 修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            query = f'''
                SELECT 
                    stock_code,
                    stock_name,
                    COUNT(DISTINCT etf_code) as etf_count,
                    SUM(shares) as total_shares
                FROM etf_holdings
                WHERE update_date = {ph}
                GROUP BY stock_code, stock_name
                HAVING COUNT(DISTINCT etf_code) > 1
                ORDER BY total_shares DESC
            '''
            
            if not date:
                dates = self.get_available_dates()
                if not dates:
                    return []
                date = dates[0]
            
            results = self.execute_query(query, (date,), fetch="all")
            
            cross_holdings = []
            if results:
                for result in results:
                    if not result:
                        continue
                        
                    stock_code = result['stock_code']
                    
                    detail_query = f'''
                        SELECT etf_code, shares, weight
                        FROM etf_holdings
                        WHERE stock_code = {ph} AND update_date = {ph}
                        ORDER BY shares DESC
                    '''
                    details = self.execute_query(detail_query, (stock_code, date), fetch="all")
                    
                    prev_query = f'''
                        SELECT etf_code, shares
                        FROM etf_holdings
                        WHERE stock_code = {ph} AND update_date < {ph}
                        ORDER BY update_date DESC
                        LIMIT 10
                    '''
                    prev_holdings = self.execute_query(prev_query, (stock_code, date), fetch="all")
                    prev_dict = {h['etf_code']: h['shares'] for h in prev_holdings} if prev_holdings else {}
                    
                    etf_details = []
                    total_increase = 0
                    total_decrease = 0
                    
                    if details:
                        for detail in details:
                            if not detail:
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
        """獲取最新持股 - 修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            if etf_code:
                query = f'''
                    SELECT stock_code, stock_name, weight, shares, unit, update_date
                    FROM etf_holdings 
                    WHERE etf_code = {ph} AND update_date = (
                        SELECT MAX(update_date) FROM etf_holdings WHERE etf_code = {ph}
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

    def diagnose_new_holdings_data(self, date: str = None) -> Dict[str, Any]:
        """診斷新增持股數據的完整性 - 修正版本"""
        if not self.db_available:
            return {"status": "database_unavailable"}
        
        diagnosis = {
            "status": "checking",
            "timestamp": datetime.now().isoformat(),
            "used_date": None,
            "total_changes": 0,
            "new_changes": 0,
            "total_holdings": 0
        }
        
        try:
            if not date:
                dates = self.get_available_dates()
                if dates:
                    date = dates[0]
                    diagnosis["used_date"] = date
                else:
                    diagnosis["status"] = "error"
                    diagnosis["error"] = "no_available_dates"
                    return diagnosis
            else:
                diagnosis["used_date"] = date
            
            ph = self._get_placeholder()
            
            # 檢查 holdings_changes 表
            changes_query = f"SELECT COUNT(*) as count FROM holdings_changes WHERE change_date = {ph}"
            changes_result = self.execute_query(changes_query, (date,), fetch="one")
            diagnosis["total_changes"] = changes_result["count"] if changes_result else 0
            
            # 檢查 NEW 類型的變化
            new_changes_query = f"SELECT COUNT(*) as count FROM holdings_changes WHERE change_date = {ph} AND change_type = 'NEW'"
            new_changes_result = self.execute_query(new_changes_query, (date,), fetch="one")
            diagnosis["new_changes"] = new_changes_result["count"] if new_changes_result else 0
            
            # 檢查 etf_holdings 表
            holdings_query = f"SELECT COUNT(*) as count FROM etf_holdings WHERE update_date = {ph}"
            holdings_result = self.execute_query(holdings_query, (date,), fetch="one")
            diagnosis["total_holdings"] = holdings_result["count"] if holdings_result else 0
            
            diagnosis["status"] = "completed"
            
        except Exception as e:
            diagnosis["status"] = "error"
            diagnosis["error"] = str(e)
        
        # 修正了返回語句，移除了未定義的 'fetch' 變數
        return diagnosis    
    def ensure_tables_exist(self):
            """確保資料表存在 - 由 database_config 處理"""
            pass  # database_config.__init__ 已處理表格初始化

    def execute_query(self, query: str, params: tuple = (), fetch: str = "all"):
        """執行資料庫查詢"""
        if not self.db_available:
            return [] if fetch == "all" else None
        
        try:
            return db_config.execute_query(query, params, fetch)
        except Exception as e:
            logger.error(f"執行查詢失敗: {e}")
            logger.error(f"查詢: {query[:200]}...")
            logger.error(f"參數: {params}")
            return [] if fetch == "all" else None

    def _get_placeholder(self) -> str:
        """獲取資料庫佔位符"""
        if db_config and db_config.db_type == "postgresql":
            return "%s"
        return "?"

    def get_etf_name(self, etf_code: str) -> str:
        """獲取 ETF 名稱"""
        return self.etf_names.get(etf_code, etf_code)

    def get_available_dates(self) -> List[str]:
        """獲取可用的日期列表"""
        if not self.db_available:
            return []
        
        try:
            query = "SELECT DISTINCT update_date FROM etf_holdings ORDER BY update_date DESC"
            results = self.execute_query(query, (), fetch="all")
            return [result['update_date'] for result in results] if results else []
        except Exception as e:
            logger.error(f"獲取可用日期錯誤: {e}")
            return []

    def get_etf_codes(self) -> List[str]:
        """獲取 ETF 代碼列表"""
        return list(self.etf_names.keys())

    def get_etf_codes_with_names(self) -> Dict[str, str]:
        """獲取 ETF 代碼和名稱字典"""
        return self.etf_names.copy()

    def get_warrant_available_dates(self) -> List[str]:
        """獲取權證資料的可用日期"""
        if not self.db_available:
            return []
        
        try:
            query = "SELECT DISTINCT update_date FROM warrant_data ORDER BY update_date DESC"
            results = self.execute_query(query, (), fetch="all")
            return [result['update_date'] for result in results] if results else []
        except Exception as e:
            logger.error(f"獲取權證可用日期錯誤: {e}")
            return []

    def get_holdings_by_etf(self, etf_code: str, date: str = None) -> List[Dict[str, Any]]:
        """獲取特定 ETF 的持股明細"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            if not date:
                dates = self.get_available_dates()
                date = dates[0] if dates else None
            
            if not date:
                return []
            
            query = f'''
                SELECT etf_code, stock_code, stock_name, weight, shares, unit, update_date
                FROM etf_holdings 
                WHERE etf_code = {ph} AND update_date = {ph}
                ORDER BY weight DESC
            '''
            
            results = self.execute_query(query, (etf_code, date), fetch="all")
            return results if results else []
        except Exception as e:
            logger.error(f"獲取 ETF 持股錯誤: {e}")
            return []

    def get_holdings_changes(self, etf_code: str = None, date: str = None) -> List[Dict[str, Any]]:
        """獲取持股變化資料"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append(f"change_date = {ph}")
                params.append(date)
            
            if etf_code:
                where_conditions.append(f"etf_code = {ph}")
                params.append(etf_code)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f'''
                SELECT etf_code, stock_code, stock_name, change_type,
                       old_shares, new_shares, old_weight, new_weight, change_date
                FROM holdings_changes 
                {where_clause}
                ORDER BY change_date DESC, etf_code, stock_code
            '''
            
            results = self.execute_query(query, tuple(params), fetch="all")
            
            # 添加 ETF 名稱
            if results:
                for result in results:
                    if result:
                        result['etf_name'] = self.get_etf_name(result['etf_code'])
            
            return results if results else []
        except Exception as e:
            logger.error(f"獲取持股變化錯誤: {e}")
            return []


    # ========== 權證相關查詢方法 ==========
    
    
    
    def get_warrant_ranking(self, date: str = None, warrant_type: str = None, sort_by: str = 'ranking', limit: int = None):
        """獲取權證排行資料 - 修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append(f"update_date = {ph}")
                params.append(date)
            else:
                where_conditions.append(f"update_date = (SELECT MAX(update_date) FROM warrant_data)")
            
            if warrant_type and warrant_type in ['認購', '認售']:
                where_conditions.append(f"warrant_type = {ph}")
                params.append(warrant_type)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            sort_options = {
                'ranking': 'ranking ASC',
                'change_percent_desc': 'change_percent DESC',
                'change_percent_asc': 'change_percent ASC', 
                'volume_desc': 'volume DESC',
                'volume_asc': 'volume ASC',
                'implied_volatility_desc': 'implied_volatility DESC',
                'implied_volatility_asc': 'implied_volatility ASC'
            }
            
            order_clause = f"ORDER BY {sort_options.get(sort_by, 'ranking ASC')}"
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f'''
                SELECT * FROM warrant_data 
                {where_clause}
                {order_clause}
                {limit_clause}
            '''
            
            results = self.execute_query(query, tuple(params), fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取權證排行資料錯誤: {e}")
            return []
    
    def get_underlying_summary(self, date: str = None, sort_by: str = 'warrant_count'):
        """獲取標的統計資料 - 認購認售分開統計，修正版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            
            where_condition = f"update_date = {ph}" if date else f"update_date = (SELECT MAX(update_date) FROM warrant_underlying_summary)"
            params = (date,) if date else ()
            
            sort_options = {
                'warrant_count': 'warrant_count DESC, total_volume DESC',
                'total_volume': 'total_volume DESC, warrant_count DESC',
                'avg_implied_volatility': 'avg_implied_volatility DESC',
                'underlying_name': 'underlying_name ASC, warrant_type ASC'
            }
            
            order_clause = f"ORDER BY {sort_options.get(sort_by, 'warrant_count DESC, total_volume DESC')}"
            
            query = f'''
                SELECT 
                    underlying_name,
                    warrant_type,
                    warrant_count,
                    total_volume,
                    avg_implied_volatility,
                    total_change_amount,
                    update_date
                FROM warrant_underlying_summary 
                WHERE {where_condition}
                {order_clause}
            '''
            
            results = self.execute_query(query, params, fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取標的統計資料錯誤: {e}")
            return []
    
    def get_warrant_statistics(self, date: str = None):
        """獲取權證統計資訊 - 修正版本"""
        if not self.db_available:
            return {}
        
        try:
            ph = self._get_placeholder()
            
            where_condition = f"update_date = {ph}" if date else f"update_date = (SELECT MAX(update_date) FROM warrant_data)"
            params = (date,) if date else ()
            
            basic_stats_query = f'''
                SELECT 
                    COUNT(*) as total_warrants,
                    COUNT(CASE WHEN warrant_type = '認購' THEN 1 END) as call_warrants,
                    COUNT(CASE WHEN warrant_type = '認售' THEN 1 END) as put_warrants,
                    SUM(volume) as total_volume,
                    AVG(implied_volatility) as avg_implied_volatility,
                    AVG(change_percent) as avg_change_percent
                FROM warrant_data 
                WHERE {where_condition}
            '''
            
            basic_stats = self.execute_query(basic_stats_query, params, fetch="one")
            
            underlying_stats_query = f'''
                SELECT 
                    COUNT(DISTINCT underlying_name) as unique_underlyings
                FROM warrant_data 
                WHERE {where_condition} AND underlying_name IS NOT NULL AND underlying_name != ''
            '''
            
            underlying_stats = self.execute_query(underlying_stats_query, params, fetch="one")
            
            stats = {
                'total_warrants': basic_stats.get('total_warrants', 0) if basic_stats else 0,
                'call_warrants': basic_stats.get('call_warrants', 0) if basic_stats else 0,
                'put_warrants': basic_stats.get('put_warrants', 0) if basic_stats else 0,
                'total_volume': basic_stats.get('total_volume', 0) if basic_stats else 0,
                'avg_implied_volatility': round(basic_stats.get('avg_implied_volatility', 0), 2) if basic_stats else 0,
                'avg_change_percent': round(basic_stats.get('avg_change_percent', 0), 2) if basic_stats else 0,
                'unique_underlyings': underlying_stats.get('unique_underlyings', 0) if underlying_stats else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"獲取權證統計資訊錯誤: {e}")
            return {}
    
    def get_warrant_by_underlying(self, underlying_name: str, date: str = None, warrant_type: str = None):
        """根據標的名稱獲取相關權證 - 新增方法"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            
            where_conditions = [f"underlying_name = {ph}"]
            params = [underlying_name]
            
            if date:
                where_conditions.append(f"update_date = {ph}")
                params.append(date)
            else:
                where_conditions.append(f"update_date = (SELECT MAX(update_date) FROM warrant_data)")
            
            if warrant_type and warrant_type in ['認購', '認售']:
                where_conditions.append(f"warrant_type = {ph}")
                params.append(warrant_type)
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f'''
                SELECT * FROM warrant_data 
                {where_clause}
                ORDER BY ranking ASC
            '''
            
            results = self.execute_query(query, tuple(params), fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"根據標的獲取權證錯誤: {e}")
            return []
    
    def search_warrants(self, keyword: str, date: str = None, search_type: str = 'all'):
        """搜索權證 - 修復後版本"""
        if not self.db_available:
            return []
        
        try:
            ph = self._get_placeholder()
            like_op = "ILIKE" if db_config.db_type == "postgresql" else "LIKE"
            
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append(f"update_date = {ph}")
                params.append(date)
            else:
                where_conditions.append(f"update_date = (SELECT MAX(update_date) FROM warrant_data)")
            
            # 搜索條件
            search_conditions = []
            if search_type in ['all', 'warrant']:
                search_conditions.extend([
                    f"warrant_code {like_op} {ph}",
                    f"warrant_name {like_op} {ph}"
                ])
                params.extend([f"%{keyword}%", f"%{keyword}%"])
            
            if search_type in ['all', 'underlying']:
                search_conditions.append(f"underlying_name {like_op} {ph}")
                params.append(f"%{keyword}%")
            
            if search_conditions:
                where_conditions.append(f"({' OR '.join(search_conditions)})")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f'''
                SELECT * FROM warrant_data 
                {where_clause}
                ORDER BY ranking ASC
            '''
            
            results = self.execute_query(query, tuple(params), fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"搜索權證錯誤: {e}")
            return []
    # ========== 保留所有原有的ETF方法 ==========
    
        
    def apply_holdings_sorting(holdings: List[Dict], sort_by: str) -> List[Dict]:
        """應用持股排序"""
        if not holdings:
            return holdings
        
        if sort_by == 'weight_desc':
            return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=True)
        elif sort_by == 'weight_asc':
            return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=False)
        elif sort_by == 'shares_desc':
            return sorted(holdings, key=lambda x: x.get('shares', 0), reverse=True)
        elif sort_by == 'shares_asc':
            return sorted(holdings, key=lambda x: x.get('shares', 0), reverse=False)
        elif sort_by == 'stock_code_asc':
            return sorted(holdings, key=lambda x: x.get('stock_code', ''), reverse=False)
        elif sort_by == 'stock_name_asc':
            return sorted(holdings, key=lambda x: x.get('stock_name', ''), reverse=False)
        else:
            # 默認按權重降序
            return sorted(holdings, key=lambda x: x.get('weight', 0), reverse=True)

    def get_sort_icon(current_sort: str, field: str) -> str:
        """獲取排序圖標"""
        if current_sort == f"{field}_desc":
            return "↓"
        elif current_sort == f"{field}_asc":
            return "↑"
        return ""

    def get_sort_display(sort_by: str) -> str:
        """獲取排序顯示名稱"""
        sort_names = {
            'weight_desc': '權重降序',
            'weight_asc': '權重升序',
            'shares_desc': '股數降序',
            'shares_asc': '股數升序',
            'stock_code_asc': '股票代碼升序',
            'stock_name_asc': '股票名稱升序'
        }
        return sort_names.get(sort_by, sort_by)    


    

# 初始化數據庫查詢對象
db_query = DatabaseQuery()

# ============ 保持所有原有的路由和中間件不變 ============

# [所有原有的路由方法保持完全不變]

# ============ 新增權證相關的 API 路由 ============

@app.get("/api/warrants")
async def api_get_warrants(
    request: Request,
    date: str = Query(None, description="日期 (YYYY-MM-DD)"),
    warrant_type: str = Query(None, description="權證類型 (認購/認售)"),
    sort_by: str = Query("ranking", description="排序方式"),
    limit: int = Query(None, description="限制筆數")
):
    """API: 獲取權證排行資料"""
    try:
        # 檢查認證
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not db_query.db_available:
            raise HTTPException(status_code=503, detail="Database unavailable")
        
        data = db_query.get_warrant_ranking(date, warrant_type, sort_by, limit)
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "date": date,
            "warrant_type": warrant_type,
            "sort_by": sort_by,
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API權證查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/warrant-summary")
async def api_get_warrant_summary(
    request: Request,
    date: str = Query(None, description="日期 (YYYY-MM-DD)"),
    sort_by: str = Query("warrant_count", description="排序方式")
):
    """API: 獲取權證標的統計資料"""
    try:
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not db_query.db_available:
            raise HTTPException(status_code=503, detail="Database unavailable")
        
        data = db_query.get_underlying_summary(date, sort_by)
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "date": date,
            "sort_by": sort_by,
            "database_type": db_config.db_type if db_config else "unavailable"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API權證統計查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 權證排行頁面路由 ============
@app.get("/warrant-ranking", response_class=HTMLResponse)
async def warrant_ranking_page(
    request: Request, 
    date: str = Query(None), 
    warrant_type: str = Query(None),
    sort_by: str = Query("ranking"),
    summary_sort: str = Query("warrant_count")
):
    """權證排行頁面"""
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        # 獲取可用日期
        warrant_dates = db_query.get_warrant_available_dates()
        
        # 如果沒有指定日期，使用最新日期
        if not date and warrant_dates:
            date = warrant_dates[0]
        
        # 獲取權證統計資訊
        stats = db_query.get_warrant_statistics(date)
        
        # 獲取標的統計資料（上半部）
        underlying_summary = db_query.get_underlying_summary(date, summary_sort)
        
        # 獲取權證詳細資料（下半部）
        warrant_ranking = db_query.get_warrant_ranking(date, warrant_type, sort_by)
        
        return templates.TemplateResponse("warrant_ranking.html", {
            "request": request,
            "warrant_dates": warrant_dates,
            "selected_date": date,
            "selected_warrant_type": warrant_type,
            "sort_by": sort_by,
            "summary_sort": summary_sort,
            "stats": stats,
            "underlying_summary": underlying_summary,
            "warrant_ranking": warrant_ranking,
            "database_type": db_config.db_type if db_config else "unavailable"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"權證排行頁面錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 爬蟲相關路由 ============
@app.post("/manual-scrape")
async def manual_scrape(request: Request):
    """手動爬取功能"""
    try:
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

@app.post("/manual-scrape-warrants")
async def manual_scrape_warrants(request: Request):
    """手動權證爬取功能"""
    try:
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not warrant_scraper:
            raise HTTPException(status_code=503, detail="Warrant scraper unavailable")
        
        # 執行權證爬蟲
        success = warrant_scraper.scrape_warrants(pages=5, sort_type=3)
        
        if success:
            return {
                "status": "success",
                "message": "權證爬取成功",
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "error", 
                "message": "權證爬取失敗",
                "timestamp": datetime.now().isoformat()
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手動權證爬取錯誤: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

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

@app.post("/trigger-scrape-warrants")
async def trigger_scrape_warrants(request: Request):
    """觸發權證爬蟲（由調度器調用）"""
    try:
        # 檢查調度器令牌
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if token != settings.scheduler_token:
            raise HTTPException(status_code=401, detail="Invalid scheduler token")
        
        if not warrant_scraper:
            raise HTTPException(status_code=503, detail="Warrant scraper unavailable")
        
        # 執行權證爬蟲
        success = warrant_scraper.scrape_warrants(pages=5, sort_type=3)
        
        if success:
            return {
                "status": "success",
                "message": "權證爬蟲執行完成",
                "timestamp": datetime.now().isoformat(),
                "database_type": db_config.db_type if db_config else "unavailable"
            }
        else:
            return {
                "status": "error",
                "message": "權證爬蟲執行失敗",
                "timestamp": datetime.now().isoformat()
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"觸發權證爬蟲錯誤: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.post("/test-scrape")
async def test_scrape(request: Request, etf_code: str = Form(...)):
    """測試單個ETF爬蟲（需要認證）"""
    try:
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

# ============ 診斷路由 ============
@app.get("/diagnostic")
async def diagnostic_database(request: Request):
    """線上數據庫診斷端點"""
    try:
        diagnostic_info = {
            "timestamp": datetime.now().isoformat(),
            "environment": "production",
            "database_status": {},
            "environment_variables": {},
            "connection_test": {},
            "railway_info": {}
        }
        
        # 檢查環境變數
        database_url = os.getenv("DATABASE_URL")
        diagnostic_info["environment_variables"] = {
            "DATABASE_URL_exists": database_url is not None,
            "DATABASE_URL_length": len(database_url) if database_url else 0,
            "DATABASE_URL_prefix": database_url[:50] if database_url else None,
            "DATABASE_URL_scheme": database_url.split("://")[0] if database_url and "://" in database_url else None
        }
        
        # Railway 環境檢查
        railway_vars = {
            "RAILWAY_ENVIRONMENT": os.getenv("RAILWAY_ENVIRONMENT"),
            "RAILWAY_PROJECT_ID": os.getenv("RAILWAY_PROJECT_ID"),
            "RAILWAY_SERVICE_ID": os.getenv("RAILWAY_SERVICE_ID"),
            "PORT": os.getenv("PORT"),
        }
        diagnostic_info["railway_info"] = railway_vars
        
        # 數據庫配置狀態
        if db_config:
            diagnostic_info["database_status"] = {
                "db_config_available": True,
                "detected_type": db_config.db_type,
                "connection_status": getattr(db_config, 'connection_status', 'unknown'),
                "has_pg_pool": db_config.pg_pool is not None if hasattr(db_config, 'pg_pool') else False
            }
            
            if hasattr(db_config, 'get_status'):
                diagnostic_info["database_status"].update(db_config.get_status())
        else:
            diagnostic_info["database_status"] = {
                "db_config_available": False,
                "error": "db_config 未初始化"
            }
        
        # 連接測試
        try:
            if db_config and db_config.db_type == "postgresql":
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
        
        # 表檢查
        try:
            if db_config:
                if db_config.db_type == "postgresql":
                    query = """
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name IN ('etf_holdings', 'holdings_changes', 'warrant_data', 'warrant_underlying_summary')
                    """
                else:
                    query = """
                        SELECT name FROM sqlite_master 
                        WHERE type='table' 
                        AND name IN ('etf_holdings', 'holdings_changes', 'warrant_data', 'warrant_underlying_summary')
                    """
                
                results = db_config.execute_query(query, fetch="all")
                diagnostic_info["tables"] = {
                    "existing_tables": [row['table_name'] if 'table_name' in row else row['name'] for row in results] if results else [],
                    "expected_tables": ['etf_holdings', 'holdings_changes', 'warrant_data', 'warrant_underlying_summary']
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

# ============ 應用程序關閉處理 ============
@app.on_event("shutdown")
async def shutdown_event():
    """應用程序關閉時的清理工作"""
    try:
        if db_config:
            db_config.close()
            logger.info("應用程序關閉，數據庫連接已清理")
    except Exception as e:
        logger.error(f"關閉應用程序時出錯: {e}")

# ============ 主要頁面路由 ============
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """首頁"""
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
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
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
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
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
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
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
    
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
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
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
    if not await check_authentication(request):
        return RedirectResponse(url="/login", status_code=302)
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
    sort_by: str = Query("weight_desc", description="排序方式")
):
    """每日持股頁面"""
    try:
        if not templates:
            raise HTTPException(status_code=503, detail="Templates unavailable")
        
        logger.info(f"持股頁面請求: date={date}, etf_code={etf_code}, sort_by={sort_by}")
        
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        holdings = []
        change_stats = {}
        
        if date:
            logger.info(f"獲取持股資料: date={date}, etf_code={etf_code}")
            holdings = db_query.get_holdings_with_changes(date, etf_code)
            
            if holdings:
                logger.info(f"原始資料筆數: {len(holdings)}")
                
                # 應用排序
                holdings = apply_holdings_sorting(holdings, sort_by)
                logger.info(f"排序後資料筆數: {len(holdings)}, 排序方式: {sort_by}")
                
                # 計算變化統計
                change_stats = db_query.get_holdings_change_stats(holdings)
                logger.info(f"變化統計: {change_stats}")
            else:
                logger.warning(f"沒有找到日期 {date} 的持股資料")
        
        template_context = {
            "request": request,
            "holdings": holdings,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code,
            "sort_by": sort_by,
            "change_stats": change_stats,
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




# ============ 登入/登出路由 ============
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = Query(None)):
    """顯示登入頁面"""
    if not templates:
        raise HTTPException(status_code=503, detail="Templates unavailable")
    return templates.TemplateResponse("login.html", {"request": request, "error": error})

@app.post("/login")
async def login_process(request: Request, password: str = Form(...)):
    """處理登入請求"""
    if not verify_password(password):
        logger.warning(f"❌ 密碼錯誤，登入失敗，IP: {session_manager.get_client_ip(request)}")
        return RedirectResponse(url="/login?error=Invalid password", status_code=302)
    
    session_id = session_manager.create_session(request)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key="session_id",
        value=session_id,
        max_age=settings.session_timeout,
        httponly=True,
        secure=settings.environment == "production",
        samesite="lax"
    )
    return response

@app.get("/logout")
async def logout(request: Request):
    """處理登出請求"""
    session_id = request.cookies.get("session_id")
    if session_id and session_id in session_manager.sessions:
        del session_manager.sessions[session_id]
        logger.info(f"🧹 用戶登出，會話已刪除: {session_id[:8]}...")
    
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_id")
    return response

# ============ 主程式入口 ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)
