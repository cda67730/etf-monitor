from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import sqlite3
from datetime import datetime, timedelta
import schedule
import threading
import time
from typing import Optional, List
import logging
import traceback
from improved_etf_scraper import ETFHoldingsScraper

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ETF持股明細監控系統")
templates = Jinja2Templates(directory="templates")

# 初始化爬蟲
scraper = ETFHoldingsScraper()

class DatabaseQuery:
    def __init__(self, db_path='etf_holdings.db'):
        self.db_path = db_path
        # ETF代碼對應名稱的映射
        self.etf_names = {
            '00981A': '統一台股增長主動式ETF',
            '00982A': '群益台灣精選強棒主動式ETF', 
            '00983A': '中信ARK創新主動式ETF',
            '00984A': '安聯台灣高息成長主動式ETF',
            '00985A': '野村台灣增強50主動式ETF'
        }
        # 檢查並初始化數據庫表結構
        self.ensure_tables_exist()
    
    def ensure_tables_exist(self):
        """確保所有必要的表都存在"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 檢查 holdings_changes 表是否存在
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
            
            # 檢查 etf_holdings 表是否存在
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
            logger.info("數據庫表結構檢查完成")
            
        except Exception as e:
            logger.error(f"檢查數據庫表結構時出錯: {e}")
        finally:
            conn.close()
    
    def get_etf_name(self, etf_code: str) -> str:
        """根據ETF代碼獲取ETF名稱"""
        return self.etf_names.get(etf_code, etf_code)
    
    def get_available_dates(self):
        """獲取所有可用的日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT DISTINCT update_date FROM etf_holdings ORDER BY update_date DESC')
            dates = [row[0] for row in cursor.fetchall()]
            return dates
        except Exception as e:
            logger.error(f"獲取可用日期時出錯: {e}")
            return []
        finally:
            conn.close()
    
    def get_etf_codes(self):
        """獲取所有ETF代碼"""
        return ['00981A', '00982A', '00983A', '00984A', '00985A']
    
    def get_etf_codes_with_names(self):
        """獲取所有ETF代碼和名稱"""
        return [{'code': code, 'name': self.get_etf_name(code)} for code in self.get_etf_codes()]
    
    def get_holdings_by_date_and_etf(self, date: str, etf_code: Optional[str] = None):
        """根據日期和ETF代碼查詢持股"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if etf_code:
                cursor.execute('''
                    SELECT etf_code, stock_code, stock_name, weight, shares, unit
                    FROM etf_holdings 
                    WHERE update_date = ? AND etf_code = ?
                    ORDER BY weight DESC
                ''', (date, etf_code))
            else:
                cursor.execute('''
                    SELECT etf_code, stock_code, stock_name, weight, shares, unit
                    FROM etf_holdings 
                    WHERE update_date = ?
                    ORDER BY etf_code, weight DESC
                ''', (date,))
            
            results = cursor.fetchall()
            
            holdings = []
            for row in results:
                holdings.append({
                    'etf_code': row[0],
                    'etf_name': self.get_etf_name(row[0]),
                    'stock_code': row[1],
                    'stock_name': row[2],
                    'weight': row[3],
                    'shares': row[4],
                    'unit': row[5]
                })
            
            return holdings
            
        except Exception as e:
            logger.error(f"查詢持股時出錯: {e}")
            return []
        finally:
            conn.close()
    
    def get_cross_etf_holdings(self, date: str):
        """查詢跨ETF重複持股 - 簡化穩定版本"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            logger.info(f"查詢跨ETF重複持股，日期: {date}")
            
            # 1. 直接查詢重複持股（被2個以上ETF持有的股票）
            cursor.execute('''
                SELECT 
                    stock_code, 
                    stock_name,
                    COUNT(DISTINCT etf_code) as etf_count,
                    SUM(shares) as total_shares
                FROM etf_holdings 
                WHERE update_date = ?
                GROUP BY stock_code, stock_name
                HAVING COUNT(DISTINCT etf_code) >= 2
                ORDER BY total_shares DESC
            ''', (date,))
            
            cross_stocks = cursor.fetchall()
            logger.info(f"找到 {len(cross_stocks)} 檔重複持股")
            
            if not cross_stocks:
                return []
            
            # 2. 獲取這些股票的詳細持股信息
            cross_holdings = []
            
            for stock_code, stock_name, etf_count, total_shares in cross_stocks:
                # 查詢當日該股票在各ETF的詳細信息
                cursor.execute('''
                    SELECT etf_code, shares, weight
                    FROM etf_holdings 
                    WHERE update_date = ? AND stock_code = ?
                    ORDER BY shares DESC
                ''', (date, stock_code))
                
                current_details = cursor.fetchall()
                
                # 查詢前一日該股票的持股信息（用於計算變化）
                cursor.execute('''
                    SELECT etf_code, shares
                    FROM etf_holdings 
                    WHERE stock_code = ? 
                    AND update_date = (
                        SELECT MAX(update_date) 
                        FROM etf_holdings 
                        WHERE update_date < ? AND stock_code = ?
                    )
                ''', (stock_code, date, stock_code))
                
                previous_details = cursor.fetchall()
                
                # 構建前一日數據字典
                previous_dict = {}
                for etf_code, shares in previous_details:
                    previous_dict[etf_code] = shares if shares is not None else 0
                
                # 構建ETF詳細信息
                etf_details = []
                total_increase = 0
                total_decrease = 0
                
                for etf_code, shares, weight in current_details:
                    # 確保數據類型正確
                    current_shares = shares if shares is not None else 0
                    current_weight = weight if weight is not None else 0.0
                    previous_shares = previous_dict.get(etf_code, 0)
                    
                    # 計算變化
                    change = current_shares - previous_shares
                    
                    # 累計總變化
                    if change > 0:
                        total_increase += change
                    elif change < 0:
                        total_decrease += abs(change)
                    
                    etf_details.append({
                        'etf_code': etf_code,
                        'etf_name': self.get_etf_name(etf_code),
                        'shares': current_shares,
                        'weight': current_weight,
                        'previous_shares': previous_shares,
                        'change': change,
                        'change_type': 'increase' if change > 0 else ('decrease' if change < 0 else 'no_change')
                    })
                
                # 只保留有變化的ETF信息
                changed_etfs = [detail for detail in etf_details if detail['change'] != 0]
                
                # 添加到結果中
                cross_holdings.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'total_shares': int(total_shares) if total_shares is not None else 0,
                    'etf_count': int(etf_count),
                    'total_increase': int(total_increase),
                    'total_decrease': int(total_decrease),
                    'etf_details': etf_details,
                    'changed_etfs': changed_etfs
                })
            
            logger.info(f"成功處理 {len(cross_holdings)} 檔跨ETF重複持股")
            return cross_holdings
            
        except Exception as e:
            logger.error(f"查詢跨ETF重複持股時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
        finally:
            conn.close()
    
    def get_new_holdings(self, date: str, etf_code: Optional[str] = None):
        """查詢新增持股"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            logger.info(f"查詢新增持股，日期: {date}, ETF: {etf_code}")
            
            # 首先檢查 holdings_changes 表是否有數據
            cursor.execute('SELECT COUNT(*) FROM holdings_changes WHERE change_date = ?', (date,))
            count = cursor.fetchone()[0]
            logger.info(f"holdings_changes 表中找到記錄: {count} 筆")
            
            if count == 0:
                logger.warning(f"holdings_changes 表中沒有 {date} 的數據")
                return []
            
            if etf_code:
                cursor.execute('''
                    SELECT etf_code, stock_code, stock_name, new_shares, new_weight
                    FROM holdings_changes 
                    WHERE change_date = ? AND etf_code = ? AND change_type = 'NEW'
                    ORDER BY new_weight DESC
                ''', (date, etf_code))
            else:
                cursor.execute('''
                    SELECT etf_code, stock_code, stock_name, new_shares, new_weight
                    FROM holdings_changes 
                    WHERE change_date = ? AND change_type = 'NEW'
                    ORDER BY etf_code, new_weight DESC
                ''', (date,))
            
            results = cursor.fetchall()
            
            holdings = []
            for row in results:
                holdings.append({
                    'etf_code': row[0],
                    'etf_name': self.get_etf_name(row[0]),
                    'stock_code': row[1],
                    'stock_name': row[2],
                    'shares': row[3] if row[3] is not None else 0,
                    'weight': row[4] if row[4] is not None else 0.0
                })
            
            logger.info(f"找到新增持股: {len(holdings)} 筆")
            return holdings
            
        except Exception as e:
            logger.error(f"查詢新增持股時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
        finally:
            conn.close()
    
    def get_decreased_holdings(self, date: str, etf_code: Optional[str] = None):
        """查詢減持股票 - 簡化穩定版本"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            logger.info(f"查詢減持，日期: {date}, ETF: {etf_code}")
            
            # 檢查是否有該日期的數據
            cursor.execute('SELECT COUNT(*) FROM holdings_changes WHERE change_date = ?', (date,))
            count = cursor.fetchone()[0]
            logger.info(f"holdings_changes 表中找到 {count} 筆記錄")
            
            if count == 0:
                logger.warning(f"holdings_changes 表中沒有 {date} 的數據")
                return []
            
            # 構建查詢SQL
            if etf_code:
                sql = '''
                    SELECT etf_code, stock_code, stock_name, change_type, 
                        old_shares, new_shares, old_weight, new_weight
                    FROM holdings_changes 
                    WHERE change_date = ? AND etf_code = ? 
                    AND change_type IN ('DECREASED', 'REMOVED')
                    ORDER BY (CASE WHEN old_shares IS NULL THEN 0 ELSE old_shares END - 
                            CASE WHEN new_shares IS NULL THEN 0 ELSE new_shares END) DESC
                '''
                params = (date, etf_code)
            else:
                sql = '''
                    SELECT etf_code, stock_code, stock_name, change_type,
                        old_shares, new_shares, old_weight, new_weight
                    FROM holdings_changes 
                    WHERE change_date = ? AND change_type IN ('DECREASED', 'REMOVED')
                    ORDER BY etf_code, (CASE WHEN old_shares IS NULL THEN 0 ELSE old_shares END - 
                                    CASE WHEN new_shares IS NULL THEN 0 ELSE new_shares END) DESC
                '''
                params = (date,)
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            holdings = []
            for row in results:
                # 安全處理可能的NULL值
                old_shares = row[4] if row[4] is not None else 0
                new_shares = row[5] if row[5] is not None else 0
                old_weight = row[6] if row[6] is not None else 0.0
                new_weight = row[7] if row[7] is not None else 0.0
                
                # 計算變化數量
                change_amount = old_shares - new_shares
                
                holdings.append({
                    'etf_code': str(row[0]),
                    'etf_name': self.get_etf_name(str(row[0])),
                    'stock_code': str(row[1]),
                    'stock_name': str(row[2]),
                    'change_type': '完全移除' if row[3] == 'REMOVED' else '減持',
                    'old_shares': int(old_shares),
                    'new_shares': int(new_shares),
                    'old_weight': float(old_weight),
                    'new_weight': float(new_weight),
                    'change_amount': int(change_amount)
                })
            
            logger.info(f"找到減持記錄: {len(holdings)} 筆")
            return holdings
            
        except Exception as e:
            logger.error(f"查詢減持時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
        finally:
            conn.close()
# 初始化數據庫查詢對象
db_query = DatabaseQuery()

def run_scheduler():
    """在背景執行定時任務"""
    def should_run():
        today = datetime.now().weekday()  # 0=Monday, 6=Sunday
        return today < 5  # 週一到週五
    
    def scheduled_task():
        if should_run():
            logger.info("執行定時爬取任務")
            try:
                scraper.scrape_all_etfs()
                logger.info("定時爬取任務完成")
            except Exception as e:
                logger.error(f"定時爬取任務失敗: {e}")
        else:
            logger.info("週末，跳過爬取任務")
    
    # 設置定時任務
    schedule.every().day.at("21:30").do(scheduled_task)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

# 啟動定時任務
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

# ============ 網頁路由 ============

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
        logger.error(f"首頁加載錯誤: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/holdings", response_class=HTMLResponse)
async def holdings_page(request: Request, date: str = None, etf_code: str = None):
    """每日持股頁面"""
    try:
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        if not date and dates:
            date = dates[0]
        
        holdings = []
        if date:
            holdings = db_query.get_holdings_by_date_and_etf(date, etf_code)
        
        return templates.TemplateResponse("holdings.html", {
            "request": request,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code,
            "holdings": holdings
        })
    except Exception as e:
        logger.error(f"持股頁面加載錯誤: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/new-holdings", response_class=HTMLResponse)
async def new_holdings_page(request: Request, date: str = None, etf_code: str = None):
    """每日新增持股頁面"""
    try:
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        if not date and dates:
            date = dates[0]
        
        new_holdings = []
        if date:
            new_holdings = db_query.get_new_holdings(date, etf_code)
        
        return templates.TemplateResponse("new_holdings.html", {
            "request": request,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code,
            "new_holdings": new_holdings
        })
    except Exception as e:
        logger.error(f"新增持股頁面加載錯誤: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cross-holdings", response_class=HTMLResponse)
async def cross_holdings_page(request: Request, date: str = None):
    """跨ETF重複持股頁面"""
    try:
        dates = db_query.get_available_dates()
        
        if not date and dates:
            date = dates[0]
        
        cross_holdings = []
        if date:
            cross_holdings = db_query.get_cross_etf_holdings(date)
        
        return templates.TemplateResponse("cross_holdings.html", {
            "request": request,
            "dates": dates,
            "selected_date": date,
            "cross_holdings": cross_holdings
        })
    except Exception as e:
        logger.error(f"跨ETF重複持股頁面加載錯誤: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/decreased-holdings", response_class=HTMLResponse)
async def decreased_holdings_page(request: Request, date: str = None, etf_code: str = None):
    """每日減持表頁面"""
    try:
        dates = db_query.get_available_dates()
        etf_codes = db_query.get_etf_codes()
        
        if not date and dates:
            date = dates[0]
        
        decreased_holdings = []
        if date:
            decreased_holdings = db_query.get_decreased_holdings(date, etf_code)
        
        return templates.TemplateResponse("decreased_holdings.html", {
            "request": request,
            "dates": dates,
            "etf_codes": etf_codes,
            "selected_date": date,
            "selected_etf": etf_code,
            "decreased_holdings": decreased_holdings
        })
    except Exception as e:
        logger.error(f"減持表頁面加載錯誤: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# ============ 功能路由 ============

@app.post("/manual-scrape")
async def manual_scrape():
    """手動執行爬取"""
    try:
        success_count = scraper.scrape_all_etfs()
        return {"status": "success", "message": f"成功爬取 {success_count} 個ETF"}
    except Exception as e:
        logger.error(f"手動爬取失敗: {e}")
        return {"status": "error", "message": str(e)}

# ============ API 路由 ============

@app.get("/api/holdings/{date}")
async def api_holdings(date: str, etf_code: str = None):
    """API: 獲取持股數據"""
    try:
        return db_query.get_holdings_by_date_and_etf(date, etf_code)
    except Exception as e:
        logger.error(f"API持股查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/new-holdings/{date}")
async def api_new_holdings(date: str, etf_code: str = None):
    """API: 獲取新增持股數據"""
    try:
        return db_query.get_new_holdings(date, etf_code)
    except Exception as e:
        logger.error(f"API新增持股查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cross-holdings/{date}")
async def api_cross_holdings(date: str):
    """API: 獲取跨ETF重複持股數據"""
    try:
        return db_query.get_cross_etf_holdings(date)
    except Exception as e:
        logger.error(f"API跨ETF查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/decreased-holdings/{date}")
async def api_decreased_holdings(date: str, etf_code: str = None):
    """API: 獲取減持數據"""
    try:
        return db_query.get_decreased_holdings(date, etf_code)
    except Exception as e:
        logger.error(f"API減持查詢錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ 主程式入口 ============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)