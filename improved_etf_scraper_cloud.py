# improved_etf_scraper_cloud.py - 修改版本
import requests
import time
import json
from datetime import datetime, timedelta
import logging
import traceback
from database_config import db_config

# Cloud Run 友善的日誌設定
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 輸出到 stdout，Cloud Run 會自動收集
    ]
)
logger = logging.getLogger(__name__)

class ETFHoldingsScraper:
    def __init__(self):
        self.base_url = 'https://www.pocket.tw/api/cm/MobileService/ashx/GetDtnoData.ashx'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.pocket.tw/etf/tw/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        # 統一的DtNo，所有ETF都使用相同的
        self.dtno = '59449513'
        
        # 支援的ETF代碼清單
        self.etf_codes = ['00981A', '00982A', '00983A', '00984A', '00985A']
        
        self.init_database()
    
    def init_database(self):
        """初始化數據庫表"""
        try:
            # 主要持股表
            holdings_table_sql = '''
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
            
            # 持股變化表
            changes_table_sql = '''
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
            
            db_config.execute_query(holdings_table_sql)
            db_config.execute_query(changes_table_sql)
            
            # 創建索引
            if db_config.db_type == "postgresql":
                # PostgreSQL 索引
                index_sqls = [
                    'CREATE INDEX IF NOT EXISTS idx_etf_date ON etf_holdings(etf_code, update_date)',
                    'CREATE INDEX IF NOT EXISTS idx_changes_date ON holdings_changes(etf_code, change_date)',
                    'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_holding ON etf_holdings(etf_code, stock_code, update_date)'
                ]
            else:
                # SQLite 索引
                index_sqls = [
                    'CREATE INDEX IF NOT EXISTS idx_etf_date ON etf_holdings(etf_code, update_date)',
                    'CREATE INDEX IF NOT EXISTS idx_changes_date ON holdings_changes(etf_code, change_date)'
                ]
            
            for sql in index_sqls:
                try:
                    db_config.execute_query(sql)
                except Exception as e:
                    logger.warning(f"創建索引失敗（可能已存在）: {e}")
            
            logger.info(f"數據庫初始化完成 - 使用 {db_config.db_type}")
            
        except Exception as e:
            logger.error(f"數據庫初始化錯誤: {e}")
            raise e
    
    def get_holdings_data(self, etf_code):
        """獲取指定ETF的持股明細"""
        if etf_code not in self.etf_codes:
            logger.error(f"不支援的ETF代碼: {etf_code}")
            return None
        
        params = {
            'action': 'getdtnodata',
            'DtNo': self.dtno,
            'ParamStr': f'AssignID={etf_code};MTPeriod=0;DTMode=0;DTRange=1;DTOrder=1;MajorTable=M722;',
            'FilterNo': '0'
        }
        
        try:
            response = requests.get(self.base_url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"成功獲取 {etf_code} 的持股數據")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"獲取 {etf_code} 數據時出錯: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析 {etf_code} JSON數據時出錯: {e}")
            return None
    
    def parse_holdings_data(self, data, etf_code):
        """解析持股明細數據"""
        holdings = []
        
        try:
            # 檢查數據結構
            if not data or 'Data' not in data:
                logger.warning(f"{etf_code} 回應中沒有 Data 數據")
                return holdings
            
            title = data.get('Title', [])
            rows_data = data.get('Data', [])
            
            logger.info(f"欄位名稱: {title}")
            logger.info(f"數據行數: {len(rows_data)}")
            
            if not rows_data:
                logger.warning(f"{etf_code} Data 為空")
                return holdings
            
            # 確認欄位對應 ["日期","標的代號","標的名稱","權重(%)","持有數","單位"]
            for row in rows_data:
                try:
                    if len(row) < 6:
                        logger.warning(f"數據行長度不足: {row}")
                        continue
                    
                    date_str = str(row[0]).strip()
                    stock_code = str(row[1]).strip()
                    stock_name = str(row[2]).strip()
                    weight_str = str(row[3]).strip()
                    shares_str = str(row[4]).strip()
                    unit = str(row[5]).strip()
                    
                    # 處理權重 - 直接轉換為浮點數
                    weight = float(weight_str) if weight_str else 0.0
                    
                    # 處理持有數 - 移除逗號並轉換為整數
                    shares = int(shares_str.replace(',', '')) if shares_str else 0
                    
                    holding = {
                        'etf_code': etf_code,
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'weight': weight,
                        'shares': shares,
                        'unit': unit,
                        'update_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    
                    # 只有當股票代碼和名稱不為空時才添加
                    if holding['stock_code'] and holding['stock_name']:
                        holdings.append(holding)
                        
                except (ValueError, TypeError, IndexError) as e:
                    logger.warning(f"解析 {etf_code} 單筆數據時出錯: {e}, 數據: {row}")
                    continue
            
            logger.info(f"解析 {etf_code} 持股明細 {len(holdings)} 筆")
            return holdings
            
        except Exception as e:
            logger.error(f"解析 {etf_code} 持股數據時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def get_previous_holdings(self, etf_code, current_date):
        """獲取前一交易日的持股數據"""
        try:
            query = '''
                SELECT stock_code, stock_name, weight, shares
                FROM etf_holdings 
                WHERE etf_code = %s AND update_date < %s
                ORDER BY update_date DESC 
                LIMIT 1000
            '''
            
            results = db_config.execute_query(query, (etf_code, current_date), fetch="all")
            
            previous_data = {}
            for row in results:
                stock_code = row['stock_code']
                previous_data[stock_code] = {
                    'stock_name': row['stock_name'],
                    'weight': row['weight'],
                    'shares': row['shares']
                }
            
            return previous_data
            
        except Exception as e:
            logger.error(f"獲取前一日持股數據錯誤: {e}")
            return {}
    
    def analyze_holdings_changes(self, etf_code, current_holdings, current_date):
        """分析持股變化"""
        previous_holdings = self.get_previous_holdings(etf_code, current_date)
        changes = []
        
        # 當前持股字典
        current_dict = {h['stock_code']: h for h in current_holdings}
        
        # 檢查新增和變化的股票
        for stock_code, current_data in current_dict.items():
            if stock_code not in previous_holdings:
                # 新增的股票
                changes.append({
                    'etf_code': etf_code,
                    'stock_code': stock_code,
                    'stock_name': current_data['stock_name'],
                    'change_type': 'NEW',
                    'old_shares': 0,
                    'new_shares': current_data['shares'],
                    'old_weight': 0.0,
                    'new_weight': current_data['weight'],
                    'change_date': current_date
                })
            else:
                # 檢查持股數量變化
                old_data = previous_holdings[stock_code]
                if current_data['shares'] > old_data['shares']:
                    changes.append({
                        'etf_code': etf_code,
                        'stock_code': stock_code,
                        'stock_name': current_data['stock_name'],
                        'change_type': 'INCREASED',
                        'old_shares': old_data['shares'],
                        'new_shares': current_data['shares'],
                        'old_weight': old_data['weight'],
                        'new_weight': current_data['weight'],
                        'change_date': current_date
                    })
                elif current_data['shares'] < old_data['shares']:
                    changes.append({
                        'etf_code': etf_code,
                        'stock_code': stock_code,
                        'stock_name': current_data['stock_name'],
                        'change_type': 'DECREASED',
                        'old_shares': old_data['shares'],
                        'new_shares': current_data['shares'],
                        'old_weight': old_data['weight'],
                        'new_weight': current_data['weight'],
                        'change_date': current_date
                    })
        
        # 檢查移除的股票
        for stock_code, old_data in previous_holdings.items():
            if stock_code not in current_dict:
                changes.append({
                    'etf_code': etf_code,
                    'stock_code': stock_code,
                    'stock_name': old_data['stock_name'],
                    'change_type': 'REMOVED',
                    'old_shares': old_data['shares'],
                    'new_shares': 0,
                    'old_weight': old_data['weight'],
                    'new_weight': 0.0,
                    'change_date': current_date
                })
        
        return changes
    
    def save_to_database(self, holdings, changes=None):
        """將持股明細和變化存入資料庫"""
        if not holdings:
            return
        
        try:
            etf_code = holdings[0]['etf_code']
            today = holdings[0]['update_date']
            
            # 刪除當日舊數據
            delete_query = 'DELETE FROM etf_holdings WHERE etf_code = %s AND update_date = %s'
            db_config.execute_query(delete_query, (etf_code, today))
            
            # 插入持股數據
            insert_holding_query = '''
                INSERT INTO etf_holdings 
                (etf_code, stock_code, stock_name, weight, shares, unit, update_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            
            for holding in holdings:
                db_config.execute_query(insert_holding_query, (
                    holding['etf_code'], holding['stock_code'], holding['stock_name'],
                    holding['weight'], holding['shares'], holding['unit'], holding['update_date']
                ))
            
            # 插入變化數據
            if changes:
                delete_changes_query = 'DELETE FROM holdings_changes WHERE etf_code = %s AND change_date = %s'
                db_config.execute_query(delete_changes_query, (etf_code, today))
                
                insert_change_query = '''
                    INSERT INTO holdings_changes 
                    (etf_code, stock_code, stock_name, change_type, old_shares, new_shares, 
                     old_weight, new_weight, change_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                
                for change in changes:
                    db_config.execute_query(insert_change_query, (
                        change['etf_code'], change['stock_code'], change['stock_name'],
                        change['change_type'], change['old_shares'], change['new_shares'],
                        change['old_weight'], change['new_weight'], change['change_date']
                    ))
            
            logger.info(f"成功存入 {etf_code} 持股明細 {len(holdings)} 筆，變化 {len(changes) if changes else 0} 筆")
            
        except Exception as e:
            logger.error(f"存入資料庫時出錯: {e}")
            logger.error(traceback.format_exc())
            raise e
    
    def scrape_single_etf(self, etf_code):
        """爬取單個ETF的持股明細"""
        logger.info(f"正在處理: {etf_code}")
        
        # 獲取數據
        data = self.get_holdings_data(etf_code)
        if not data:
            logger.error(f"無法獲取 {etf_code} 的數據")
            return False
        
        # 解析數據
        holdings = self.parse_holdings_data(data, etf_code)
        if not holdings:
            logger.warning(f"{etf_code} 無持股數據")
            return False
        
        # 分析變化
        current_date = datetime.now().strftime('%Y-%m-%d')
        changes = self.analyze_holdings_changes(etf_code, holdings, current_date)
        
        # 存入資料庫
        self.save_to_database(holdings, changes)
        return True
    
    def scrape_all_etfs(self):
        """爬取所有ETF的持股明細"""
        logger.info("開始爬取所有ETF持股明細")
        
        success_count = 0
        for etf_code in self.etf_codes:
            try:
                if self.scrape_single_etf(etf_code):
                    success_count += 1
                # 避免請求過於頻繁
                time.sleep(2)
            except Exception as e:
                logger.error(f"處理 {etf_code} 時發生錯誤: {e}")
                logger.error(traceback.format_exc())
        
        logger.info(f"爬取完成，成功: {success_count}/{len(self.etf_codes)}")
        return success_count

    def test_single_request(self, etf_code='00981A'):
        """測試單個請求，用於確認程式正確性"""
        logger.info(f"測試請求 {etf_code}")
        data = self.get_holdings_data(etf_code)
        if data:
            logger.info(f"原始回應數據結構: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            
            # 詳細檢查數據結構
            if isinstance(data, dict):
                for key in data.keys():
                    value = data[key]
                    if isinstance(value, list):
                        logger.info(f"鍵 '{key}' 包含 {len(value)} 個項目")
                        if value and len(value) > 0:
                            logger.info(f"第一個項目範例: {value[0]}")
                    else:
                        logger.info(f"鍵 '{key}': {type(value)} - {value}")
            
            holdings = self.parse_holdings_data(data, etf_code)
            logger.info(f"解析後持股數量: {len(holdings)}")
            if holdings:
                logger.info(f"第一筆持股範例: {holdings[0]}")
            return holdings
        return None