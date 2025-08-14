# improved_etf_scraper_cloud_final.py - 完全修復版本（解決所有邏輯問題）
import requests
import time
import json
from datetime import datetime, timedelta
import logging
import traceback
import re
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
        self.etf_codes = ['00980A', '00981A', '00982A', '00983A', '00984A', '00985A']
        
        # 🔧 關鍵修復：驗證數據庫可用性
        if not db_config:
            raise Exception("❌ 數據庫配置不可用，無法初始化爬蟲")
        
        logger.info(f"✅ 初始化爬蟲，數據庫類型: {db_config.db_type}")
        self.init_database()
    
    def parse_date_from_api(self, date_str):
        """🔧 關鍵修復：解析API返回的日期"""
        try:
            if not date_str or not isinstance(date_str, str):
                logger.warning(f"⚠️ 無效的日期字符串: {date_str}")
                return datetime.now().strftime('%Y-%m-%d')
            
            date_str = date_str.strip()
            
            # 常見的日期格式
            date_patterns = [
                r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY-MM-DD 或 YYYY/MM/DD
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # MM/DD/YYYY 或 DD/MM/YYYY
                r'(\d{4})(\d{2})(\d{2})',              # YYYYMMDD
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_str)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        # 判斷年份位置
                        if len(groups[0]) == 4:  # YYYY-MM-DD
                            year, month, day = groups
                        elif len(groups[2]) == 4:  # MM/DD/YYYY
                            month, day, year = groups
                        else:
                            continue
                        
                        try:
                            parsed_date = datetime(int(year), int(month), int(day))
                            result = parsed_date.strftime('%Y-%m-%d')
                            logger.debug(f"📅 解析日期: '{date_str}' → '{result}'")
                            return result
                        except ValueError:
                            continue
            
            # 如果都解析失敗，使用當前日期
            logger.warning(f"⚠️ 無法解析日期格式: '{date_str}'，使用當前日期")
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.error(f"❌ 日期解析錯誤: {e}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def init_database(self):
        """初始化數據庫表"""
        try:
            # 根據數據庫類型調整SQL語法
            if db_config.db_type == "postgresql":
                id_type = "SERIAL PRIMARY KEY"
                timestamp_default = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            else:
                id_type = "INTEGER PRIMARY KEY AUTOINCREMENT"
                timestamp_default = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            
            # 主要持股表
            holdings_table_sql = f'''
                CREATE TABLE IF NOT EXISTS etf_holdings (
                    id {id_type},
                    etf_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    weight REAL NOT NULL,
                    shares INTEGER NOT NULL,
                    unit TEXT DEFAULT '股',
                    update_date TEXT NOT NULL,
                    created_at {timestamp_default}
                )
            '''
            
            # 持股變化表
            changes_table_sql = f'''
                CREATE TABLE IF NOT EXISTS holdings_changes (
                    id {id_type},
                    etf_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    old_shares INTEGER DEFAULT 0,
                    new_shares INTEGER DEFAULT 0,
                    old_weight REAL DEFAULT 0.0,
                    new_weight REAL DEFAULT 0.0,
                    change_date TEXT NOT NULL,
                    created_at {timestamp_default}
                )
            '''
            
            db_config.execute_query(holdings_table_sql)
            db_config.execute_query(changes_table_sql)
            
            # 創建索引
            index_sqls = [
                'CREATE INDEX IF NOT EXISTS idx_etf_date ON etf_holdings(etf_code, update_date)',
                'CREATE INDEX IF NOT EXISTS idx_changes_date ON holdings_changes(etf_code, change_date)'
            ]
            
            if db_config.db_type == "postgresql":
                # PostgreSQL 特有的唯一索引
                try:
                    unique_index_sql = 'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_holding ON etf_holdings(etf_code, stock_code, update_date)'
                    db_config.execute_query(unique_index_sql)
                except Exception as e:
                    logger.warning(f"創建唯一索引失敗（可能已存在）: {e}")
            
            for sql in index_sqls:
                try:
                    db_config.execute_query(sql)
                except Exception as e:
                    logger.warning(f"創建索引失敗（可能已存在）: {e}")
            
            logger.info(f"✅ 數據庫初始化完成 - 使用 {db_config.db_type}")
            
        except Exception as e:
            logger.error(f"❌ 數據庫初始化錯誤: {e}")
            raise e
    
    def check_existing_data(self, etf_code, date):
        """🔧 新增：檢查是否已有該日期的數據"""
        try:
            if db_config.db_type == "postgresql":
                placeholder = "%s"
            else:
                placeholder = "?"
            
            query = f'SELECT COUNT(*) as count FROM etf_holdings WHERE etf_code = {placeholder} AND update_date = {placeholder}'
            result = db_config.execute_query(query, (etf_code, date), fetch="one")
            
            count = result['count'] if result else 0
            return count > 0
            
        except Exception as e:
            logger.error(f"❌ 檢查現有數據時出錯: {e}")
            return False
    
    def get_holdings_data(self, etf_code):
        """獲取指定ETF的持股明細"""
        if etf_code not in self.etf_codes:
            logger.error(f"❌ 不支持的ETF代碼: {etf_code}")
            return None
        
        params = {
            'action': 'getdtnodata',
            'DtNo': self.dtno,
            'ParamStr': f'AssignID={etf_code};MTPeriod=0;DTMode=0;DTRange=1;DTOrder=1;MajorTable=M722;',
            'FilterNo': '0'
        }
        
        try:
            logger.info(f"🔄 開始獲取 {etf_code} 數據...")
            response = requests.get(self.base_url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"✅ 成功獲取 {etf_code} 的持股數據")
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ 獲取 {etf_code} 數據超時")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 獲取 {etf_code} 數據時網路錯誤: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ 解析 {etf_code} JSON數據時出錯: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 獲取 {etf_code} 數據時未知錯誤: {e}")
            return None
    
    def parse_holdings_data(self, data, etf_code):
        """🔧 關鍵修復：解析持股明細數據（使用API返回的日期）"""
        holdings = []
        
        try:
            # 檢查數據結構
            if not data or 'Data' not in data:
                logger.warning(f"⚠️ {etf_code} 回應中沒有 Data 數據")
                return holdings
            
            title = data.get('Title', [])
            rows_data = data.get('Data', [])
            
            logger.info(f"📊 {etf_code} 欄位名稱: {title}")
            logger.info(f"📊 {etf_code} 數據行數: {len(rows_data)}")
            
            if not rows_data:
                logger.warning(f"⚠️ {etf_code} Data 為空")
                return holdings
            
            # 🔧 關鍵修復：從第一筆數據解析日期
            data_date = None
            if rows_data and len(rows_data[0]) > 0:
                first_date_str = str(rows_data[0][0]).strip()
                data_date = self.parse_date_from_api(first_date_str)
                logger.info(f"📅 {etf_code} 數據日期: {data_date}")
            
            if not data_date:
                data_date = datetime.now().strftime('%Y-%m-%d')
                logger.warning(f"⚠️ {etf_code} 無法解析數據日期，使用當前日期: {data_date}")
            
            # 🔧 關鍵修復：檢查是否已有該日期的數據
            if self.check_existing_data(etf_code, data_date):
                logger.info(f"ℹ️ {etf_code} {data_date} 的數據已存在，將覆蓋")
            
            successful_parsed = 0
            
            # 確認欄位對應 ["日期","標的代號","標的名稱","權重(%)","持有數","單位"]
            for i, row in enumerate(rows_data):
                try:
                    if len(row) < 6:
                        logger.warning(f"⚠️ 第{i+1}行數據長度不足: {row}")
                        continue
                    
                    date_str = str(row[0]).strip()
                    stock_code = str(row[1]).strip()
                    stock_name = str(row[2]).strip()
                    weight_str = str(row[3]).strip()
                    shares_str = str(row[4]).strip()
                    unit = str(row[5]).strip()
                    
                    # 跳過空白或無效記錄
                    if not stock_code or not stock_name:
                        continue
                    
                    # 處理權重 - 直接轉換為浮點數
                    try:
                        weight = float(weight_str) if weight_str else 0.0
                    except ValueError:
                        logger.warning(f"⚠️ 無法解析權重: {weight_str}")
                        weight = 0.0
                    
                    # 處理持有數 - 移除逗號並轉換為整數
                    try:
                        shares = int(shares_str.replace(',', '')) if shares_str else 0
                    except ValueError:
                        logger.warning(f"⚠️ 無法解析持有數: {shares_str}")
                        shares = 0
                    
                    holding = {
                        'etf_code': etf_code,
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'weight': weight,
                        'shares': shares,
                        'unit': unit,
                        'update_date': data_date  # 🔧 關鍵修復：使用解析的日期
                    }
                    
                    holdings.append(holding)
                    successful_parsed += 1
                        
                except Exception as e:
                    logger.warning(f"⚠️ 解析第{i+1}行數據時出錯: {e}, 數據: {row}")
                    continue
            
            logger.info(f"✅ {etf_code} 解析完成: 總計 {len(rows_data)} 行，成功解析 {successful_parsed} 筆，日期: {data_date}")
            return holdings
            
        except Exception as e:
            logger.error(f"❌ 解析 {etf_code} 持股數據時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def get_previous_holdings(self, etf_code, current_date):
        """🔧 修復：獲取前一交易日的持股數據"""
        try:
            logger.info(f"📅 查找 {etf_code} 在 {current_date} 之前的持股數據...")
            
            # 根據數據庫類型使用正確的參數佔位符
            if db_config.db_type == "postgresql":
                placeholder = "%s"
            else:
                placeholder = "?"
            
            # 第一步：找到前一個交易日
            find_previous_date_query = f'''
                SELECT MAX(update_date) as prev_date
                FROM etf_holdings 
                WHERE etf_code = {placeholder} AND update_date < {placeholder}
            '''
            
            result = db_config.execute_query(
                find_previous_date_query, 
                (etf_code, current_date), 
                fetch="one"
            )
            
            if not result or not result.get('prev_date'):
                logger.info(f"📅 {etf_code} 沒有找到前一交易日的數據")
                return {}
            
            previous_date = result['prev_date']
            logger.info(f"📅 {etf_code} 找到前一交易日: {previous_date}")
            
            # 第二步：獲取那一天的記錄
            get_holdings_query = f'''
                SELECT stock_code, stock_name, weight, shares
                FROM etf_holdings 
                WHERE etf_code = {placeholder} AND update_date = {placeholder}
            '''
            
            holdings_results = db_config.execute_query(
                get_holdings_query, 
                (etf_code, previous_date), 
                fetch="all"
            )
            
            # 第三步：建立字典
            previous_data = {}
            if holdings_results:
                for row in holdings_results:
                    stock_code = row['stock_code']
                    previous_data[stock_code] = {
                        'stock_name': row['stock_name'],
                        'weight': row['weight'],
                        'shares': row['shares']
                    }
            
            logger.info(f"📊 {etf_code} 前一日持股數量: {len(previous_data)}")
            return previous_data
            
        except Exception as e:
            logger.error(f"❌ 獲取 {etf_code} 前一日持股時出錯: {e}")
            logger.error(traceback.format_exc())
            return {}

    def analyze_holdings_changes(self, etf_code, current_holdings, current_date):
        """🔧 修復：分析持股變化"""
        try:
            logger.info(f"🔍 分析 {etf_code} 持股變化...")
            
            previous_holdings = self.get_previous_holdings(etf_code, current_date)
            changes = []
            
            # 當前持股字典
            current_dict = {h['stock_code']: h for h in current_holdings}
            
            # 統計變化
            new_count = 0
            increased_count = 0
            decreased_count = 0
            removed_count = 0
            
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
                    new_count += 1
                    logger.debug(f"  ➕ 新增: {stock_code} ({current_data['stock_name']})")
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
                        increased_count += 1
                        logger.debug(f"  📈 增持: {stock_code} {old_data['shares']:,} → {current_data['shares']:,}")
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
                        decreased_count += 1
                        logger.debug(f"  📉 減持: {stock_code} {old_data['shares']:,} → {current_data['shares']:,}")
            
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
                    removed_count += 1
                    logger.debug(f"  ➖ 移除: {stock_code} ({old_data['stock_name']})")
            
            logger.info(f"📊 {etf_code} 變化統計: 新增{new_count}, 增持{increased_count}, 減持{decreased_count}, 移除{removed_count}")
            return changes
            
        except Exception as e:
            logger.error(f"❌ 分析 {etf_code} 持股變化時出錯: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def save_to_database(self, holdings, changes=None):
        """🔧 完全修復：將持股明細和變化存入資料庫（正確的事務處理）"""
        if not holdings:
            logger.warning("⚠️ 沒有持股數據需要保存")
            return False
        
        etf_code = holdings[0]['etf_code']
        date = holdings[0]['update_date']
        
        logger.info(f"💾 開始保存 {etf_code} 的 {len(holdings)} 筆持股數據到資料庫 (日期: {date})...")
        
        # 根據數據庫類型使用正確的參數佔位符
        if db_config.db_type == "postgresql":
            ph = "%s"
        else:
            ph = "?"
        
        try:
            # 🔧 關鍵修復：正確的事務處理
            with db_config.get_connection() as conn:
                # 🔧 關鍵修復：關閉autocommit以啟用事務
                original_autocommit = getattr(conn, 'autocommit', None)
                if original_autocommit is not None:
                    conn.autocommit = False
                
                cursor = conn.cursor()
                
                try:
                    # 1. 刪除當日舊數據
                    delete_holdings_query = f'DELETE FROM etf_holdings WHERE etf_code = {ph} AND update_date = {ph}'
                    cursor.execute(delete_holdings_query, (etf_code, date))
                    logger.debug(f"🗑️ 已清理 {etf_code} {date} 的舊持股數據")
                    
                    delete_changes_query = f'DELETE FROM holdings_changes WHERE etf_code = {ph} AND change_date = {ph}'
                    cursor.execute(delete_changes_query, (etf_code, date))
                    logger.debug(f"🗑️ 已清理 {etf_code} {date} 的舊變化數據")
                    
                    # 2. 插入持股數據
                    insert_holding_query = f'''
                        INSERT INTO etf_holdings 
                        (etf_code, stock_code, stock_name, weight, shares, unit, update_date)
                        VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                    '''
                    
                    holdings_inserted = 0
                    for holding in holdings:
                        cursor.execute(insert_holding_query, (
                            holding['etf_code'], holding['stock_code'], holding['stock_name'],
                            holding['weight'], holding['shares'], holding['unit'], holding['update_date']
                        ))
                        holdings_inserted += 1
                    
                    logger.info(f"✅ 成功插入 {etf_code} {holdings_inserted} 筆持股數據")
                    
                    # 3. 插入變化數據
                    changes_inserted = 0
                    if changes:
                        insert_change_query = f'''
                            INSERT INTO holdings_changes 
                            (etf_code, stock_code, stock_name, change_type, old_shares, new_shares, 
                             old_weight, new_weight, change_date)
                            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                        '''
                        
                        for change in changes:
                            cursor.execute(insert_change_query, (
                                change['etf_code'], change['stock_code'], change['stock_name'],
                                change['change_type'], change['old_shares'], change['new_shares'],
                                change['old_weight'], change['new_weight'], change['change_date']
                            ))
                            changes_inserted += 1
                        
                        logger.info(f"✅ 成功插入 {etf_code} {changes_inserted} 筆變化數據")
                    
                    # 4. 提交事務
                    conn.commit()
                    
                    logger.info(f"🎉 {etf_code} 數據保存完成，事務已提交")
                    return True
                    
                except Exception as e:
                    # 回滾事務
                    conn.rollback()
                    logger.error(f"❌ 保存 {etf_code} 數據時出錯，事務已回滾: {e}")
                    raise e
                
                finally:
                    # 🔧 關鍵修復：恢復原始autocommit設置
                    if original_autocommit is not None:
                        conn.autocommit = original_autocommit
                    
        except Exception as e:
            logger.error(f"❌ 存入資料庫時出錯: {e}")
            logger.error(f"錯誤詳情: {traceback.format_exc()}")
            return False
    
    def scrape_single_etf(self, etf_code):
        """🔧 修復：爬取單個ETF的持股明細"""
        logger.info(f"🎯 開始處理: {etf_code}")
        
        try:
            # 1. 獲取數據
            data = self.get_holdings_data(etf_code)
            if not data:
                logger.error(f"❌ 無法獲取 {etf_code} 的數據")
                return False
            
            # 2. 解析數據
            holdings = self.parse_holdings_data(data, etf_code)
            if not holdings:
                logger.warning(f"⚠️ {etf_code} 無持股數據")
                return False
            
            # 3. 分析變化（使用解析出的日期）
            data_date = holdings[0]['update_date']
            changes = self.analyze_holdings_changes(etf_code, holdings, data_date)
            
            # 4. 存入資料庫
            success = self.save_to_database(holdings, changes)
            
            if success:
                logger.info(f"✅ {etf_code} 處理完成: {len(holdings)} 筆持股, {len(changes)} 項變化 (日期: {data_date})")
                return True
            else:
                logger.error(f"❌ {etf_code} 數據保存失敗")
                return False
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ {etf_code} 網路請求錯誤: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 處理 {etf_code} 時發生未知錯誤: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def scrape_all_etfs(self):
        """🔧 修復：爬取所有ETF的持股明細"""
        start_time = datetime.now()
        logger.info("🚀 開始爬取所有ETF持股明細")
        logger.info(f"📋 待處理ETF: {', '.join(self.etf_codes)}")
        logger.info(f"🕐 開始時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        success_count = 0
        failed_etfs = []
        
        for i, etf_code in enumerate(self.etf_codes, 1):
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 處理 {etf_code} ({i}/{len(self.etf_codes)})")
                
                if self.scrape_single_etf(etf_code):
                    success_count += 1
                    logger.info(f"✅ {etf_code} 成功")
                else:
                    failed_etfs.append(etf_code)
                    logger.error(f"❌ {etf_code} 失敗")
                
                # 避免請求過於頻繁
                if i < len(self.etf_codes):  # 不是最後一個
                    logger.debug("⏳ 等待 2 秒避免頻繁請求...")
                    time.sleep(2)
                    
            except KeyboardInterrupt:
                logger.warning(f"⚠️ 用戶中斷操作，已處理 {i-1}/{len(self.etf_codes)}")
                break
            except Exception as e:
                logger.error(f"❌ 處理 {etf_code} 時發生嚴重錯誤: {e}")
                logger.error(traceback.format_exc())
                failed_etfs.append(etf_code)
        
        # 總結報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("📊 爬取完成總結")
        logger.info(f"🕐 結束時間: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️ 耗時: {duration}")
        logger.info(f"✅ 成功: {success_count}/{len(self.etf_codes)}")
        
        if failed_etfs:
            logger.warning(f"❌ 失敗的ETF: {', '.join(failed_etfs)}")
        else:
            logger.info("🎉 全部成功！")
        
        # 🔧 新增：提供數據庫狀態摘要
        try:
            if db_config.db_type == "postgresql":
                ph = "%s"
            else:
                ph = "?"
            
            summary_query = f'''
                SELECT etf_code, update_date, COUNT(*) as holdings_count
                FROM etf_holdings 
                WHERE update_date = (SELECT MAX(update_date) FROM etf_holdings)
                GROUP BY etf_code, update_date
                ORDER BY etf_code
            '''
            
            summary_results = db_config.execute_query(summary_query, fetch="all")
            if summary_results:
                logger.info("📈 最新數據摘要:")
                for row in summary_results:
                    logger.info(f"  {row['etf_code']}: {row['holdings_count']} 筆持股 ({row['update_date']})")
            
        except Exception as e:
            logger.warning(f"⚠️ 無法獲取數據摘要: {e}")
        
        return success_count

    def test_single_request(self, etf_code='00981A'):
        """🧪 測試單個請求，用於確認程式正確性"""
        logger.info(f"🧪 開始測試 {etf_code}")
        
        try:
            # 測試數據獲取
            data = self.get_holdings_data(etf_code)
            if not data:
                logger.error("❌ 測試失敗：無法獲取數據")
                return None
            
            logger.info(f"📊 原始回應數據結構: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            
            # 詳細檢查數據結構
            if isinstance(data, dict):
                for key in data.keys():
                    value = data[key]
                    if isinstance(value, list):
                        logger.info(f"🔑 鍵 '{key}' 包含 {len(value)} 個項目")
                        if value and len(value) > 0:
                            logger.info(f"📄 第一個項目範例: {value[0]}")
                    else:
                        logger.info(f"🔑 鍵 '{key}': {type(value)} - {str(value)[:100]}...")
            
            # 測試數據解析
            holdings = self.parse_holdings_data(data, etf_code)
            logger.info(f"📈 解析後持股數量: {len(holdings)}")
            if holdings:
                logger.info(f"📄 第一筆持股範例: {holdings[0]}")
                data_date = holdings[0]['update_date']
                logger.info(f"📅 解析出的數據日期: {data_date}")
                
                # 測試變化分析
                changes = self.analyze_holdings_changes(etf_code, holdings, data_date)
                logger.info(f"📊 變化分析結果: {len(changes)} 項變化")
                
                # 測試數據庫操作（不實際保存）
                logger.info("🧪 測試數據庫連接...")
                if db_config.db_type == "postgresql":
                    ph = "%s"
                else:
                    ph = "?"
                
                test_query = f"SELECT COUNT(*) as count FROM etf_holdings WHERE etf_code = {ph}"
                result = db_config.execute_query(test_query, (etf_code,), fetch="one")
                existing_count = result['count'] if result else 0
                logger.info(f"📊 數據庫中現有 {etf_code} 記錄: {existing_count}")
                
                # 測試重複數據檢查
                has_existing = self.check_existing_data(etf_code, data_date)
                logger.info(f"🔍 {data_date} 是否已有數據: {has_existing}")
            
            logger.info("✅ 測試完成")
            return holdings
                
        except Exception as e:
            logger.error(f"❌ 測試過程中出錯: {e}")
            logger.error(traceback.format_exc())
            return None