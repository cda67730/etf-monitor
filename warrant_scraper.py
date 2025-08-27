# warrant_scraper_html_complete.py - 完整HTML解析權證爬蟲
import requests
import re
import time
import random
from datetime import datetime
import logging
import traceback
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from database_config import db_config

# 設置日誌
logger = logging.getLogger(__name__)

class WarrantScraperHTMLComplete:
    """完整HTML解析權證爬蟲"""
    
    def __init__(self):
        self.session = requests.Session()
        
        # 根據診斷結果，使用基本但有效的標頭
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # 確認數據庫可用性
        if not db_config:
            raise Exception("數據庫配置不可用，無法初始化權證爬蟲")
        
        logger.info(f"完整HTML解析權證爬蟲初始化完成")
    
    def get_warrant_data(self, sort_type=3, pages=5):
        """爬取權證資料"""
        
        if isinstance(pages, int):
            pages = list(range(1, pages + 1))
        
        all_warrants = []
        
        logger.info(f"開始爬取權證資料，頁數: {pages}，排序類型: {sort_type}")
        
        for page_num in pages:
            logger.info(f"正在爬取第 {page_num} 頁...")
            
            url = f"https://ebroker-dj.fbs.com.tw/WRT/zx/zxd/zxd.djhtm?A={sort_type}&B=&Page={page_num}"
            
            try:
                response = self.session.get(url, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"第 {page_num} 頁請求失敗: {response.status_code}")
                    continue
                
                # 處理編碼（根據診斷結果，big5編碼有效）
                try:
                    content = response.content.decode('big5', errors='ignore')
                    logger.info(f"第 {page_num} 頁成功解碼，內容長度: {len(content)}")
                except Exception as e:
                    logger.error(f"第 {page_num} 頁解碼失敗: {e}")
                    continue
                
                # 解析HTML獲取權證資料
                warrants = self._parse_html_content(content, page_num)
                
                if warrants:
                    logger.info(f"第 {page_num} 頁成功獲取 {len(warrants)} 筆權證")
                    all_warrants.extend(warrants)
                else:
                    logger.warning(f"第 {page_num} 頁未獲取到權證資料")
                
                # 延遲避免請求過於頻繁
                time.sleep(random.uniform(2.0, 4.0))
                
            except Exception as e:
                logger.error(f"第 {page_num} 頁發生錯誤: {e}")
                logger.error(traceback.format_exc())
                continue
        
        logger.info(f"總共獲取 {len(all_warrants)} 筆權證資料")
        return all_warrants
    
    def _parse_html_content(self, content, page_num):
        """解析HTML內容獲取權證資料"""
        warrants = []
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # 尋找包含權證資料的主表格
            main_table = None
            tables = soup.find_all('table')
            
            for table in tables:
                # 檢查表格是否包含權證資料
                table_text = table.get_text()
                if '權證商品' in table_text and ('成交量' in table_text or '排行' in table_text):
                    main_table = table
                    logger.info(f"第 {page_num} 頁找到權證資料表格")
                    break
            
            if not main_table:
                logger.warning(f"第 {page_num} 頁未找到權證資料表格")
                # 嘗試直接從頁面中提取權證連結
                return self._extract_from_page_content(content, page_num)
            
            # 解析表格行
            rows = main_table.find_all('tr')
            logger.info(f"第 {page_num} 頁表格共有 {len(rows)} 行")
            
            ranking = 1
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 6:  # 跳過欄位不足的行
                    continue
                
                # 檢查是否為標題行
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                if '權證商品' in cell_texts or '標的名稱' in cell_texts:
                    continue
                
                # 解析權證資料行
                warrant_data = self._parse_warrant_row(cells, ranking, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
                    ranking += 1
                
                # 限制每頁最多20筆
                if ranking > 20:
                    break
            
            logger.info(f"第 {page_num} 頁從表格解析出 {len(warrants)} 筆權證")
            
            # 如果表格解析失敗，嘗試從頁面內容直接提取
            if len(warrants) == 0:
                warrants = self._extract_from_page_content(content, page_num)
            
            return warrants
            
        except Exception as e:
            logger.error(f"解析第 {page_num} 頁HTML內容失敗: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def _parse_warrant_row(self, cells, ranking, page_num):
        """解析表格行中的權證資料"""
        try:
            # 從所有單元格中尋找權證連結和資料
            warrant_code = ""
            warrant_name = ""
            underlying_name = ""
            warrant_type = ""
            close_price = 0.0
            change_amount = 0.0
            change_percent = 0.0
            volume = 0
            implied_vol = 0.0
            
            # 提取所有單元格的文本和HTML
            cell_texts = []
            cell_html = ""
            for cell in cells:
                cell_texts.append(cell.get_text(strip=True))
                cell_html += str(cell)

            # 尋找權證連結 Link2Stk('AQ... 
            warrant_match = re.search(r"Link2Stk	ing('AQ([A-Z0-9]+)'	ing);(.+?)</a>", cell_html)
            if warrant_match:
                warrant_code = warrant_match.group(1)
                warrant_name = warrant_match.group(2).replace(f"{warrant_code}&nbsp;", "").strip()

            if not warrant_code:
                return None

            # 尋找標的股票名稱 GenLink2stk
            underlying_match = re.search(r"GenLink2stk	ing('AS[A-Z0-9]+	ing','([^']+)'	ing)", cell_html)
            if underlying_match:
                underlying_name = underlying_match.group(1)
            
            logger.debug(f"找到權證: {warrant_code}, 標的: {underlying_name}, 單元格內容: {cell_texts}")
            
            # 從單元格文本中提取數值資料
            for text in cell_texts:
                if not text or text == '|':
                    continue
                
                # 權證類型
                if text in ['認購', '認售']:
                    warrant_type = text
                # 價格（小數點，通常小於100）
                elif self._is_price_like(text) and close_price == 0.0:
                    close_price = self._safe_float(text)
                # 漲跌（可能為負數）
                elif self._is_change_like(text) and change_amount == 0.0:
                    change_amount = self._safe_float(text)
                # 百分比
                elif '%' in text and change_percent == 0.0:
                    change_percent = self._safe_float(text.replace('%', ''))
                # 成交量（大數字，有逗號）
                elif self._is_volume_like(text) and volume == 0:
                    volume = self._safe_int(text.replace(',', ''))
                # 隱含波動率（百分比，通常10-200之間）
                elif self._is_implied_vol_like(text) and implied_vol == 0.0:
                    implied_vol = self._safe_float(text.replace('%', ''))
            
            # 驗證必要欄位
            if not warrant_type or warrant_type not in ['認購', '認售']:
                warrant_type = "認購"  # 預設值
            
            return {
                'ranking': ranking,
                'warrant_code': warrant_code,
                'warrant_name': warrant_name,
                'underlying_name': underlying_name,
                'warrant_type': warrant_type,
                'close_price': close_price,
                'change_amount': change_amount,
                'change_percent': abs(change_percent),
                'volume': volume,
                'implied_volatility': implied_vol,
                'page_number': page_num,
                'update_date': datetime.now().strftime('%Y-%m-%d'),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.warning(f"解析權證行失敗: {e}")
            return None
    
    def _extract_from_page_content(self, content, page_num):
        """從頁面內容直接提取權證資料（備用方法）"""
        warrants = []
        
        try:
            # 使用正則表達式提取權證連結
            warrant_pattern = r"Link2Stk\('AQ([A-Z0-9]+)'\)"
            warrant_codes = re.findall(warrant_pattern, content)
            
            logger.info(f"第 {page_num} 頁從內容中找到 {len(warrant_codes)} 個權證代碼")
            
            for i, code in enumerate(warrant_codes):
                if i >= 20:  # 限制每頁20筆
                    break
                
                # 嘗試從周圍文本提取更多資訊
                warrant_data = self._extract_warrant_details_from_context(content, code, i + 1, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
            
            return warrants
            
        except Exception as e:
            logger.error(f"從頁面內容提取資料失敗: {e}")
            return []
    
    def _extract_warrant_details_from_context(self, content, warrant_code, ranking, page_num):
        """從上下文中提取權證詳細資訊"""
        try:
            # 尋找權證代碼附近的文本
            pattern = rf"Link2Stk\('AQ{warrant_code}'\)"
            match = re.search(pattern, content)
            
            if not match:
                return None
            
            # 獲取權證代碼前後的文本
            start = max(0, match.start() - 200)
            end = min(len(content), match.end() + 200)
            context = content[start:end]
            
            # 提取權證名稱（通常在連結文本中）
            name_pattern = rf"\[{warrant_code}\s+([^\]]+)\]"
            name_match = re.search(name_pattern, context)
            warrant_name = name_match.group(1).strip() if name_match else f"權證{warrant_code}"
            
            # 嘗試提取數值（這裡可以進一步優化）
            numbers = re.findall(r'(\d+\.?\d*)', context)
            
            return {
                'ranking': ranking,
                'warrant_code': warrant_code,
                'warrant_name': warrant_name,
                'underlying_name': "",
                'warrant_type': "認購",  # 預設值
                'close_price': 0.0,
                'change_amount': 0.0,
                'change_percent': 0.0,
                'volume': 0,
                'implied_volatility': 0.0,
                'page_number': page_num,
                'update_date': datetime.now().strftime('%Y-%m-%d'),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.warning(f"從上下文提取權證 {warrant_code} 詳情失敗: {e}")
            return None
    
    def _is_price_like(self, text):
        """判斷是否像價格"""
        try:
            val = float(text.replace(',', ''))
            return 0.01 <= val <= 100.0
        except:
            return False
    
    def _is_change_like(self, text):
        """判斷是否像漲跌"""
        try:
            val = float(text.replace(',', ''))
            return -50.0 <= val <= 50.0
        except:
            return False
    
    def _is_volume_like(self, text):
        """判斷是否像成交量"""
        try:
            if ',' not in text:
                return False
            val = int(text.replace(',', ''))
            return val >= 100
        except:
            return False
    
    def _is_implied_vol_like(self, text):
        """判斷是否像隱含波動率"""
        try:
            if '%' not in text:
                return False
            val = float(text.replace('%', ''))
            return 5.0 <= val <= 500.0
        except:
            return False
    
    def _safe_float(self, text):
        """安全轉換浮點數"""
        try:
            if not text or text in ['', '|', '-']:
                return 0.0
            cleaned = text.replace(',', '').replace('%', '').strip()
            return float(cleaned)
        except:
            return 0.0
    
    def _safe_int(self, text):
        """安全轉換整數"""
        try:
            if not text or text in ['', '|', '-']:
                return 0
            cleaned = text.replace(',', '').strip()
            return int(float(cleaned))
        except:
            return 0
    
    def save_warrants_to_database(self, warrants: List[Dict[str, Any]], date: str = None):
        """將權證資料存入資料庫"""
        if not warrants:
            logger.warning("沒有權證資料需要保存")
            return False
        
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        # 去重處理
        unique_warrants = {}
        for warrant in warrants:
            warrant_code = warrant.get('warrant_code')
            if warrant_code and warrant_code not in unique_warrants:
                unique_warrants[warrant_code] = warrant
        
        deduplicated_warrants = list(unique_warrants.values())
        original_count = len(warrants)
        deduplicated_count = len(deduplicated_warrants)

        if original_count > deduplicated_count:
            logger.info(f"去重處理完成，原始資料: {original_count} 筆, 去重後: {deduplicated_count} 筆")

        logger.info(f"開始保存 {deduplicated_count} 筆權證資料到資料庫 (日期: {date})...")
        
        # 根據數據庫類型使用正確的參數佔位符
        if db_config.db_type == "postgresql":
            ph = "%s"
        else:
            ph = "?"
        
        try:
            with db_config.get_connection() as conn:
                # 關閉 autocommit 以啟用事務
                original_autocommit = getattr(conn, 'autocommit', None)
                if original_autocommit is not None:
                    conn.autocommit = False
                
                cursor = conn.cursor()
                
                try:
                    # 1. 刪除當日舊資料
                    delete_query = f'DELETE FROM warrant_data WHERE update_date = {ph}'
                    cursor.execute(delete_query, (date,))
                    logger.info(f"已清理 {date} 的舊權證資料")
                    
                    # 2. 插入權證資料
                    insert_query = f'''
                        INSERT INTO warrant_data 
                        (ranking, warrant_code, warrant_name, underlying_name, warrant_type,
                         close_price, change_amount, change_percent, volume, implied_volatility,
                         page_number, update_date)
                        VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                    '''
                    
                    warrants_inserted = 0
                    for warrant in deduplicated_warrants:
                        cursor.execute(insert_query, (
                            warrant['ranking'], warrant['warrant_code'], warrant['warrant_name'],
                            warrant['underlying_name'], warrant['warrant_type'],
                            warrant['close_price'], warrant['change_amount'], warrant['change_percent'],
                            warrant['volume'], warrant['implied_volatility'],
                            warrant['page_number'], warrant['update_date']
                        ))
                        warrants_inserted += 1
                    
                    # 3. 更新標的統計資料
                    self._update_underlying_summary(cursor, date, ph)
                    
                    # 4. 提交事務
                    conn.commit()
                    
                    logger.info(f"成功保存 {warrants_inserted} 筆權證資料")
                    return True
                    
                except Exception as e:
                    # 回滾事務
                    conn.rollback()
                    logger.error(f"保存權證資料時出錯，事務已回滾: {e}")
                    raise e
                
                finally:
                    # 恢復原始 autocommit 設置
                    if original_autocommit is not None:
                        conn.autocommit = original_autocommit
                    
        except Exception as e:
            logger.error(f"存入權證資料庫時出錯: {e}")
            logger.error(f"錯誤詳情: {traceback.format_exc()}")
            return False
    
    def _update_underlying_summary(self, cursor, date: str, ph: str):
        """更新標的統計表"""
        try:
            # 先刪除當日舊統計資料
            delete_summary_query = f'DELETE FROM warrant_underlying_summary WHERE update_date = {ph}'
            cursor.execute(delete_summary_query, (date,))
            
            # 重新計算並插入統計資料
            summary_query = f'''
                INSERT INTO warrant_underlying_summary 
                (underlying_name, warrant_type, warrant_count, total_volume, 
                 avg_implied_volatility, total_change_amount, update_date)
                SELECT 
                    underlying_name,
                    warrant_type,
                    COUNT(*) as warrant_count,
                    SUM(volume) as total_volume,
                    AVG(implied_volatility) as avg_implied_volatility,
                    SUM(ABS(change_amount)) as total_change_amount,
                    {ph} as update_date
                FROM warrant_data 
                WHERE update_date = {ph} AND underlying_name IS NOT NULL AND underlying_name != ''
                GROUP BY underlying_name, warrant_type
            '''
            
            cursor.execute(summary_query, (date, date))
            logger.info(f"標的統計資料更新完成")
            
        except Exception as e:
            logger.error(f"更新標的統計資料失敗: {e}")
            raise e
    
    def scrape_warrants(self, pages: int = 5, sort_type: int = 3):
        """執行權證爬取"""
        logger.info(f"開始執行完整HTML解析權證爬取，頁數: {pages}, 排序類型: {sort_type}")
        
        try:
            # 爬取權證資料
            warrants = self.get_warrant_data(sort_type=sort_type, pages=pages)
            
            if not warrants:
                logger.warning("未能獲取權證資料")
                return False
            
            # 保存到資料庫
            success = self.save_warrants_to_database(warrants)
            
            if success:
                logger.info(f"完整HTML解析權證爬取完成，成功處理 {len(warrants)} 筆資料")
                return True
            else:
                logger.error("權證資料保存失敗")
                return False
                
        except Exception as e:
            logger.error(f"完整HTML解析權證爬取過程中發生錯誤: {e}")
            logger.error(traceback.format_exc())
            return False

# 為了向後兼容，使用原來的類名
class WarrantScraper(WarrantScraperHTMLComplete):
    pass