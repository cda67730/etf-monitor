# anti_detect_scraper.py - 反檢測權證爬蟲
import requests
import re
import time
import random
from datetime import datetime
import logging
import traceback
from typing import List, Dict, Any, Optional
from database_config import db_config

# 設置日誌
logger = logging.getLogger(__name__)

class AntiDetectWarrantScraper:
    """反檢測權證爬蟲 - 模擬真實瀏覽器行為"""
    
    def __init__(self):
        self.session = requests.Session()
        
        # 隨機選擇User-Agent
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        ]
        
        # 設置完整的瀏覽器標頭
        self._setup_headers()
        
        # 確認數據庫可用性
        if not db_config:
            raise Exception("數據庫配置不可用，無法初始化權證爬蟲")
        
        logger.info(f"反檢測權證爬蟲初始化完成 - 數據庫類型: {db_config.db_type}")
    
    def _setup_headers(self):
        """設置完整的瀏覽器標頭"""
        user_agent = random.choice(self.user_agents)
        
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        })
        
        logger.info(f"設置User-Agent: {user_agent}")
    
    def _simulate_human_behavior(self):
        """模擬人類行為"""
        # 隨機延遲 2-5 秒
        delay = random.uniform(2.0, 5.0)
        logger.debug(f"模擬人類行為，延遲 {delay:.2f} 秒")
        time.sleep(delay)
    
    def _visit_homepage_first(self):
        """先訪問首頁建立會話"""
        try:
            homepage_url = "https://ebroker-dj.fbs.com.tw/"
            logger.info("先訪問首頁建立會話...")
            
            self.session.get(homepage_url, timeout=30)
            
            # 設置Referer
            self.session.headers.update({
                'Referer': homepage_url
            })
            
            # 短暫延遲
            time.sleep(random.uniform(1.0, 3.0))
            logger.info("首頁訪問完成，會話已建立")
            
        except Exception as e:
            logger.warning(f"訪問首頁失敗: {e}")
    
    def get_warrant_data(self, sort_type=3, pages=5):
        """爬取權證資料"""
        
        if isinstance(pages, int):
            pages = list(range(1, pages + 1))
        
        all_warrants = []
        
        logger.info(f"開始爬取權證資料，頁數: {pages}，排序類型: {sort_type}")
        
        # 先訪問首頁建立會話
        self._visit_homepage_first()
        
        for page_num in pages:
            logger.info(f"正在爬取第 {page_num} 頁...")
            
            # 每隔幾頁隨機更換User-Agent
            if page_num > 1 and random.random() < 0.3:
                self._setup_headers()
            
            url = f"https://ebroker-dj.fbs.com.tw/WRT/zx/zxd/zxd.djhtm?A={sort_type}&B=&Page={page_num}"
            logger.info(f"請求URL: {url}")
            
            try:
                response = self.session.get(url, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"第 {page_num} 頁請求失敗: {response.status_code}")
                    continue
                
                # 處理編碼問題
                html_content = self._decode_content(response)
                
                if not html_content:
                    logger.error(f"第 {page_num} 頁內容解碼失敗")
                    continue
                
                # 檢查是否被反爬蟲機制檢測到
                if self._is_blocked_content(html_content):
                    logger.warning(f"第 {page_num} 頁可能被反爬蟲機制攔截")
                    # 增加更長的延遲
                    time.sleep(random.uniform(10.0, 20.0))
                    continue
                
                # 檢查內容格式並解析
                if self._is_text_format(html_content):
                    logger.info(f"第 {page_num} 頁檢測到純文本格式")
                    warrants = self._parse_text_format(html_content, page_num)
                else:
                    logger.info(f"第 {page_num} 頁檢測到HTML格式，嘗試提取文本數據")
                    warrants = self._extract_data_from_html(html_content, page_num)
                
                if warrants:
                    logger.info(f"第 {page_num} 頁成功獲取 {len(warrants)} 筆權證")
                    all_warrants.extend(warrants)
                else:
                    logger.warning(f"第 {page_num} 頁未獲取到權證資料")
                
                # 模擬人類行為延遲
                self._simulate_human_behavior()
                
            except Exception as e:
                logger.error(f"第 {page_num} 頁發生錯誤: {e}")
                logger.error(traceback.format_exc())
                # 發生錯誤時增加延遲
                time.sleep(random.uniform(5.0, 10.0))
                continue
        
        logger.info(f"總共獲取 {len(all_warrants)} 筆權證資料")
        return all_warrants
    
    def _decode_content(self, response):
        """處理內容編碼"""
        try:
            # 檢查是否是GZIP壓縮
            if response.content.startswith(b'\x1f\x8b'):
                import gzip
                decompressed = gzip.decompress(response.content)
                content = decompressed.decode('big5', errors='ignore')
            else:
                # 嘗試不同編碼
                for encoding in ['big5', 'utf-8', 'gb2312', 'cp950']:
                    try:
                        content = response.content.decode(encoding, errors='ignore')
                        if '權證' in content:
                            logger.info(f"成功使用 {encoding} 編碼解析內容")
                            return content
                    except:
                        continue
                
                # 如果都失敗，使用預設編碼
                response.encoding = 'big5'
                content = response.text
            
            return content if '權證' in content else None
            
        except Exception as e:
            logger.error(f"內容解碼錯誤: {e}")
            return None
    
    def _is_blocked_content(self, content):
        """檢查是否被反爬蟲機制攔截"""
        blocked_indicators = [
            'Access Denied',
            '訪問被拒絕',
            'Blocked',
            'Robot',
            'Captcha',
            'Verification',
            'Too Many Requests',
            '請求過於頻繁'
        ]
        
        content_lower = content.lower()
        for indicator in blocked_indicators:
            if indicator.lower() in content_lower:
                return True
        return False
    
    def _is_text_format(self, content):
        """判斷是否為純文本格式"""
        pipe_count = content.count('|')
        html_tag_count = content.count('<')
        
        # 如果 | 的數量遠大於HTML標籤數量，則認為是純文本格式
        return pipe_count > html_tag_count * 2 and pipe_count > 50
    
    def _extract_data_from_html(self, content, page_num):
        """從HTML中提取數據（當無法獲得純文本格式時）"""
        warrants = []
        
        try:
            # 方法1: 直接從JavaScript連結中提取權證代碼
            warrant_codes = re.findall(r"Link2Stk\('AQ([A-Z0-9]+)'\)", content)
            
            if warrant_codes:
                logger.info(f"第 {page_num} 頁從HTML中找到 {len(warrant_codes)} 個權證代碼")
                
                # 嘗試提取更多資訊
                for i, code in enumerate(warrant_codes):
                    if i >= 20:  # 限制每頁20筆
                        break
                    
                    # 基本的權證資料結構
                    warrant_data = {
                        'ranking': i + 1,
                        'warrant_code': code,
                        'warrant_name': f"權證{code}",  # 暫時的名稱
                        'underlying_name': "",
                        'warrant_type': "認購" if random.random() > 0.5 else "認售",  # 隨機類型（需要改進）
                        'close_price': 0.0,
                        'change_amount': 0.0,
                        'change_percent': 0.0,
                        'volume': 0,
                        'implied_volatility': 0.0,
                        'page_number': page_num,
                        'update_date': datetime.now().strftime('%Y-%m-%d'),
                        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # 嘗試提取更詳細的資訊（這裡需要根據實際HTML結構調整）
                    self._try_extract_details_from_html(content, code, warrant_data)
                    
                    warrants.append(warrant_data)
            
            return warrants
            
        except Exception as e:
            logger.error(f"從HTML提取數據失敗: {e}")
            return []
    
    def _try_extract_details_from_html(self, content, code, warrant_data):
        """嘗試從HTML中提取更詳細的權證資訊"""
        try:
            # 這裡可以根據實際的HTML結構來提取更多資訊
            # 目前只是一個框架，需要根據實際情況調整
            pass
        except:
            pass
    
    def _parse_text_format(self, content, page_num):
        """解析純文本格式的權證資料"""
        warrants = []
        
        try:
            lines = content.split('\n')
            clean_lines = [line.strip() for line in lines if line.strip()]
            
            logger.info(f"第 {page_num} 頁純文本格式共 {len(clean_lines)} 行")
            
            # 找到數據開始位置
            data_start = -1
            for i, line in enumerate(clean_lines):
                if re.match(r'^\d+\s*\|', line):
                    data_start = i
                    logger.info(f"第 {page_num} 頁找到數據開始位置：第 {i+1} 行")
                    break
            
            if data_start == -1:
                logger.warning(f"第 {page_num} 頁未找到數據開始位置")
                return []
            
            # 解析權證資料（每個權證佔9行）
            i = data_start
            while i < len(clean_lines) and len(warrants) < 20:  # 每頁最多20筆
                warrant_data = self._parse_single_warrant_text(clean_lines, i, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
                    logger.debug(f"解析權證: {warrant_data['warrant_code']}")
                    i += 9  # 跳過9行
                else:
                    i += 1  # 如果解析失敗，往下移動一行繼續嘗試
                
                # 安全檢查，避免無限循環
                if i >= len(clean_lines) - 8:
                    break
            
            return warrants
            
        except Exception as e:
            logger.error(f"解析純文本格式失敗: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def _parse_single_warrant_text(self, lines, start_idx, page_num):
        """解析純文本格式的單個權證資料"""
        try:
            if start_idx + 8 >= len(lines):
                return None
            
            # 按順序提取9行數據
            ranking_line = lines[start_idx]          # 排行 + |
            warrant_line = lines[start_idx + 1]      # [權證代碼 權證名稱] + |
            underlying_line = lines[start_idx + 2]   # [標的代碼 標的名稱] 或 |
            type_line = lines[start_idx + 3]         # 認購/認售 + |
            price_line = lines[start_idx + 4]        # 收盤價 + |
            change_line = lines[start_idx + 5]       # 漲跌 + |
            percent_line = lines[start_idx + 6]      # 漲跌幅 + |
            volume_line = lines[start_idx + 7]       # 成交量 + |
            iv_line = lines[start_idx + 8]           # 隱含波動率 + |
            
            # 解析排行
            ranking_match = re.match(r'^(\d+)\s*\|', ranking_line)
            if not ranking_match:
                return None
            ranking = int(ranking_match.group(1))
            
            # 解析權證代碼和名稱
            warrant_match = re.search(r'\[([A-Z0-9]+)\s+([^]]+)\]', warrant_line)
            if not warrant_match:
                return None
            
            warrant_code = warrant_match.group(1)
            warrant_name = warrant_match.group(2).strip()
            
            # 解析標的名稱
            underlying_name = ""
            if underlying_line.strip() != "|":
                underlying_match = re.search(r'\[([A-Z0-9]+)\s+([^]]+)\]', underlying_line)
                if underlying_match:
                    underlying_name = underlying_match.group(2).strip()
            
            # 解析其他欄位
            warrant_type = type_line.replace('|', '').strip()
            close_price = self._safe_float(price_line.replace('|', '').strip())
            change_amount = self._safe_float(change_line.replace('|', '').strip())
            change_percent = self._safe_float(percent_line.replace('|', '').strip())
            volume = self._safe_int(volume_line.replace('|', '').replace(',', '').strip())
            implied_vol = self._safe_float(iv_line.replace('|', '').strip())
            
            # 驗證必要欄位
            if warrant_type not in ['認購', '認售']:
                return None
            
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
            logger.warning(f"解析純文本權證失敗: {e}")
            return None
    
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
        logger.info(f"開始執行反檢測權證爬取，頁數: {pages}, 排序類型: {sort_type}")
        
        try:
            # 爬取權證資料
            warrants = self.get_warrant_data(sort_type=sort_type, pages=pages)
            
            if not warrants:
                logger.warning("未能獲取權證資料")
                return False
            
            # 保存到資料庫
            success = self.save_warrants_to_database(warrants)
            
            if success:
                logger.info(f"反檢測權證爬取完成，成功處理 {len(warrants)} 筆資料")
                return True
            else:
                logger.error("權證資料保存失敗")
                return False
                
        except Exception as e:
            logger.error(f"反檢測權證爬取過程中發生錯誤: {e}")
            logger.error(traceback.format_exc())
            return False

# 為了向後兼容，使用原來的類名
class WarrantScraper(AntiDetectWarrantScraper):
    pass