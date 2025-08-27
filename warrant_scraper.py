# warrant_scraper_debug.py - 調試版權證爬蟲
import requests
import re
import time
from datetime import datetime
import logging
import traceback
from typing import List, Dict, Any, Optional
from database_config import db_config

# 設置日誌
logger = logging.getLogger(__name__)

class WarrantScraperDebug:
    """調試版權證爬蟲 - 增加詳細日誌"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        })
        
        # 確認數據庫可用性
        if not db_config:
            raise Exception("數據庫配置不可用，無法初始化權證爬蟲")
        
        logger.info(f"權證爬蟲初始化完成 - 數據庫類型: {db_config.db_type}")
    
    def get_warrant_data(self, sort_type=3, pages=5):
        """爬取權證資料"""
        
        if isinstance(pages, int):
            pages = list(range(1, pages + 1))
        
        all_warrants = []
        
        logger.info(f"開始爬取權證資料，頁數: {pages}，排序類型: {sort_type}")
        
        for page_num in pages:
            logger.info(f"正在爬取第 {page_num} 頁...")
            
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
                
                # 調試：輸出內容的前1000個字符
                logger.info(f"第 {page_num} 頁內容前1000字符:")
                logger.info(f"{html_content[:1000]}...")
                logger.info("=" * 50)
                
                # 解析這一頁的權證資料
                warrants = self._parse_warrant_data_from_text_debug(html_content, page_num)
                
                if warrants:
                    logger.info(f"第 {page_num} 頁成功獲取 {len(warrants)} 筆權證")
                    all_warrants.extend(warrants)
                else:
                    logger.warning(f"第 {page_num} 頁未獲取到權證資料")
                
                # 避免請求太頻繁
                if page_num < max(pages):
                    time.sleep(2)
                
            except Exception as e:
                logger.error(f"第 {page_num} 頁發生錯誤: {e}")
                logger.error(traceback.format_exc())
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
                        if '權證' in content and ('成交量' in content or '排行' in content):
                            logger.info(f"成功使用 {encoding} 編碼解析內容")
                            return content
                    except:
                        continue
                
                # 如果都失敗，使用預設編碼
                response.encoding = 'big5'
                content = response.text
            
            # 驗證內容
            if '權證' in content and ('成交量' in content or '排行' in content):
                return content
            else:
                logger.warning("網頁內容不包含預期的權證資料")
                return None
            
        except Exception as e:
            logger.error(f"內容解碼錯誤: {e}")
            return None
    
    def _parse_warrant_data_from_text_debug(self, content, page_num):
        """解析文本格式的權證資料 - 調試版"""
        warrants = []
        
        try:
            # 將內容按行分割
            lines = content.split('\n')
            logger.info(f"第 {page_num} 頁總共有 {len(lines)} 行")
            
            # 清理並找到數據開始位置
            clean_lines = []
            for i, line in enumerate(lines):
                clean_line = line.strip()
                if clean_line:
                    clean_lines.append(clean_line)
                    # 調試：顯示前50行的內容
                    if i < 50:
                        logger.debug(f"第{i+1}行: '{clean_line}'")
            
            logger.info(f"第 {page_num} 頁清理後有 {len(clean_lines)} 行非空內容")
            
            # 找到數據區域 - 尋找第一個數字行（排行）
            data_start = -1
            for i, line in enumerate(clean_lines):
                # 調試：檢查每一行是否匹配數字模式
                if re.match(r'^\d+\s*\|', line):
                    logger.info(f"第 {page_num} 頁找到數據開始位置：第 {i+1} 行，內容: '{line}'")
                    data_start = i
                    break
                elif line.strip() and line.strip().isdigit():
                    # 有些情況下，數字可能在單獨一行，下一行是 |
                    if i + 1 < len(clean_lines) and clean_lines[i + 1].strip() == '|':
                        logger.info(f"第 {page_num} 頁找到分離式數據開始位置：第 {i+1} 行，數字: '{line}'")
                        data_start = i
                        break
            
            if data_start == -1:
                logger.warning(f"第 {page_num} 頁未找到數據開始位置")
                # 調試：顯示所有行看看格式
                logger.info("顯示前20行內容進行調試:")
                for i, line in enumerate(clean_lines[:20]):
                    logger.info(f"  {i+1:2d}: '{line}'")
                return []
            
            # 改進的解析邏輯
            warrants = self._parse_warrants_improved(clean_lines, data_start, page_num)
            
            logger.info(f"第 {page_num} 頁解析完成，獲取 {len(warrants)} 筆權證資料")
            return warrants
            
        except Exception as e:
            logger.error(f"解析第 {page_num} 頁權證資料失敗: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def _parse_warrants_improved(self, lines, start_idx, page_num):
        """改進的權證解析邏輯"""
        warrants = []
        i = start_idx
        
        while i < len(lines):
            # 檢查當前行是否是排行數字
            current_line = lines[i].strip()
            
            # 模式1: "1 |" 格式
            ranking_match = re.match(r'^(\d+)\s*\|', current_line)
            if ranking_match:
                ranking = int(ranking_match.group(1))
                logger.debug(f"找到排行 {ranking}")
                
                # 檢查是否有足夠的後續行
                if i + 8 >= len(lines):
                    logger.warning(f"排行 {ranking} 後續行數不足，跳過")
                    break
                
                # 提取權證資料
                warrant_data = self._extract_warrant_data_improved(lines, i, ranking, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
                    logger.info(f"成功解析權證：{warrant_data['warrant_code']} - {warrant_data['warrant_name']}")
                
                i += 9  # 跳過9行
                
            # 模式2: 單獨數字行
            elif current_line.isdigit():
                ranking = int(current_line)
                logger.debug(f"找到分離式排行 {ranking}")
                
                if i + 9 >= len(lines):
                    logger.warning(f"分離式排行 {ranking} 後續行數不足，跳過")
                    break
                
                # 提取權證資料（從下一行開始）
                warrant_data = self._extract_warrant_data_separated(lines, i, ranking, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
                    logger.info(f"成功解析分離式權證：{warrant_data['warrant_code']} - {warrant_data['warrant_name']}")
                
                i += 10  # 跳過10行（數字+9個資料行）
            else:
                i += 1
        
        return warrants
    
    def _extract_warrant_data_improved(self, lines, start_idx, ranking, page_num):
        """改進的權證資料提取（"1 |" 格式）"""
        try:
            # 從 start_idx+1 開始提取8個欄位
            if start_idx + 8 >= len(lines):
                return None
            
            warrant_line = lines[start_idx + 1].strip()  # 權證商品
            underlying_line = lines[start_idx + 2].strip()  # 標的名稱
            type_line = lines[start_idx + 3].strip()     # 權證類型
            price_line = lines[start_idx + 4].strip()    # 收盤價
            change_line = lines[start_idx + 5].strip()   # 漲跌
            percent_line = lines[start_idx + 6].strip()  # 漲跌幅
            volume_line = lines[start_idx + 7].strip()   # 成交量
            iv_line = lines[start_idx + 8].strip()       # 隱含波動率
            
            return self._extract_warrant_fields(warrant_line, underlying_line, type_line, 
                                              price_line, change_line, percent_line, 
                                              volume_line, iv_line, ranking, page_num)
        
        except Exception as e:
            logger.warning(f"提取改進格式權證資料失敗: {e}")
            return None
    
    def _extract_warrant_data_separated(self, lines, start_idx, ranking, page_num):
        """提取分離式權證資料（數字單獨一行）"""
        try:
            # 從 start_idx+2 開始提取8個欄位（跳過數字行和分隔符行）
            if start_idx + 9 >= len(lines):
                return None
            
            warrant_line = lines[start_idx + 2].strip()  # 權證商品
            underlying_line = lines[start_idx + 3].strip()  # 標的名稱
            type_line = lines[start_idx + 4].strip()     # 權證類型
            price_line = lines[start_idx + 5].strip()    # 收盤價
            change_line = lines[start_idx + 6].strip()   # 漲跌
            percent_line = lines[start_idx + 7].strip()  # 漲跌幅
            volume_line = lines[start_idx + 8].strip()   # 成交量
            iv_line = lines[start_idx + 9].strip()       # 隱含波動率
            
            return self._extract_warrant_fields(warrant_line, underlying_line, type_line, 
                                              price_line, change_line, percent_line, 
                                              volume_line, iv_line, ranking, page_num)
        
        except Exception as e:
            logger.warning(f"提取分離式權證資料失敗: {e}")
            return None
    
    def _extract_warrant_fields(self, warrant_line, underlying_line, type_line, 
                               price_line, change_line, percent_line, 
                               volume_line, iv_line, ranking, page_num):
        """提取權證欄位資料"""
        try:
            # 解析權證商品
            warrant_match = re.search(r'\[([A-Z0-9]+)\s+([^]]+)\]', warrant_line)
            if not warrant_match:
                logger.warning(f"無法解析權證資料: {warrant_line}")
                return None
            
            warrant_code = warrant_match.group(1)
            warrant_name = warrant_match.group(2).strip()
            
            # 解析標的名稱
            underlying_name = ""
            if underlying_line != "|" and underlying_line:
                underlying_match = re.search(r'\[([A-Z0-9]+)\s+([^]]+)\]', underlying_line)
                if underlying_match:
                    underlying_name = underlying_match.group(2).strip()
                elif underlying_line != "|":
                    # 有時候標的名稱可能不在方括號中
                    underlying_name = underlying_line.replace('|', '').strip()
            
            # 解析權證類型
            warrant_type = type_line.replace('|', '').strip()
            if warrant_type not in ['認購', '認售']:
                logger.warning(f"無效的權證類型: {warrant_type}")
                return None
            
            # 解析數值欄位
            close_price = self._safe_float(price_line.replace('|', '').strip())
            change_amount = self._safe_float(change_line.replace('|', '').strip())
            change_percent = self._safe_float(percent_line.replace('|', '').strip())
            volume = self._safe_int(volume_line.replace('|', '').replace(',', '').strip())
            implied_vol = self._safe_float(iv_line.replace('|', '').strip())
            
            # 構建權證資料
            warrant_data = {
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
            
            return warrant_data
            
        except Exception as e:
            logger.warning(f"提取權證欄位失敗: {e}")
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
            return int(float(cleaned))  # 先轉float再轉int，處理小數點
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
        logger.info(f"開始執行權證爬取，頁數: {pages}, 排序類型: {sort_type}")
        
        try:
            # 爬取權證資料
            warrants = self.get_warrant_data(sort_type=sort_type, pages=pages)
            
            if not warrants:
                logger.warning("未能獲取權證資料")
                return False
            
            # 保存到資料庫
            success = self.save_warrants_to_database(warrants)
            
            if success:
                logger.info(f"權證爬取完成，成功處理 {len(warrants)} 筆資料")
                return True
            else:
                logger.error("權證資料保存失敗")
                return False
                
        except Exception as e:
            logger.error(f"權證爬取過程中發生錯誤: {e}")
            logger.error(traceback.format_exc())
            return False

# 為了向後兼容，使用原來的類名
class WarrantScraper(WarrantScraperDebug):
    pass