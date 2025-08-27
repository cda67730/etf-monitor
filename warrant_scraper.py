# warrant_scraper.py - 權證爬蟲整合模組
import requests
import re
import time
from datetime import datetime, timedelta
import logging
import traceback
from typing import List, Dict, Any, Optional
from database_config import db_config

# 設置日誌
logger = logging.getLogger(__name__)

class WarrantScraper:
    """富邦權證爬蟲整合到現有系統"""
    
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
    
    def get_warrant_data(self, sort_type=2, pages=None):
        """
        獲取權證資料
        
        Args:
            sort_type (int): 排序類型 (2=跌幅排行)
            pages (int or list): 頁數
            
        Returns:
            list: 權證資料列表
        """
        
        if pages is None:
            pages = [1]
        elif isinstance(pages, int):
            pages = list(range(1, pages + 1))
        
        all_warrants = []
        
        logger.info(f"開始獲取權證資料，頁數: {pages}")
        
        for page_num in pages:
            logger.info(f"正在獲取第 {page_num} 頁...")
            
            url = f"https://ebroker-dj.fbs.com.tw/WRT/zx/zxd/zxd.djhtm?A={sort_type}&B=&Page={page_num}"
            
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
                
                # 解析這一頁的權證資料
                warrants = self._parse_html_table(html_content, page_num)
                
                if warrants:
                    logger.info(f"第 {page_num} 頁成功獲取 {len(warrants)} 筆權證")
                    all_warrants.extend(warrants)
                else:
                    logger.warning(f"第 {page_num} 頁未獲取到權證資料")
                
                # 避免請求太頻繁
                if page_num < max(pages):
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"第 {page_num} 頁發生錯誤: {e}")
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
                response.encoding = 'big5'
                content = response.text
            
            # 驗證內容
            if '權證' in content and len(content) > 5000:
                return content
            else:
                # 嘗試其他編碼
                for encoding in ['utf-8', 'gb2312', 'cp950']:
                    try:
                        test_content = response.content.decode(encoding, errors='ignore')
                        if '權證' in test_content and len(test_content) > 5000:
                            return test_content
                    except:
                        continue
            
            return None
            
        except Exception as e:
            logger.error(f"內容解碼錯誤: {e}")
            return None
    
    def _parse_html_table(self, html_content, page_num):
        """解析HTML表格中的權證資料"""
        
        warrants = []
        
        # 尋找表格行模式
        tr_pattern = r'<tr>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*><a[^>]*>(\w+)&nbsp;([^<]+)</a></td>\s*<td[^>]*>.*?</td>\s*<td[^>]*>(認購|認售)</td>\s*<td[^>]*>([\d.]+)</td>\s*<td[^>]*>([-\d.]+)</td>\s*<td[^>]*>([-\d.]+)</td>\s*<td[^>]*>([\d,]+)</td>\s*<td[^>]*>([\d.]+)</td>\s*</tr>'
        
        matches = re.finditer(tr_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            try:
                ranking = int(match.group(1))
                warrant_code = match.group(2)
                warrant_name = match.group(3)
                warrant_type = match.group(4)
                close_price = float(match.group(5))
                change = float(match.group(6))
                change_pct = float(match.group(7))
                volume_str = match.group(8).replace(',', '')
                volume = int(volume_str) if volume_str.isdigit() else 0
                implied_vol = float(match.group(9))
                
                # 提取標的股票名稱
                underlying = self._extract_underlying(html_content, warrant_code)
                
                warrant_info = {
                    'ranking': ranking,
                    'warrant_code': warrant_code,
                    'warrant_name': warrant_name,
                    'underlying_name': underlying,
                    'warrant_type': warrant_type,
                    'close_price': close_price,
                    'change_amount': change,
                    'change_percent': abs(change_pct),
                    'volume': volume,
                    'implied_volatility': implied_vol,
                    'page_number': page_num,
                    'update_date': datetime.now().strftime('%Y-%m-%d'),
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                warrants.append(warrant_info)
                
            except Exception as e:
                logger.warning(f"解析權證資料失敗: {e}")
                continue
        
        # 如果正則表達式方法失敗，嘗試逐行解析
        if not warrants:
            warrants = self._parse_line_by_line(html_content, page_num)
        
        return warrants
    
    def _parse_line_by_line(self, html_content, page_num):
        """逐行解析方法（備用）"""
        
        warrants = []
        lines = html_content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # 尋找包含權證連結的行
            warrant_link_pattern = r'<a href="javascript:Link2Stk\(\'AQ(\w+)\'\);">(\w+)&nbsp;([^<]+)</a>'
            warrant_match = re.search(warrant_link_pattern, line)
            
            if warrant_match:
                try:
                    warrant_code = warrant_match.group(2)
                    warrant_name = warrant_match.group(3)
                    
                    # 在接下來的幾行中尋找相關資料
                    warrant_data = self._extract_warrant_data_from_nearby_lines(lines, i, warrant_code, warrant_name, page_num)
                    
                    if warrant_data:
                        warrants.append(warrant_data)
                        
                except Exception as e:
                    logger.warning(f"逐行解析失敗: {e}")
            
            i += 1
        
        return warrants
    
    def _extract_warrant_data_from_nearby_lines(self, lines, start_idx, warrant_code, warrant_name, page_num):
        """從附近的行中提取權證資料"""
        
        # 收集附近的行
        nearby_lines = []
        for i in range(max(0, start_idx-2), min(len(lines), start_idx+15)):
            nearby_lines.append(lines[i])
        
        combined_text = ' '.join(nearby_lines)
        
        try:
            # 提取排行
            ranking_match = re.search(r'<td[^>]*>(\d+)</td>', combined_text)
            ranking = int(ranking_match.group(1)) if ranking_match else 0
            
            # 提取權證類型
            warrant_type = "認購" if "認購" in combined_text else ("認售" if "認售" in combined_text else "未知")
            
            # 提取數值資料
            td_values = re.findall(r'<td[^>]*>([-\d.,]+)</td>', combined_text)
            
            # 過濾和轉換數值
            numbers = []
            for val in td_values:
                try:
                    clean_val = val.replace(',', '')
                    if '.' in clean_val or clean_val.isdigit() or (clean_val.startswith('-') and clean_val[1:].replace('.', '').isdigit()):
                        numbers.append(float(clean_val))
                except:
                    continue
            
            # 確保有足夠的數值
            while len(numbers) < 5:
                numbers.append(0.0)
            
            # 提取標的股票
            underlying = self._extract_underlying_from_text(combined_text)
            
            return {
                'ranking': ranking,
                'warrant_code': warrant_code,
                'warrant_name': warrant_name,
                'underlying_name': underlying,
                'warrant_type': warrant_type,
                'close_price': numbers[0] if len(numbers) > 0 else 0,
                'change_amount': numbers[1] if len(numbers) > 1 else 0,
                'change_percent': abs(numbers[2]) if len(numbers) > 2 else 0,
                'volume': int(numbers[3]) if len(numbers) > 3 and numbers[3] < 1000000 else 0,
                'implied_volatility': numbers[4] if len(numbers) > 4 else 0,
                'page_number': page_num,
                'update_date': datetime.now().strftime('%Y-%m-%d'),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.warning(f"提取權證資料失敗: {e}")
            return None
    
    def _extract_underlying(self, html_content, warrant_code):
        """提取標的股票名稱"""
        
        # 方法1: 尋找JavaScript GenLink2stk調用
        genlink_pattern = rf"GenLink2stk\('AS\d+','([^']+)'\)"
        matches = re.findall(genlink_pattern, html_content)
        
        if matches:
            return matches[0] if matches else ""
        
        # 方法2: 尋找直接的股票連結
        stock_link_pattern = r'<a href="javascript:Link2Stk\(\'AP\d+\'\);">(\d+)&nbsp;([^<]+)</a>'
        stock_matches = re.findall(stock_link_pattern, html_content)
        
        if stock_matches:
            return stock_matches[0][1]
        
        return ""
    
    def _extract_underlying_from_text(self, text):
        """從文本中提取標的名稱"""
        
        # 尋找GenLink2stk
        genlink_match = re.search(r"GenLink2stk\('AS\d+','([^']+)'\)", text)
        if genlink_match:
            return genlink_match.group(1)
        
        # 尋找股票連結
        stock_link_match = re.search(r'<a href="javascript:Link2Stk\(\'AP\d+\'\);">(\d+)&nbsp;([^<]+)</a>', text)
        if stock_link_match:
            return stock_link_match.group(2)
        
        return ""
    
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
    
    def scrape_warrants(self, pages: int = 3, sort_type: int = 2):
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
    
    def get_warrant_ranking(self, date: str = None, warrant_type: str = None, limit: int = None):
        """獲取權證排行資料"""
        try:
            if db_config.db_type == "postgresql":
                ph = "%s"
            else:
                ph = "?"
            
            where_conditions = []
            params = []
            
            if date:
                where_conditions.append(f"update_date = {ph}")
                params.append(date)
            else:
                where_conditions.append(f"update_date = (SELECT MAX(update_date) FROM warrant_data)")
            
            if warrant_type:
                where_conditions.append(f"warrant_type = {ph}")
                params.append(warrant_type)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f'''
                SELECT * FROM warrant_data 
                {where_clause}
                ORDER BY ranking ASC
                {limit_clause}
            '''
            
            results = db_config.execute_query(query, tuple(params), fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取權證排行資料錯誤: {e}")
            return []
    
    def get_underlying_summary(self, date: str = None):
        """獲取標的統計資料"""
        try:
            if db_config.db_type == "postgresql":
                ph = "%s"
            else:
                ph = "?"
            
            where_condition = f"update_date = {ph}" if date else f"update_date = (SELECT MAX(update_date) FROM warrant_underlying_summary)"
            params = (date,) if date else ()
            
            query = f'''
                SELECT * FROM warrant_underlying_summary 
                WHERE {where_condition}
                ORDER BY warrant_count DESC, total_volume DESC
            '''
            
            results = db_config.execute_query(query, params, fetch="all")
            return results if results else []
            
        except Exception as e:
            logger.error(f"獲取標的統計資料錯誤: {e}")
            return []