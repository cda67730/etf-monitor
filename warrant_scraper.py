# warrant_scraper_diagnose.py - 診斷版權證爬蟲
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

class WarrantScraperDiagnose:
    """診斷版權證爬蟲 - 查看網站返回的實際內容"""
    
    def __init__(self):
        self.session = requests.Session()
        
        # 使用最基本的瀏覽器標頭
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
        
        logger.info(f"診斷版權證爬蟲初始化完成 - 數據庫類型: {db_config.db_type}")
    
    def diagnose_website_response(self, url):
        """診斷網站響應"""
        logger.info(f"診斷URL: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            
            # 記錄響應詳情
            logger.info(f"響應狀態碼: {response.status_code}")
            logger.info(f"響應標頭: {dict(response.headers)}")
            logger.info(f"內容長度: {len(response.content)}")
            logger.info(f"內容類型: {response.headers.get('content-type', 'unknown')}")
            
            if len(response.content) > 0:
                # 嘗試不同的解碼方式
                decoded_content = None
                
                # 方法1: 原始bytes檢查
                logger.info(f"原始bytes前200字符: {response.content[:200]}")
                
                # 方法2: 嘗試各種編碼
                encodings_to_try = ['big5', 'utf-8', 'gb2312', 'cp950', 'latin1']
                for encoding in encodings_to_try:
                    try:
                        test_content = response.content.decode(encoding, errors='ignore')
                        logger.info(f"使用 {encoding} 編碼長度: {len(test_content)}")
                        if '權證' in test_content or 'warrant' in test_content.lower():
                            logger.info(f"✅ {encoding} 編碼包含權證相關內容")
                            decoded_content = test_content
                            break
                        else:
                            logger.info(f"❌ {encoding} 編碼不包含權證內容")
                            # 顯示前500字符
                            logger.info(f"{encoding} 內容預覽: {test_content[:500]}")
                    except Exception as e:
                        logger.warning(f"❌ {encoding} 編碼失敗: {e}")
                
                # 方法3: 檢查是否被重定向或阻擋
                if not decoded_content:
                    logger.warning("所有編碼方式都無法找到權證內容")
                    logger.info("可能的原因:")
                    logger.info("1. 網站返回錯誤頁面或空白頁面")
                    logger.info("2. 需要JavaScript渲染")
                    logger.info("3. 被反爬蟲機制完全阻擋")
                    logger.info("4. 需要登入或驗證")
                
                return decoded_content
            else:
                logger.error("響應內容為空")
                return None
                
        except Exception as e:
            logger.error(f"診斷請求失敗: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def test_multiple_methods(self):
        """測試多種請求方法"""
        base_url = "https://ebroker-dj.fbs.com.tw/WRT/zx/zxd/zxd.djhtm?A=3&B=&Page=1"
        
        logger.info("=== 開始多方法測試 ===")
        
        # 方法1: 基本請求
        logger.info("1. 測試基本請求")
        content1 = self.diagnose_website_response(base_url)
        
        # 方法2: 先訪問首頁
        logger.info("2. 測試先訪問首頁")
        try:
            homepage_response = self.session.get("https://ebroker-dj.fbs.com.tw/")
            logger.info(f"首頁狀態碼: {homepage_response.status_code}")
            time.sleep(2)
            
            self.session.headers.update({'Referer': 'https://ebroker-dj.fbs.com.tw/'})
            content2 = self.diagnose_website_response(base_url)
        except Exception as e:
            logger.error(f"首頁訪問失敗: {e}")
            content2 = None
        
        # 方法3: 更改User-Agent
        logger.info("3. 測試Firefox User-Agent")
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
        })
        content3 = self.diagnose_website_response(base_url)
        
        # 方法4: 移除gzip編碼
        logger.info("4. 測試不使用gzip壓縮")
        self.session.headers.update({'Accept-Encoding': 'identity'})
        content4 = self.diagnose_website_response(base_url)
        
        # 總結測試結果
        logger.info("=== 測試結果總結 ===")
        methods = ["基本請求", "先訪問首頁", "Firefox UA", "不使用gzip"]
        contents = [content1, content2, content3, content4]
        
        for i, (method, content) in enumerate(zip(methods, contents)):
            if content and '權證' in content:
                logger.info(f"✅ {method}: 成功獲取權證內容")
                return content
            else:
                logger.info(f"❌ {method}: 失敗")
        
        logger.warning("所有方法都無法獲取權證內容")
        return None
    
    def get_warrant_data(self, sort_type=3, pages=1):
        """診斷版爬取權證資料（只測試1頁）"""
        
        logger.info(f"診斷版權證爬取開始，測試第1頁，排序類型: {sort_type}")
        
        # 執行多方法測試
        content = self.test_multiple_methods()
        
        if content:
            logger.info("成功獲取到內容，嘗試解析...")
            
            # 檢查內容格式
            if self._is_text_format(content):
                logger.info("檢測到純文本格式")
                warrants = self._parse_text_format(content, 1)
            else:
                logger.info("檢測到HTML格式")
                warrants = self._extract_basic_info_from_html(content)
            
            if warrants:
                logger.info(f"成功解析出 {len(warrants)} 筆權證資料")
                for warrant in warrants[:3]:  # 顯示前3筆
                    logger.info(f"權證: {warrant.get('warrant_code')} - {warrant.get('warrant_name')}")
                return warrants
            else:
                logger.warning("無法解析權證資料")
                return []
        else:
            logger.error("完全無法獲取網站內容")
            return []
    
    def _is_text_format(self, content):
        """判斷是否為純文本格式"""
        pipe_count = content.count('|')
        html_tag_count = content.count('<')
        logger.info(f"內容分析: | 符號數量={pipe_count}, HTML標籤數量={html_tag_count}")
        return pipe_count > html_tag_count * 2 and pipe_count > 50
    
    def _parse_text_format(self, content, page_num):
        """解析純文本格式"""
        warrants = []
        try:
            lines = content.split('\n')
            clean_lines = [line.strip() for line in lines if line.strip()]
            
            logger.info(f"純文本格式共 {len(clean_lines)} 行")
            
            # 顯示前10行內容
            logger.info("前10行內容:")
            for i, line in enumerate(clean_lines[:10]):
                logger.info(f"  {i+1:2d}: '{line}'")
            
            # 尋找數據開始位置
            data_start = -1
            for i, line in enumerate(clean_lines):
                if re.match(r'^\d+\s*\|', line):
                    data_start = i
                    logger.info(f"找到數據開始位置：第 {i+1} 行")
                    break
            
            if data_start >= 0:
                # 嘗試解析第一個權證
                warrant_data = self._parse_single_warrant_text(clean_lines, data_start, page_num)
                if warrant_data:
                    warrants.append(warrant_data)
                    logger.info(f"成功解析第一個權證: {warrant_data['warrant_code']}")
            
        except Exception as e:
            logger.error(f"解析純文本失敗: {e}")
            logger.error(traceback.format_exc())
        
        return warrants
    
    def _extract_basic_info_from_html(self, content):
        """從HTML中提取基本資訊"""
        warrants = []
        try:
            # 尋找JavaScript權證連結
            warrant_codes = re.findall(r"Link2Stk\('AQ([A-Z0-9]+)'\)", content)
            logger.info(f"從HTML中找到 {len(warrant_codes)} 個權證代碼: {warrant_codes[:5]}")
            
            if warrant_codes:
                # 創建基本權證資料
                for i, code in enumerate(warrant_codes[:5]):  # 只處理前5個
                    warrant_data = {
                        'ranking': i + 1,
                        'warrant_code': code,
                        'warrant_name': f"權證{code}",
                        'underlying_name': "",
                        'warrant_type': "認購",
                        'close_price': 0.0,
                        'change_amount': 0.0,
                        'change_percent': 0.0,
                        'volume': 0,
                        'implied_volatility': 0.0,
                        'page_number': 1,
                        'update_date': datetime.now().strftime('%Y-%m-%d'),
                        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    warrants.append(warrant_data)
        
        except Exception as e:
            logger.error(f"從HTML提取資訊失敗: {e}")
        
        return warrants
    
    def _parse_single_warrant_text(self, lines, start_idx, page_num):
        """解析純文本格式的單個權證資料"""
        try:
            if start_idx + 8 >= len(lines):
                return None
            
            # 提取9行數據
            warrant_lines = []
            for i in range(9):
                if start_idx + i < len(lines):
                    warrant_lines.append(lines[start_idx + i])
                else:
                    warrant_lines.append("")
            
            logger.info(f"提取的權證資料行:")
            for i, line in enumerate(warrant_lines):
                logger.info(f"  {i+1}: '{line}'")
            
            # 解析排行
            ranking_match = re.match(r'^(\d+)\s*\|', warrant_lines[0])
            if not ranking_match:
                return None
            ranking = int(ranking_match.group(1))
            
            # 解析權證代碼和名稱
            warrant_match = re.search(r'\[([A-Z0-9]+)\s+([^]]+)\]', warrant_lines[1])
            if not warrant_match:
                logger.warning(f"無法解析權證行: {warrant_lines[1]}")
                return None
            
            warrant_code = warrant_match.group(1)
            warrant_name = warrant_match.group(2).strip()
            
            # 解析其他欄位
            warrant_type = warrant_lines[3].replace('|', '').strip()
            
            return {
                'ranking': ranking,
                'warrant_code': warrant_code,
                'warrant_name': warrant_name,
                'underlying_name': "",
                'warrant_type': warrant_type,
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
            logger.warning(f"解析單個權證失敗: {e}")
            return None
    
    def save_warrants_to_database(self, warrants: List[Dict[str, Any]], date: str = None):
        """保存權證到資料庫（簡化版）"""
        if not warrants:
            logger.warning("沒有權證資料需要保存")
            return False
        
        logger.info(f"診斷模式：模擬保存 {len(warrants)} 筆權證資料")
        return True
    
    def scrape_warrants(self, pages: int = 1, sort_type: int = 3):
        """執行診斷版權證爬取"""
        logger.info(f"開始執行診斷版權證爬取")
        
        try:
            # 只測試第一頁
            warrants = self.get_warrant_data(sort_type=sort_type, pages=1)
            
            if warrants:
                logger.info(f"診斷成功：獲取到 {len(warrants)} 筆權證資料")
                # 不實際保存到資料庫，只是測試
                return True
            else:
                logger.warning("診斷失敗：未能獲取權證資料")
                return False
                
        except Exception as e:
            logger.error(f"診斷版權證爬取錯誤: {e}")
            logger.error(traceback.format_exc())
            return False

# 為了向後兼容，使用原來的類名
class WarrantScraper(WarrantScraperDiagnose):
    pass