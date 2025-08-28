# warrant_volume_analyzer.py - 權證標的成交量分析器 (修正版)
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from database_config import db_config

logger = logging.getLogger(__name__)

class WarrantVolumeAnalyzer:
    """權證標的成交量分析器 - 比對當日與前五日平均"""
    
    def __init__(self):
        if not db_config:
            raise Exception("資料庫配置不可用，無法初始化分析器")
        self.db_available = db_config is not None
        logger.info("權證成交量分析器初始化完成")
    
    def get_volume_comparison_analysis(self, target_date: str = None) -> Dict[str, Any]:
        """獲取權證標的成交量比對分析 - 分認購認售"""
        if not self.db_available:
            return {'call_data': [], 'put_data': [], 'analysis_info': {}}
        
        try:
            # 如果沒有指定日期，使用最新日期
            if not target_date:
                target_date = self._get_latest_date()
                if not target_date:
                    logger.warning("沒有找到權證資料")
                    return {'call_data': [], 'put_data': [], 'analysis_info': {}}
            
            logger.info(f"分析目標日期: {target_date}")
            
            # 獲取前五日的日期列表
            previous_dates = self._get_previous_five_dates(target_date)
            if len(previous_dates) < 5:
                logger.warning(f"前五日資料不足，僅有 {len(previous_dates)} 天資料")
            
            # 獲取當日資料（分認購認售）
            current_call_data = self._get_underlying_volume_by_date_and_type(target_date, '認購')
            current_put_data = self._get_underlying_volume_by_date_and_type(target_date, '認售')
            
            # 計算認購權證分析
            call_analysis = self._analyze_volume_comparison(
                current_call_data, previous_dates, '認購', target_date
            )
            
            # 計算認售權證分析
            put_analysis = self._analyze_volume_comparison(
                current_put_data, previous_dates, '認售', target_date
            )
            
            # 統計資訊
            analysis_info = {
                'analysis_date': target_date,
                'previous_dates_count': len(previous_dates),
                'previous_dates': previous_dates,
                'call_underlyings_count': len(call_analysis),
                'put_underlyings_count': len(put_analysis),
                'call_high_change_count': len([x for x in call_analysis if x['change_percent'] >= 70]),
                'put_high_change_count': len([x for x in put_analysis if x['change_percent'] >= 70])
            }
            
            logger.info(f"分析完成 - 認購: {len(call_analysis)} 個標的, 認售: {len(put_analysis)} 個標的")
            
            return {
                'call_data': call_analysis,
                'put_data': put_analysis,
                'analysis_info': analysis_info
            }
            
        except Exception as e:
            logger.error(f"成交量比對分析錯誤: {e}")
            return {'call_data': [], 'put_data': [], 'analysis_info': {}}
    
    def _analyze_volume_comparison(self, current_data: Dict[str, int], 
                                 previous_dates: List[str], 
                                 warrant_type: str, 
                                 target_date: str) -> List[Dict[str, Any]]:
        """分析單一類型權證的成交量比對"""
        analysis_results = []
        
        for underlying_name, current_volume in current_data.items():
            # 計算前五日平均
            five_day_avg = self._calculate_five_day_average_for_type(
                underlying_name, previous_dates, warrant_type
            )
            
            # 計算差異
            volume_diff = current_volume - five_day_avg
            change_percent = 0
            if five_day_avg > 0:
                change_percent = (volume_diff / five_day_avg) * 100
            
            analysis_results.append({
                'underlying_name': underlying_name,
                'current_volume': current_volume,
                'five_day_avg': five_day_avg,
                'volume_diff': volume_diff,
                'change_percent': round(change_percent, 2),
                'is_high_change': abs(change_percent) >= 70,  # 修正：使用絕對值
                'warrant_type': warrant_type,
                'analysis_date': target_date
            })
        
        # 預設按變動量絕對值排序（大到小）
        analysis_results.sort(key=lambda x: abs(x['volume_diff']), reverse=True)
        return analysis_results
    
    def sort_analysis_data(self, data: List[Dict[str, Any]], 
                          sort_by: str = 'volume_diff', 
                          ascending: bool = False) -> List[Dict[str, Any]]:
        """排序分析資料 - 修正版"""
        if not data:
            return data
        
        sort_key_map = {
            'underlying_name': 'underlying_name',
            'current_volume': 'current_volume', 
            'five_day_avg': 'five_day_avg',
            'volume_diff': 'volume_diff',
            'change_percent': 'change_percent'
        }
        
        if sort_by not in sort_key_map:
            sort_by = 'volume_diff'
        
        # 修正：變動量和變動率按絕對值排序，其他按實際值排序
        if sort_by == 'volume_diff':
            return sorted(data, key=lambda x: abs(x['volume_diff']), reverse=not ascending)
        elif sort_by == 'change_percent':
            return sorted(data, key=lambda x: abs(x['change_percent']), reverse=not ascending)
        else:
            return sorted(data, key=lambda x: x[sort_key_map[sort_by]], reverse=not ascending)
    
    def _get_latest_date(self) -> Optional[str]:
        """獲取最新資料日期"""
        try:
            query = "SELECT MAX(update_date) as latest_date FROM warrant_data"
            result = self._execute_query(query, fetch="one")
            return result['latest_date'] if result else None
        except Exception as e:
            logger.error(f"獲取最新日期錯誤: {e}")
            return None
    
    def _get_previous_five_dates(self, target_date: str) -> List[str]:
        """獲取目標日期前五個交易日"""
        try:
            ph = self._get_placeholder()
            query = f'''
                SELECT DISTINCT update_date 
                FROM warrant_data 
                WHERE update_date < {ph}
                ORDER BY update_date DESC 
                LIMIT 5
            '''
            results = self._execute_query(query, (target_date,), fetch="all")
            return [row['update_date'] for row in results] if results else []
        except Exception as e:
            logger.error(f"獲取前五日日期錯誤: {e}")
            return []
    
    def _get_underlying_volume_by_date_and_type(self, date: str, warrant_type: str) -> Dict[str, int]:
        """獲取指定日期和類型的標的成交量"""
        try:
            ph = self._get_placeholder()
            query = f'''
                SELECT 
                    underlying_name,
                    SUM(volume) as total_volume
                FROM warrant_data 
                WHERE update_date = {ph} 
                AND warrant_type = {ph}
                AND underlying_name IS NOT NULL 
                AND underlying_name != ''
                GROUP BY underlying_name
            '''
            results = self._execute_query(query, (date, warrant_type), fetch="all")
            
            volume_data = {}
            if results:
                for row in results:
                    volume_data[row['underlying_name']] = row['total_volume']
            
            return volume_data
        except Exception as e:
            logger.error(f"獲取成交量資料錯誤: {e}")
            return {}
    
    def _calculate_five_day_average_for_type(self, underlying_name: str, 
                                           dates: List[str], 
                                           warrant_type: str) -> int:
        """計算指定標的和類型的五日平均成交量"""
        if not dates:
            return 0
        
        try:
            ph = self._get_placeholder()
            placeholders = ','.join([ph] * len(dates))
            
            query = f'''
                SELECT 
                    SUM(volume) as daily_volume,
                    update_date
                FROM warrant_data 
                WHERE underlying_name = {ph} 
                AND warrant_type = {ph}
                AND update_date IN ({placeholders})
                GROUP BY update_date
            '''
            
            params = [underlying_name, warrant_type] + dates
            results = self._execute_query(query, tuple(params), fetch="all")
            
            # 計算平均值
            total_volume = sum(row['daily_volume'] for row in results) if results else 0
            days_count = len(dates)
            
            return int(total_volume / days_count) if days_count > 0 else 0
            
        except Exception as e:
            logger.error(f"計算五日平均錯誤: {e}")
            return 0
    
    def _get_placeholder(self) -> str:
        """獲取資料庫佔位符 - 修正版"""
        if (db_config and 
            hasattr(db_config, 'db_type') and 
            db_config.db_type == "postgresql"):
            return "%s"
        return "?"
    
    def _execute_query(self, query: str, params: tuple = (), fetch: str = "none"):
        """執行資料庫查詢 - 統一錯誤處理"""
        if not self.db_available:
            return [] if fetch == "all" else None
        
        try:
            return db_config.execute_query(query, params, fetch)
        except Exception as e:
            logger.error(f"查詢執行錯誤: {e}")
            logger.error(f"查詢: {query[:200]}...")
            logger.error(f"參數: {params}")
            return [] if fetch == "all" else None
    
    def get_available_dates(self) -> List[str]:
        """獲取可用的分析日期"""
        try:
            query = '''
                SELECT DISTINCT update_date 
                FROM warrant_data 
                ORDER BY update_date DESC 
                LIMIT 30
            '''
            results = self._execute_query(query, fetch="all")
            return [row['update_date'] for row in results] if results else []
        except Exception as e:
            logger.error(f"獲取可用日期錯誤: {e}")
            return []

# 全局實例
warrant_volume_analyzer = WarrantVolumeAnalyzer() if db_config else None