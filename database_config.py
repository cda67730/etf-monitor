# database_config_improved.py - 改進版本，增強調試和錯誤處理
import os
import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import logging
from typing import Optional, Dict, Any, Union
from contextlib import contextmanager
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DatabaseConfig:
    """數據庫配置管理 - 改進版本"""
    
    def __init__(self):
        # 詳細記錄初始化過程
        logger.info("開始初始化數據庫配置...")
        
        # 檢查所有可能的環境變數
        self.database_url = self._get_database_url()
        
        logger.info(f"最終使用的 DATABASE_URL: {self.database_url[:50] if self.database_url else 'None'}...")
        
        self.db_type = self._detect_db_type()
        logger.info(f"檢測到數據庫類型: {self.db_type}")
        
        # PostgreSQL 連接池
        self.pg_pool: Optional[SimpleConnectionPool] = None
        
        # SQLite 路徑
        self.sqlite_path = None
        
        # 連接狀態
        self.connection_status = "unknown"
        
        self._initialize_database()
    
    def _get_database_url(self) -> str:
        """獲取數據庫 URL，檢查多個可能的環境變數"""
        
        # 檢查常見的環境變數
        env_vars_to_check = [
            "DATABASE_URL",
            "POSTGRES_URL", 
            "POSTGRESQL_URL",
            "DB_URL"
        ]
        
        logger.info("檢查環境變數...")
        for var_name in env_vars_to_check:
            value = os.getenv(var_name)
            if value:
                logger.info(f"找到環境變數 {var_name}: {value[:50]}...")
                return value
            else:
                logger.debug(f"環境變數 {var_name} 未設置")
        
        # 檢查 Railway 特定環境變數
        railway_vars = [
            "RAILWAY_ENVIRONMENT",
            "RAILWAY_PROJECT_ID", 
            "RAILWAY_SERVICE_ID"
        ]
        
        railway_detected = False
        for var in railway_vars:
            if os.getenv(var):
                railway_detected = True
                logger.info(f"檢測到 Railway 環境變數: {var}={os.getenv(var)}")
        
        if railway_detected:
            logger.warning("檢測到 Railway 環境但未找到 DATABASE_URL，請檢查環境變數設置")
        
        # 如果在 Railway 環境但沒有找到 DATABASE_URL，這是個問題
        if railway_detected and not any(os.getenv(var) for var in env_vars_to_check):
            logger.error("❌ 在 Railway 環境中但未找到數據庫 URL！")
            logger.error("請確保在 Railway 項目中正確設置了 DATABASE_URL 環境變數")
        
        # 默認使用 SQLite
        default_sqlite = "sqlite:///etf_holdings.db"
        logger.warning(f"未找到數據庫 URL 環境變數，使用默認 SQLite: {default_sqlite}")
        return default_sqlite
    
    def _detect_db_type(self) -> str:
        """檢測數據庫類型，增強錯誤處理"""
        if not self.database_url:
            logger.warning("database_url 為空，默認使用 SQLite")
            return "sqlite"
        
        logger.info(f"分析數據庫 URL: {self.database_url[:50]}...")
        
        if self.database_url.startswith(("postgresql://", "postgres://")):
            logger.info("檢測到 PostgreSQL URL")
            return "postgresql"
        elif self.database_url.startswith("sqlite://"):
            logger.info("檢測到 SQLite URL") 
            return "sqlite"
        elif self.database_url.endswith(".db"):
            logger.info("檢測到 SQLite 文件路徑")
            return "sqlite"
        else:
            logger.warning(f"未知數據庫 URL 格式: {self.database_url[:50]}..., 默認使用 SQLite")
            return "sqlite"
    
    def _initialize_database(self):
        """初始化數據庫連接，增強錯誤處理"""
        logger.info(f"初始化數據庫連接，類型: {self.db_type}")
        
        if self.db_type == "postgresql":
            success = self._initialize_postgresql()
            if success:
                self.connection_status = "postgresql_connected"
                logger.info("✅ PostgreSQL 初始化成功")
            else:
                logger.error("❌ PostgreSQL 初始化失敗，降級到 SQLite")
                self.db_type = "sqlite"
                self._initialize_sqlite()
                self.connection_status = "sqlite_fallback"
        else:
            self._initialize_sqlite()
            self.connection_status = "sqlite_only"
        
        logger.info(f"最終數據庫狀態: {self.connection_status}")
    
    def _initialize_postgresql(self) -> bool:
        """初始化 PostgreSQL，詳細錯誤處理"""
        try:
            logger.info("開始 PostgreSQL 初始化...")
            
            # 處理 Railway 的 DATABASE_URL 格式
            database_url = self.database_url
            if database_url.startswith("postgres://"):
                logger.info("轉換 postgres:// 為 postgresql://")
                database_url = database_url.replace("postgres://", "postgresql://", 1)
            
            # 解析 DATABASE_URL
            logger.info("解析數據庫 URL...")
            parsed = urlparse(database_url)
            
            if not parsed.hostname:
                logger.error("❌ PostgreSQL URL 缺少 hostname")
                return False
            
            if not parsed.username:
                logger.error("❌ PostgreSQL URL 缺少 username") 
                return False
            
            logger.info(f"PostgreSQL 連接信息:")
            logger.info(f"  Host: {parsed.hostname}")
            logger.info(f"  Port: {parsed.port or 5432}")
            logger.info(f"  Database: {parsed.path[1:] if parsed.path else 'Unknown'}")
            logger.info(f"  Username: {parsed.username}")
            logger.info(f"  Password: {'設置' if parsed.password else '未設置'}")
            
            # 先進行連接測試
            logger.info("執行連接測試...")
            test_conn = None
            try:
                test_conn = psycopg2.connect(
                    database_url,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=15
                )
                test_conn.autocommit = True
                
                with test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    logger.info(f"✅ PostgreSQL 版本: {version['version'][:80]}...")
                    
                    # 測試基本操作
                    cur.execute("SELECT current_database();")
                    db_name = cur.fetchone()
                    logger.info(f"✅ 當前數據庫: {db_name['current_database']}")
                
                logger.info("✅ PostgreSQL 連接測試成功")
                
            except psycopg2.OperationalError as e:
                logger.error(f"❌ PostgreSQL 連接測試失敗 (OperationalError): {e}")
                return False
            except psycopg2.Error as e:
                logger.error(f"❌ PostgreSQL 錯誤: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ 連接測試異常: {e}")
                return False
            finally:
                if test_conn:
                    test_conn.close()
            
            # 連接測試成功，創建連接池
            logger.info("創建 PostgreSQL 連接池...")
            try:
                self.pg_pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,  # 減少最大連接數
                    host=parsed.hostname,
                    port=parsed.port or 5432,
                    database=parsed.path[1:] if parsed.path else 'railway',
                    user=parsed.username,
                    password=parsed.password,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=30
                )
                
                # 測試從連接池獲取連接
                test_pool_conn = self.pg_pool.getconn()
                self.pg_pool.putconn(test_pool_conn)
                
                logger.info("✅ PostgreSQL 連接池創建成功")
                return True
                
            except Exception as e:
                logger.error(f"❌ PostgreSQL 連接池創建失敗: {e}")
                return False
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL 初始化失敗: {e}")
            logger.error(f"錯誤詳情: {type(e).__name__}: {str(e)}")
            return False
    
    def _initialize_sqlite(self):
        """初始化 SQLite"""
        if self.database_url.startswith("sqlite://"):
            self.sqlite_path = self.database_url[9:]  # 移除 'sqlite://'
        elif self.database_url.startswith("sqlite:///"):
            self.sqlite_path = self.database_url[10:]  # 移除 'sqlite:///'
        else:
            self.sqlite_path = self.database_url
        
        logger.info(f"✅ 使用 SQLite 數據庫: {self.sqlite_path}")
        
        # 測試 SQLite 連接
        try:
            conn = sqlite3.connect(self.sqlite_path)
            conn.close()
            logger.info("✅ SQLite 連接測試成功")
        except Exception as e:
            logger.error(f"❌ SQLite 連接測試失敗: {e}")
    
    @contextmanager
    def get_connection(self):
        """獲取數據庫連接的上下文管理器"""
        if self.db_type == "postgresql":
            if not self.pg_pool:
                raise Exception("PostgreSQL 連接池未初始化")
                
            conn = None
            try:
                conn = self.pg_pool.getconn()
                conn.autocommit = True
                yield conn
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"PostgreSQL 連接錯誤: {e}")
                raise e
            finally:
                if conn:
                    self.pg_pool.putconn(conn)
        else:
            conn = sqlite3.connect(self.sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = (), fetch: str = "none") -> Union[list, dict, None]:
        """執行查詢"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 轉換查詢語法
                converted_query = self._convert_query_syntax(query)
                cursor.execute(converted_query, params)
                
                if fetch == "all":
                    result = cursor.fetchall()
                    return [dict(row) for row in result]
                elif fetch == "one":
                    result = cursor.fetchone()
                    if result:
                        return dict(result)
                    return None
                else:
                    if self.db_type == "sqlite":
                        conn.commit()
                    return None
        except Exception as e:
            logger.error(f"查詢執行錯誤: {e}")
            logger.error(f"查詢: {query}")
            logger.error(f"參數: {params}")
            raise e
    
    def _convert_query_syntax(self, query: str) -> str:
        """轉換查詢語法以適應不同數據庫"""
        if self.db_type == "postgresql":
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            query = query.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        return query
    
    def get_status(self) -> Dict[str, Any]:
        """獲取數據庫狀態信息"""
        return {
            "db_type": self.db_type,
            "connection_status": self.connection_status,
            "database_url_prefix": self.database_url[:50] if self.database_url else None,
            "has_pool": self.pg_pool is not None,
            "sqlite_path": self.sqlite_path if self.db_type == "sqlite" else None
        }
    
    def close(self):
        """關閉數據庫連接"""
        if self.db_type == "postgresql" and self.pg_pool:
            self.pg_pool.closeall()
            logger.info("PostgreSQL 連接池已關閉")

# 全局數據庫實例
db_config = DatabaseConfig()