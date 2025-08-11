# database_config.py - 完整修復版本
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
    """數據庫配置管理"""
    
    def __init__(self):
        # 優先使用 Railway 環境變數
        self.database_url = os.getenv("DATABASE_URL")
        
        # 如果沒有環境變數，使用本地 SQLite（開發環境）
        if not self.database_url:
            self.database_url = "sqlite:///etf_holdings.db"
            logger.warning("未找到 DATABASE_URL 環境變數，使用本地 SQLite")
        
        self.db_type = self._detect_db_type()
        
        # PostgreSQL 連接池
        self.pg_pool: Optional[SimpleConnectionPool] = None
        
        # SQLite 路徑
        self.sqlite_path = None
        
        self._initialize_database()
    
    def _detect_db_type(self) -> str:
        """檢測數據庫類型"""
        if not self.database_url:
            return "sqlite"
            
        if self.database_url.startswith("postgresql://") or self.database_url.startswith("postgres://"):
            return "postgresql"
        elif self.database_url.startswith("sqlite://"):
            return "sqlite"
        else:
            # 如果是未知格式，默認 SQLite
            logger.warning(f"未知數據庫 URL 格式: {self.database_url[:50]}...")
            return "sqlite"
    
    def _initialize_database(self):
        """初始化數據庫連接"""
        logger.info(f"初始化數據庫連接，類型: {self.db_type}")
        logger.info(f"數據庫 URL: {self.database_url[:50]}...")
        
        if self.db_type == "postgresql":
            success = self._initialize_postgresql()
            if not success:
                logger.error("PostgreSQL 初始化失敗，降級到 SQLite")
                self.db_type = "sqlite"
                self._initialize_sqlite()
        else:
            self._initialize_sqlite()
    
    def _initialize_postgresql(self) -> bool:
        """初始化 PostgreSQL 連接池"""
        try:
            # 處理 Railway 的 DATABASE_URL 格式
            database_url = self.database_url
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql://", 1)
            
            # 解析 DATABASE_URL
            parsed = urlparse(database_url)
            
            if not parsed.hostname or not parsed.username:
                logger.error("PostgreSQL URL 格式不正確")
                return False
            
            logger.info(f"連接到 PostgreSQL: {parsed.hostname}:{parsed.port or 5432}")
            
            # 先測試連接 - 直接測試而不使用連接池
            try:
                logger.info("測試 PostgreSQL 連接...")
                test_conn = psycopg2.connect(
                    database_url,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=10
                )
                test_conn.autocommit = True
                
                with test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    logger.info(f"PostgreSQL 版本: {version['version'][:50]}...")
                
                test_conn.close()
                logger.info("PostgreSQL 連接測試成功")
                
            except Exception as e:
                logger.error(f"PostgreSQL 連接測試失敗: {e}")
                return False
            
            # 連接測試成功，創建連接池
            try:
                logger.info("創建 PostgreSQL 連接池...")
                self.pg_pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=20,
                    host=parsed.hostname,
                    port=parsed.port or 5432,
                    database=parsed.path[1:] if parsed.path else 'railway',
                    user=parsed.username,
                    password=parsed.password,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    sslmode='require',
                    connect_timeout=30
                )
                
                logger.info("PostgreSQL 連接池初始化成功")
                return True
                
            except Exception as e:
                logger.error(f"PostgreSQL 連接池創建失敗: {e}")
                return False
            
        except Exception as e:
            logger.error(f"PostgreSQL 初始化失敗: {e}")
            return False
    
    def _initialize_sqlite(self):
        """初始化 SQLite"""
        if self.database_url.startswith("sqlite://"):
            self.sqlite_path = self.database_url[10:]  # 移除 'sqlite://'
        else:
            self.sqlite_path = self.database_url
        
        logger.info(f"使用 SQLite 數據庫: {self.sqlite_path}")
    
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
            conn.row_factory = sqlite3.Row  # 使結果像字典一樣可訪問
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
                    # 轉換為字典列表
                    if self.db_type == "sqlite":
                        return [dict(row) for row in result]
                    else:
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
            # SQLite -> PostgreSQL 語法轉換
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            query = query.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            # 參數佔位符已經是 %s，不需要轉換
        return query
    
    def close(self):
        """關閉數據庫連接"""
        if self.db_type == "postgresql" and self.pg_pool:
            self.pg_pool.closeall()
            logger.info("PostgreSQL 連接池已關閉")

# 全局數據庫實例
db_config = DatabaseConfig()