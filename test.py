# diagnose_db_connection.py - 診斷數據庫連接問題
import os
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

def diagnose_connection():
    """診斷數據庫連接問題"""
    print("=== 數據庫連接診斷工具 ===\n")
    
    # 1. 檢查環境變數
    print("📋 檢查環境變數...")
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("❌ DATABASE_URL 環境變數不存在")
        print("可能的原因:")
        print("1. Railway 環境變數設置錯誤")
        print("2. 應用沒有正確讀取環境變數")
        return
    
    print(f"✅ DATABASE_URL 存在")
    print(f"   值: {database_url[:50]}...")
    
    # 2. 檢查 URL 格式
    print(f"\n🔍 分析 DATABASE_URL...")
    
    if database_url.startswith("sqlite"):
        print("❌ DATABASE_URL 指向 SQLite!")
        print(f"   完整值: {database_url}")
        print("這解釋了為什麼應用使用 SQLite")
        return
    elif database_url.startswith("postgresql://") or database_url.startswith("postgres://"):
        print("✅ DATABASE_URL 指向 PostgreSQL")
        
        # 解析 URL
        try:
            parsed = urlparse(database_url)
            print(f"   主機: {parsed.hostname}")
            print(f"   端口: {parsed.port}")
            print(f"   數據庫: {parsed.path[1:] if parsed.path else 'None'}")
            print(f"   用戶: {parsed.username}")
        except Exception as e:
            print(f"   ❌ URL 解析錯誤: {e}")
            return
    else:
        print(f"❌ 未知的 DATABASE_URL 格式: {database_url}")
        return
    
    # 3. 測試連接
    print(f"\n🔗 測試 PostgreSQL 連接...")
    
    try:
        # 處理 postgres:// 前綴
        test_url = database_url
        if test_url.startswith("postgres://"):
            test_url = test_url.replace("postgres://", "postgresql://", 1)
        
        conn = psycopg2.connect(
            test_url,
            cursor_factory=psycopg2.extras.RealDictCursor,
            sslmode='require',
            connect_timeout=10
        )
        conn.autocommit = True
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print("✅ PostgreSQL 連接成功!")
        print(f"   版本: {version['version'][:50]}...")
        
        # 檢查表是否存在
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        print(f"   表數量: {len(tables)}")
        if tables:
            print(f"   表名: {[t['table_name'] for t in tables]}")
            
            # 檢查數據
            if 'etf_holdings' in [t['table_name'] for t in tables]:
                cursor.execute("SELECT COUNT(*) as count FROM etf_holdings")
                count = cursor.fetchone()['count']
                print(f"   etf_holdings 記錄數: {count}")
        
        conn.close()
        
    except psycopg2.OperationalError as e:
        print(f"❌ PostgreSQL 連接失敗: {e}")
        print("可能的原因:")
        print("1. 網絡連接問題")
        print("2. PostgreSQL 服務未運行")
        print("3. 連接字符串錯誤")
        print("4. SSL/安全配置問題")
        return
    except Exception as e:
        print(f"❌ 其他連接錯誤: {e}")
        return
    
    # 4. 測試應用的數據庫配置
    print(f"\n🧪 測試應用數據庫配置...")
    
    try:
        from database_config import db_config
        
        print(f"✅ database_config 模組加載成功")
        print(f"   檢測到的數據庫類型: {db_config.db_type}")
        print(f"   數據庫 URL: {db_config.database_url[:50]}...")
        
        if db_config.db_type == "sqlite":
            print("❌ 應用仍在使用 SQLite!")
            print("可能的原因:")
            print("1. PostgreSQL 連接失敗，自動降級到 SQLite")
            print("2. database_config.py 中的邏輯問題")
            print("3. 環境變數讀取問題")
        elif db_config.db_type == "postgresql":
            print("✅ 應用配置為使用 PostgreSQL")
            
            # 測試應用的連接
            try:
                with db_config.get_connection() as conn:
                    print("✅ 應用數據庫連接測試成功")
            except Exception as e:
                print(f"❌ 應用數據庫連接測試失敗: {e}")
        
    except ImportError as e:
        print(f"❌ 無法導入 database_config: {e}")
    except Exception as e:
        print(f"❌ database_config 測試錯誤: {e}")
    
    # 5. 檢查 Railway 特定問題
    print(f"\n🚂 Railway 特定檢查...")
    
    # 檢查是否在 Railway 環境中
    railway_env = os.getenv("RAILWAY_ENVIRONMENT")
    if railway_env:
        print(f"✅ 在 Railway 環境中: {railway_env}")
    else:
        print("⚠️ 可能不在 Railway 環境中")
    
    # 檢查其他 Railway 變數
    railway_vars = [
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID", 
        "RAILWAY_DEPLOYMENT_ID"
    ]
    
    for var in railway_vars:
        value = os.getenv(var)
        if value:
            print(f"   {var}: {value[:20]}...")
        else:
            print(f"   {var}: 未設置")

if __name__ == "__main__":
    diagnose_connection()