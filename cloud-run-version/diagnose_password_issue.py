#!/usr/bin/env python3
# 密碼診斷腳本
import os
import sys
from pathlib import Path

def diagnose_password_issue():
    """診斷密碼設置問題"""
    print("🔍 密碼設置診斷報告")
    print("=" * 50)
    
    # 1. 檢查環境變數
    web_password_env = os.getenv("WEB_PASSWORD")
    print(f"1. 環境變數 WEB_PASSWORD: {web_password_env or '未設置'}")
    
    # 2. 檢查預設值
    default_password = "etf2024"
    current_password = os.getenv("WEB_PASSWORD", default_password)
    print(f"2. 當前使用的密碼: {current_password}")
    
    # 3. 檢查 .env 文件
    env_files = [".env", ".env.local", ".env.development"]
    print(f"3. 檢查 .env 文件:")
    
    for env_file in env_files:
        if Path(env_file).exists():
            print(f"   ✅ 找到文件: {env_file}")
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if "WEB_PASSWORD" in content:
                        lines = [line.strip() for line in content.split('\n') 
                                if line.strip() and 'WEB_PASSWORD' in line and not line.strip().startswith('#')]
                        for line in lines:
                            print(f"      內容: {line}")
                    else:
                        print(f"      沒有 WEB_PASSWORD 設置")
            except Exception as e:
                print(f"      讀取錯誤: {e}")
        else:
            print(f"   ❌ 文件不存在: {env_file}")
    
    # 4. 檢查所有環境變數
    print(f"4. 所有相關環境變數:")
    for key, value in os.environ.items():
        if "PASSWORD" in key.upper() or "PASS" in key.upper():
            print(f"   {key}: {value}")
    
    # 5. 測試密碼驗證
    print(f"5. 密碼驗證測試:")
    test_passwords = ["etf2024", "TestPassword123", current_password]
    
    for test_pwd in set(test_passwords):  # 去重
        is_correct = test_pwd == current_password
        status = "✅ 正確" if is_correct else "❌ 錯誤"
        print(f"   測試密碼 '{test_pwd}': {status}")
    
    # 6. 建議
    print(f"6. 修復建議:")
    if current_password == "TestPassword123":
        print("   🚨 檢查是否有 .env 文件設置了 WEB_PASSWORD=TestPassword123")
        print("   🔧 建議清除環境變數或修改 .env 文件")
    elif current_password == "etf2024":
        print("   ✅ 密碼設置正常，應該使用 'etf2024' 登錄")
    else:
        print(f"   🤔 密碼被設置為非預期值: {current_password}")
    
    print("=" * 50)
    print(f"🔑 結論：請使用密碼 '{current_password}' 登錄")

if __name__ == "__main__":
    diagnose_password_issue()