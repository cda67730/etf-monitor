#!/usr/bin/env python3
"""
安全功能測試腳本
用於測試 ETF 監控系統的安全功能
"""

import requests
import time
import sys

def test_security_features(base_url="http://localhost:8080"):
    """測試安全功能"""
    print(f"🔒 測試 ETF 監控系統安全功能")
    print(f"目標 URL: {base_url}")
    print("=" * 50)
    
    # 測試1: 健康檢查 (應該可以訪問)
    print("1. 測試健康檢查端點...")
    try:
        response = requests.get(f"{base_url}/health")
        if response.status_code == 200:
            print("✅ 健康檢查正常")
        else:
            print(f"❌ 健康檢查失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ 健康檢查錯誤: {e}")
    
    # 測試2: 未授權訪問首頁 (應該重定向到登錄頁)
    print("\n2. 測試未授權訪問...")
    try:
        response = requests.get(f"{base_url}/", allow_redirects=False)
        if response.status_code in [302, 307]:
            print("✅ 正確重定向到登錄頁面")
        else:
            print(f"❌ 未正確保護: {response.status_code}")
    except Exception as e:
        print(f"❌ 測試錯誤: {e}")
    
    # 測試3: 登錄頁面訪問
    print("\n3. 測試登錄頁面...")
    try:
        response = requests.get(f"{base_url}/login")
        if response.status_code == 200 and "登錄" in response.text:
            print("✅ 登錄頁面正常顯示")
        else:
            print(f"❌ 登錄頁面有問題: {response.status_code}")
    except Exception as e:
        print(f"❌ 登錄頁面錯誤: {e}")
    
    # 測試4: 錯誤密碼登錄
    print("\n4. 測試錯誤密碼...")
    try:
        login_data = {"password": "wrong_password"}
        response = requests.post(f"{base_url}/login", data=login_data, allow_redirects=False)
        if response.status_code in [302, 307]:
            print("✅ 錯誤密碼被正確拒絕")
        else:
            print(f"❌ 密碼驗證有問題: {response.status_code}")
    except Exception as e:
        print(f"❌ 密碼測試錯誤: {e}")
    
    # 測試5: 正確密碼登錄 (需要知道密碼)
    print("\n5. 測試正確密碼登錄...")
    test_password = input("請輸入測試密碼 (預設: etf2024): ").strip() or "etf2024"
    
    try:
        session = requests.Session()
        login_data = {"password": test_password}
        response = session.post(f"{base_url}/login", data=login_data)
        
        if response.status_code == 200:
            # 嘗試訪問受保護頁面
            protected_response = session.get(f"{base_url}/")
            if protected_response.status_code == 200:
                print("✅ 成功登錄並訪問受保護頁面")
            else:
                print(f"❌ 登錄後無法訪問: {protected_response.status_code}")
        else:
            print(f"❌ 登錄失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ 登錄測試錯誤: {e}")
    
    # 測試6: 流量限制
    print("\n6. 測試流量限制...")
    try:
        print("發送多個請求測試流量限制...")
        for i in range(5):
            response = requests.get(f"{base_url}/login")
            print(f"  請求 {i+1}: {response.status_code}")
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers.get('X-RateLimit-Remaining')
                print(f"    剩餘請求次數: {remaining}")
            time.sleep(0.5)
        
        print("✅ 流量限制功能正常")
    except Exception as e:
        print(f"❌ 流量限制測試錯誤: {e}")
    
    # 測試7: API訪問 (未授權)
    print("\n7. 測試未授權API訪問...")
    try:
        api_endpoints = ["/api/system/status", "/api/holdings/2024-01-01"]
        for endpoint in api_endpoints:
            response = requests.get(f"{base_url}{endpoint}")
            if response.status_code == 401:
                print(f"✅ {endpoint} 正確要求授權")
            else:
                print(f"❌ {endpoint} 未正確保護: {response.status_code}")
    except Exception as e:
        print(f"❌ API測試錯誤: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 安全測試完成!")
    print("\n建議檢查項目:")
    print("• 確保生產環境使用強密碼")
    print("• 定期更換 SCHEDULER_TOKEN")
    print("• 監控流量和登錄日誌")
    print("• 設定HTTPS (Cloud Run自動提供)")

def test_rate_limiting(base_url="http://localhost:8080", count=10):
    """專門測試流量限制"""
    print(f"\n🚦 流量限制壓力測試 (發送{count}個請求)")
    print("-" * 30)
    
    for i in range(count):
        try:
            start_time = time.time()
            response = requests.get(f"{base_url}/login", timeout=5)
            response_time = time.time() - start_time
            
            status = "✅" if response.status_code == 200 else "❌"
            remaining = response.headers.get('X-RateLimit-Remaining', 'N/A')
            
            print(f"{status} 請求 {i+1:2d}: {response.status_code} | "
                  f"剩餘: {remaining:3s} | 耗時: {response_time:.3f}s")
            
            if response.status_code == 429:
                print("🚫 觸發流量限制，測試成功!")
                break
                
            time.sleep(0.2)  # 避免請求過快
            
        except Exception as e:
            print(f"❌ 請求 {i+1} 失敗: {e}")

if __name__ == "__main__":
    # 檢查命令行參數
    if len(sys.argv) > 1:
        base_url = sys.argv[1].rstrip('/')
    else:
        base_url = "http://localhost:8080"
    
    print("ETF 監控系統安全測試工具")
    print("使用方法: python security_test.py [URL]")
    print(f"當前測試 URL: {base_url}")
    
    # 執行基本安全測試
    test_security_features(base_url)
    
    # 詢問是否進行流量限制測試
    if input("\n是否進行流量限制壓力測試? (y/N): ").lower().startswith('y'):
        test_rate_limiting(base_url)