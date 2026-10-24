"""產生新的金鑰（INGEST_SECRET、SCHEDULER_TOKEN、WEB_PASSWORD…），寫進本機 .env 並複製到剪貼簿

用法（Windows CMD，在 breakout_detector 專案資料夾裡執行，或用 --env 指定 .env 路徑）：
    python new_secret.py                     # 換 INGEST_SECRET，寫進目前資料夾的 .env
    python new_secret.py --env D:\\path\\.env  # 指定 .env
    python new_secret.py --name SCHEDULER_TOKEN --no-env   # 只產生、複製，不寫檔
    python new_secret.py --show              # 同時印出完整值（預設只印前後 4 碼）

做的事：
  1. 用 secrets.token_hex(32) 產生 64 個十六進位字元（256 bit）
  2. 寫進 .env（同名那行換掉，沒有就加在最後；原檔先備份成 .env.bak）
  3. 複製到剪貼簿 → 直接到 Railway 的 etf-monitor → Variables 貼上，存檔後 Deploy
金鑰不會上傳到任何地方；這支檔案可以放進 git，.env 不行。
"""
import argparse
import secrets
import shutil
import subprocess
import sys
from pathlib import Path


def copy(text):
    try:
        if sys.platform.startswith("win"):
            subprocess.run("clip", input=text.encode("utf-16-le"), check=True, shell=True)
        elif sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text.encode(), check=True)
        else:
            subprocess.run(["xclip", "-selection", "clipboard"], input=text.encode(), check=True)
        return True
    except Exception:
        return False


def write_env(path: Path, name, value):
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    if path.exists():
        shutil.copy2(path, path.with_name(path.name + ".bak"))
    hit = False
    for i, ln in enumerate(lines):
        if ln.strip().split("=", 1)[0].strip() == name:
            lines[i] = f"{name}={value}"
            hit = True
    if not hit:
        lines.append(f"{name}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return hit


def main():
    ap = argparse.ArgumentParser(description="產生新金鑰並寫入 .env、複製到剪貼簿")
    ap.add_argument("--name", default="INGEST_SECRET", help="變數名稱（預設 INGEST_SECRET）")
    ap.add_argument("--env", default=".env", help=".env 路徑（預設目前資料夾的 .env）")
    ap.add_argument("--no-env", action="store_true", help="不寫 .env，只產生並複製")
    ap.add_argument("--show", action="store_true", help="印出完整值")
    a = ap.parse_args()

    value = secrets.token_hex(32)
    print(f"新的 {a.name}：{value if a.show else value[:4] + '…' + value[-4:]}（64 字元）")

    if not a.no_env:
        p = Path(a.env).expanduser().resolve()
        replaced = write_env(p, a.name, value)
        print(f"✅ 已{'更新' if replaced else '加入'} {p}" + ("（舊檔備份為 .env.bak）" if p.with_name(p.name + '.bak').exists() else ""))

    if copy(value):
        print("📋 已複製到剪貼簿")
    elif not a.show:
        print(f"⚠ 無法複製到剪貼簿，請手動複製：{value}")

    print("\n接下來：")
    print(f"  1. Railway → etf-monitor → Variables → {a.name}，貼上（Ctrl+V）→ 存檔 → Deploy")
    if a.name == "INGEST_SECRET":
        print("  2. 部署完成後，本機推一次量化研究結果測試；401『簽章不符』代表兩邊不一致")
    print("  ※ 不要把這串貼到聊天、email 或 git")


if __name__ == "__main__":
    main()
