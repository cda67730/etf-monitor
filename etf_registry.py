"""ETF 清單（etf_registry 資料表）

爬蟲與頁面都從這裡讀取要追蹤的 ETF，取代原本寫死在程式裡的代號清單。
資料庫不可用時退回 DEFAULT_ETFS，確保網站與爬蟲照常運作。
"""
import logging
import time

logger = logging.getLogger(__name__)

CATEGORIES = ["國內主動", "國外為主", "高股息"]
NON_AGGRESSIVE = {"國外為主", "高股息"}   # 積極型＝排除這些分類

# 首次建表時匯入；分類一律先設「國內主動」，由使用者在 /admin/etfs 調整
DEFAULT_ETFS = {
    '00980A': '主動野村臺灣優選ETF',
    '00981A': '統一台股增長主動式ETF',
    '00982A': '群益台灣精選強棒主動式ETF',
    '00984A': '安聯台灣高股息成長主動式ETF',
    '00985A': '野村台灣增強50主動式ETF',
    '00991A': '復華未來50主動式ETF',
    '00992A': '群益科技創新主動式ETF',
    '00993A': '安聯台灣主動式ETF',
    '00994A': '第一金台股趨勢優選主動式ETF',
    '00995A': '中信台灣卓越主動式ETF',
    '00403A': '統一台股升級50主動式ETF',
    '00996A': '兆豐台灣豐收主動式ETF',
    '00999A': '野村臺灣策略高息主動式ETF',
    '00404A': '聯博台灣動能收益50主動式ETF',
    '00405A': '富邦台灣龍耀主動式ETF',
    '00406A': '中信台灣收益主動式ETF',
}

# 之後補進來的 ETF：每批只匯入一次（記在 etf_registry_migrations），已存在的代號不動，
# 使用者之後在管理頁刪掉也不會被加回來。代號: (名稱, 分類)
ADDITIONS = {
    "2026-10-04": {
        "00400A": ("國泰動能高息主動式ETF", "高股息"),
        "00401A": ("摩根台灣鑫收主動式ETF", "高股息"),
        "00407A": ("凱基台灣主動式ETF", "國內主動"),
        "00408A": ("第一金優股息主動式ETF", "高股息"),
        "00410A": ("永豐台灣科技趨勢主動式ETF", "國內主動"),
        "00986A": ("台新龍頭成長主動式ETF", "國內主動"),
        "00987A": ("台新優勢成長主動式ETF", "國內主動"),
        "00998A": ("復華金融股息主動式ETF", "高股息"),
    },
}

_CACHE_SEC = 30


class ETFRegistry:
    def __init__(self, db_config):
        self.db = db_config
        self._cache = None
        self._cache_at = 0
        self.ready = False
        if self.db:
            try:
                self._ensure_table()
                self.ready = True
            except Exception as e:
                logger.error(f"❌ etf_registry 初始化失敗，改用內建清單: {e}")

    # ---------- 基礎 ----------
    @property
    def ph(self):
        return "%s" if self.db.db_type == "postgresql" else "?"

    def _q(self, sql, params=(), fetch="none"):
        return self.db.execute_query(sql.replace("?", self.ph), params, fetch=fetch)

    def _ensure_table(self):
        self._q("""
            CREATE TABLE IF NOT EXISTS etf_registry (
                etf_code   TEXT PRIMARY KEY,
                etf_name   TEXT,
                category   TEXT NOT NULL DEFAULT '國內主動',
                enabled    INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        n = self._q("SELECT COUNT(*) AS n FROM etf_registry", fetch="one")["n"]
        if n == 0:
            for i, (code, name) in enumerate(DEFAULT_ETFS.items()):
                self._q("INSERT INTO etf_registry (etf_code, etf_name, category, enabled, sort_order) "
                        "VALUES (?, ?, ?, 1, ?)", (code, name, "國內主動", i))
            logger.info(f"✅ etf_registry 首次建立，匯入 {len(DEFAULT_ETFS)} 檔 ETF")
        self._apply_additions()

    def _apply_additions(self):
        self._q("CREATE TABLE IF NOT EXISTS etf_registry_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        done = {r["name"] for r in (self._q("SELECT name FROM etf_registry_migrations", fetch="all") or [])}
        for batch, items in ADDITIONS.items():
            if batch in done:
                continue
            have = {r["etf_code"] for r in (self._q("SELECT etf_code FROM etf_registry", fetch="all") or [])}
            mx = self._q("SELECT COALESCE(MAX(sort_order), -1) AS m FROM etf_registry", fetch="one")["m"]
            added = []
            for code, (name, cat) in items.items():
                if code in have:
                    continue
                mx += 1
                self._q("INSERT INTO etf_registry (etf_code, etf_name, category, enabled, sort_order) VALUES (?, ?, ?, 1, ?)",
                        (code, name, cat, mx))
                added.append(code)
            self._q("INSERT INTO etf_registry_migrations (name) VALUES (?)", (batch,))
            logger.info(f"✅ etf_registry 補入 {batch}：{'、'.join(added) or '（都已存在）'}")

    def _invalidate(self):
        self._cache = None

    def _rows(self):
        if not self.ready:
            return [{"etf_code": c, "etf_name": n, "category": "國內主動", "enabled": 1, "sort_order": i}
                    for i, (c, n) in enumerate(DEFAULT_ETFS.items())]
        if self._cache is None or time.time() - self._cache_at > _CACHE_SEC:
            self._cache = self._q("SELECT etf_code, etf_name, category, enabled, sort_order "
                                  "FROM etf_registry ORDER BY sort_order, etf_code", fetch="all") or []
            self._cache_at = time.time()
        return self._cache

    # ---------- 讀取 ----------
    def all(self):
        return [dict(r, enabled=bool(r["enabled"])) for r in self._rows()]

    def enabled(self, aggressive_only=False):
        rows = [r for r in self.all() if r["enabled"]]
        if aggressive_only:
            rows = [r for r in rows if r["category"] not in NON_AGGRESSIVE]
        return rows

    def enabled_codes(self, aggressive_only=False):
        return [r["etf_code"] for r in self.enabled(aggressive_only)]

    def names(self, enabled_only=True):
        rows = self.enabled() if enabled_only else self.all()
        return {r["etf_code"]: (r["etf_name"] or r["etf_code"]) for r in rows}

    def get(self, code):
        return next((r for r in self.all() if r["etf_code"] == code), None)

    # ---------- 寫入 ----------
    def _require(self):
        if not self.ready:
            raise RuntimeError("資料庫不可用，無法修改 ETF 清單")

    def add(self, code, name, category):
        self._require()
        mx = self._q("SELECT COALESCE(MAX(sort_order), -1) AS m FROM etf_registry", fetch="one")["m"]
        self._q("INSERT INTO etf_registry (etf_code, etf_name, category, enabled, sort_order) "
                "VALUES (?, ?, ?, 1, ?)", (code, name, category, mx + 1))
        self._invalidate()

    def update(self, code, **fields):
        self._require()
        allowed = {k: v for k, v in fields.items() if k in ("etf_name", "category", "enabled")}
        if not allowed:
            return
        if "enabled" in allowed:
            allowed["enabled"] = 1 if allowed["enabled"] else 0
        sets = ", ".join(f"{k} = ?" for k in allowed) + ", updated_at = CURRENT_TIMESTAMP"
        self._q(f"UPDATE etf_registry SET {sets} WHERE etf_code = ?", (*allowed.values(), code))
        self._invalidate()

    def reorder(self, codes):
        self._require()
        for i, code in enumerate(codes):
            self._q("UPDATE etf_registry SET sort_order = ? WHERE etf_code = ?", (i, code))
        self._invalidate()

    def delete(self, code):
        self._require()
        self._q("DELETE FROM etf_registry WHERE etf_code = ?", (code,))
        self._invalidate()

    # ---------- 管理頁用：每檔最新資料日期與筆數 ----------
    def latest_stats(self):
        if not self.ready:
            return {}
        rows = self._q("""
            SELECT h.etf_code, h.update_date AS latest_date, COUNT(*) AS latest_rows
            FROM etf_holdings h
            JOIN (SELECT etf_code, MAX(update_date) AS d FROM etf_holdings GROUP BY etf_code) m
              ON h.etf_code = m.etf_code AND h.update_date = m.d
            GROUP BY h.etf_code, h.update_date
        """, fetch="all") or []
        return {r["etf_code"]: r for r in rows}


registry = None


def init(db_config):
    global registry
    registry = ETFRegistry(db_config)
    return registry
