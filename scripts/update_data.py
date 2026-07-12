# -*- coding: utf-8 -*-
"""
update_data.py
每日自動抓取 TWSE 上市股票資料，計算「核心1：近5日強度」與「核心2：近20日趨勢潛力」選股名單。

執行流程：
  1. 抓取 TWSE OpenAPI STOCK_DAY_ALL
  2. 用台積電(2330)+聯發科(2454)雙重比對，判斷今天是否為新的交易日（排除假日/國定假日）
  3. 若是新交易日：篩選成交金額前30 + 漲跌>=0 → 計算當日分數 → 寫入滾動資料庫
  4. 計算核心1（近5日累積分數前15檔）
  5. 計算核心2（近20日雙斜率趨勢前15檔，排除核心1名單）
  6. 比對升降分類，標記紅色▲/綠色▼
  7. 執行20日滾動清理（逐股獨立判斷）
  8. 輸出 data/stock_pool.json（滾動資料庫）與 docs/result.json（給網頁顯示用）
"""

import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ---------- 路徑設定 ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)  # scripts/ 的上一層 = repo 根目錄
POOL_PATH = os.path.join(REPO_ROOT, "data", "stock_pool.json")
RESULT_PATH = os.path.join(REPO_ROOT, "docs", "result.json")
THEME_MAPPING_PATH = os.path.join(REPO_ROOT, "data", "theme_mapping.json")
INDUSTRY_MAPPING_PATH = os.path.join(REPO_ROOT, "data", "industry_mapping.json")

STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
MI_INDEX_URL = "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX"  # 大盤指數(含加權指數)當日快照
TWSE_HISTORICAL_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"  # 支援查詢過去任一交易日
ISIN_LISTED_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
ISIN_OTC_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
TPEX_DAILY_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
INDUSTRY_REFRESH_DAYS = 7   # 官方產業別資料變動很慢，快取幾天重抓一次即可，不用每天抓

TOP_N = 30          # 成交金額排名取前幾檔
CORE1_DAYS = 5       # 核心1觀察天數
CORE2_DAYS = 20      # 核心2觀察天數
CORE1_TOPK = 15
CORE2_TOPK = 15
MIN_APPEARANCE_FOR_CORE2 = 2   # 核心2最低上榜次數門檻
TRADING_DAYS_BUFFER = 30       # trading_days 清單保留的緩衝天數（要大於20才能正確判斷滾動刪除）
REFERENCE_CODES = ["2330", "2454"]  # 用來判斷是否為新交易日的基準股

HEAT_BREADTH_TOP_N = 50   # 熱度指標：全市場成交金額前幾檔納入統計
# 熱度燈號門檻(由高到低比對，avg >= 門檻值 即屬於該級距)
HEAT_THRESHOLDS = [(44, 5), (32, 4), (20, 3), (8, 2), (0, 1)]
CORE1_HEAT_LABELS = {
    5: "短線市場極度強勢", 4: "短線市場樂觀", 3: "短線市場震盪",
    2: "短線市場走弱", 1: "短線極度弱勢",
}
CORE2_HEAT_LABELS = {
    5: "中期市場極度強勢", 4: "中期市場樂觀", 3: "中期市場震盪",
    2: "中期市場走弱", 1: "中期極度弱勢",
}


# ---------- 小工具 ----------
def parse_float(value):
    """把 TWSE 回傳的字串數字（可能有逗號、正負號、空字串、'--'）轉成 float，失敗回傳 None"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace(",", "").strip()
    if s in ("", "--", "---", "X", "x", "N/A", "除息", "除權", "除權息"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def linear_slope(xs, ys):
    """簡單最小平方法算斜率，資料點不足2個回傳0"""
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def today_taipei_str():
    """回傳台灣時區的今天日期字串 YYYY-MM-DD"""
    tz = ZoneInfo("Asia/Taipei")
    return datetime.now(tz).date().isoformat()


def to_roc_date(iso_date_str):
    """把 YYYY-MM-DD 轉成 TPEx 需要的民國年格式 YYY/MM/DD"""
    y, m, d = iso_date_str.split("-")
    roc_year = int(y) - 1911
    return f"{roc_year}/{m}/{d}"


def to_compact_date(iso_date_str):
    """把 YYYY-MM-DD 轉成 TWSE歷史API需要的 YYYYMMDD 格式"""
    return iso_date_str.replace("-", "")


def strip_html_tags(s):
    """TWSE歷史API的漲跌符號欄位有時包在HTML標籤裡(如 <p style=...>+</p>)，去掉標籤只留文字"""
    if s is None:
        return ""
    return re.sub(r"<[^>]+>", "", s).strip()


def get_field(row, keys):
    """依序嘗試多個可能的欄位名稱，回傳第一個有值的"""
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return None


# ---------- 資料庫讀寫 ----------
def load_pool():
    default_pool = {"reference_check": {}, "trading_days": [], "stocks": {}}
    if not os.path.exists(POOL_PATH):
        return default_pool
    try:
        with open(POOL_PATH, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            print(f"警告：{POOL_PATH} 是空檔案，視為全新資料庫重新開始")
            return default_pool
        data = json.loads(content)
        # 確保基本結構齊全，避免舊版/手動編輯過的檔案缺欄位
        data.setdefault("reference_check", {})
        data.setdefault("trading_days", [])
        data.setdefault("stocks", {})
        return data
    except json.JSONDecodeError as e:
        print(f"警告：{POOL_PATH} JSON格式錯誤（{e}），視為全新資料庫重新開始")
        return default_pool


def save_pool(pool):
    os.makedirs(os.path.dirname(POOL_PATH), exist_ok=True)
    with open(POOL_PATH, "w", encoding="utf-8") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)


def save_result(result):
    os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def load_theme_mapping():
    """
    讀取「股票代號 -> 題材標籤」對照表(data/theme_mapping.json)。
    這是選填的補充標籤（例如想特別標注"AI概念"），不加也沒關係，
    因為已經有 industry_mapping 自動抓的官方產業別打底。
    格式範例: {"2330": ["AI伺服器供應鏈"], "2454": ["AI概念"]}
    """
    if not os.path.exists(THEME_MAPPING_PATH):
        return {}
    try:
        with open(THEME_MAPPING_PATH, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return {}
        return json.loads(content)
    except Exception as e:
        print(f"讀取 theme_mapping.json 失敗，將略過補充標籤：{e}")
        return {}


# ---------- 官方產業別：自動抓取 + 快取 ----------
def load_industry_cache():
    if not os.path.exists(INDUSTRY_MAPPING_PATH):
        return None
    try:
        with open(INDUSTRY_MAPPING_PATH, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return None
        return json.loads(content)
    except Exception:
        return None


def save_industry_cache(cache):
    os.makedirs(os.path.dirname(INDUSTRY_MAPPING_PATH), exist_ok=True)
    with open(INDUSTRY_MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_industry_and_names_from_twse():
    """
    抓取 TWSE ISIN 上市+上櫃證券總表，解析「產業別」與「股票名稱」。
    這是全上市櫃公司的官方分類（如：半導體業、航運業），免費、免Key。
    回傳 (industry_map, name_map) 兩個字典，key都是股票代號。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    industry_map = {}
    name_map = {}

    for url, label in [(ISIN_LISTED_URL, "上市"), (ISIN_OTC_URL, "上櫃")]:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.encoding = "big5"
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"class": "h4"})
            if table is None:
                print(f"警告：找不到{label}產業別資料表格，網頁結構可能已變動")
                continue

            count_before = len(industry_map)
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) != 7:
                    continue  # 跳過標題列、分類標題列(如"股票"/"特別股")
                code_name = cells[0].get_text(strip=True)
                parts = code_name.split("\u3000")  # 全形空格分隔代號與名稱
                if len(parts) != 2:
                    continue
                code, name = parts[0], parts[1]
                if not re.match(r"^\d{4}$", code):
                    continue  # 只取4位數股票代號的普通股
                if name:
                    name_map[code] = name
                industry = cells[4].get_text(strip=True)
                if industry:
                    industry_map[code] = industry
            print(f"{label}產業別解析完成，累計 {len(industry_map) - count_before} 檔")
        except Exception as e:
            print(f"抓取{label}產業別資料失敗：{e}")

    if not industry_map:
        print("警告：產業別資料解析結果為空（上市+上櫃皆失敗）")
        return None, None
    return industry_map, name_map


def get_industry_mapping(today):
    """
    產業別資料變動很慢，用快取機制：
    快取存在且未超過 INDUSTRY_REFRESH_DAYS 天 -> 直接用快取
    快取過期或不存在 -> 嘗試重新抓取，成功就更新快取
    重新抓取失敗 -> 退回用舊快取（有總比沒有好），都沒有就回傳空字典
    """
    cache = load_industry_cache()
    if cache and cache.get("generated_date"):
        try:
            days_old = (datetime.fromisoformat(today) - datetime.fromisoformat(cache["generated_date"])).days
            if 0 <= days_old < INDUSTRY_REFRESH_DAYS:
                return cache.get("mapping", {})
        except Exception:
            pass

    industry_fresh, names_fresh = fetch_industry_and_names_from_twse()
    if industry_fresh:
        save_industry_cache({"generated_date": today, "mapping": industry_fresh, "names": names_fresh or {}})
        return industry_fresh

    if cache:
        print("重新抓取產業別失敗，使用舊快取資料")
        return cache.get("mapping", {})

    print("沒有可用的產業別資料（首次抓取失敗），本次結果將不含產業別標籤")
    return {}


def get_stock_name_mapping(today):
    """
    取得「股票代號 -> 名稱」對照表，跟產業別共用同一份快取(同一次ISIN網頁抓取)，
    不會多打一次網路請求。主要給歷史資料回補用，因為FinMind的股價資料不含股票名稱。
    """
    cache = load_industry_cache()
    if cache and cache.get("generated_date"):
        try:
            days_old = (datetime.fromisoformat(today) - datetime.fromisoformat(cache["generated_date"])).days
            if 0 <= days_old < INDUSTRY_REFRESH_DAYS:
                return cache.get("names", {})
        except Exception:
            pass

    industry_fresh, names_fresh = fetch_industry_and_names_from_twse()
    if industry_fresh:
        save_industry_cache({"generated_date": today, "mapping": industry_fresh, "names": names_fresh or {}})
        return names_fresh or {}

    if cache:
        return cache.get("names", {})

    return {}


_twse_session = None


def get_twse_session():
    """
    建立一個共用的requests.Session，先訪問TWSE的報表網頁取得cookie，
    再用同一個session查詢歷史資料。有些網站的防護機制會要求先有
    「瀏覽過網頁」的session紀錄才放行後續的資料查詢，這是常見的繞過技巧。
    整個程式執行期間只會暖身一次(session會被快取重複使用)。
    """
    global _twse_session
    if _twse_session is not None:
        return _twse_session

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }
    try:
        session.get("https://www.twse.com.tw/zh/trading/historical/mi-index.html",
                     headers=headers, timeout=15)
        print("TWSE session暖身完成（已取得cookie）")
    except Exception as e:
        print(f"TWSE session暖身失敗（不影響後續，仍會嘗試直接查詢）：{e}")

    _twse_session = session
    return session


# ---------- 歷史資料回補專用：查詢過去任一交易日的上市資料 ----------
def fetch_twse_historical_day(date_str, max_retries=3):
    """
    抓取指定日期(YYYY-MM-DD)的上市個股全部交易資訊，用於歷史資料回補。
    回傳值：
      - list：正規化後的資料(可能是空list，代表這天判斷為非交易日/假日)
      - None：重試多次後仍抓取失敗(網路問題，非假日判斷)
    """
    import time

    params = {"response": "json", "date": to_compact_date(date_str), "type": "ALLBUT0999"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html",
    }
    session = get_twse_session()

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(TWSE_HISTORICAL_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception as je:
                print(f"{date_str} TWSE歷史資料回應不是合法JSON（{je}），第{attempt}次嘗試")
                time.sleep(3 * attempt)
                continue

            rows = data.get("data9")
            if not rows:
                stat = data.get("stat")
                # stat不是"OK"，比較能確定是查詢範圍或格式問題；
                # 但如果stat是"OK"卻沒有data9，很可能是短時間內連續查詢被節流/擋下，值得重試
                if stat == "OK" and attempt < max_retries:
                    print(f"{date_str} stat=OK但無data9，疑似被節流，第{attempt}次嘗試，稍後重試")
                    time.sleep(3 * attempt)
                    continue
                print(f"{date_str} TWSE歷史資料無交易紀錄(可能是假日)，stat={stat!r}")
                return []  # 空list：判斷為非交易日

            normalized = []
            for row in rows:
                if len(row) < 11:
                    continue
                code = str(row[0]).strip()
                name = str(row[1]).strip()
                trade_value = parse_float(row[4])
                close = parse_float(row[8])
                sign = strip_html_tags(row[9])
                diff = parse_float(row[10])

                if close is None or trade_value is None or trade_value <= 0:
                    continue

                if diff is None:
                    change = 0.0
                elif sign == "+":
                    change = diff
                elif sign == "-":
                    change = -diff
                else:
                    change = 0.0  # 平盤(空白)或無法判斷符號(如除權息當日的X)，視為0

                normalized.append({
                    "code": code, "name": name, "close": close,
                    "change": change, "trade_value": trade_value, "market": "上市",
                })
            return normalized
        except Exception as e:
            print(f"{date_str} 抓取TWSE歷史資料失敗（第{attempt}次嘗試）：{e}")
            time.sleep(3 * attempt)

    print(f"{date_str} TWSE歷史資料共嘗試{max_retries}次仍失敗")
    return None


FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"


def fetch_finmind_historical_day(date_str, max_retries=3):
    """
    改用 FinMind 開放資料 API 抓取歷史資料，用於歷史資料回補。
    這隻API是設計給程式化查詢用的，不像 www.twse.com.tw 那樣容易被
    當成爬蟲擋下來；而且不指定 data_id 時，一次呼叫就能拿到「當天全部股票」
    (上市+上櫃+興櫃合併)的資料，不用像TWSE/TPEx那樣分開查兩次。

    「全市場、不指定單一股票」的查法需要FinMind的註冊token才能使用，
    從環境變數 FINMIND_TOKEN 讀取(在GitHub Actions裡透過repository secret設定，
    不會出現在程式碼或commit紀錄裡)。

    回傳值：
      - list：正規化後的資料(可能是空list，代表這天判斷為非交易日/假日)
      - None：重試多次後仍抓取失敗(網路問題，非假日判斷)
    """
    import time

    token = os.environ.get("FINMIND_TOKEN", "")
    params = {"dataset": "TaiwanStockPrice", "start_date": date_str, "end_date": date_str}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        print("警告：環境變數 FINMIND_TOKEN 是空的，全市場查詢可能會被拒絕(400)")

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=30)
            print(f"FinMind請求：{date_str}，狀態碼：{resp.status_code}（第{attempt}次嘗試）")

            if resp.status_code == 402:
                print(f"FinMind API 已達當前額度上限（402），第{attempt}次嘗試")
                time.sleep(5 * attempt)
                continue

            if resp.status_code == 400:
                print(f"{date_str} FinMind回應400，內容：{resp.text[:300]!r}")

            resp.raise_for_status()
            try:
                payload = resp.json()
            except Exception as je:
                print(f"{date_str} FinMind回應不是合法JSON（{je}）")
                time.sleep(3 * attempt)
                continue

            rows = payload.get("data")
            if rows is None:
                print(f"{date_str} FinMind回應缺少data欄位，內容片段：{str(payload)[:200]!r}")
                time.sleep(3 * attempt)
                continue

            if len(rows) == 0:
                print(f"{date_str} FinMind回傳空資料(可能是假日)")
                return []  # 空list：判斷為非交易日

            normalized = []
            for row in rows:
                code = str(row.get("stock_id", "")).strip()
                if not re.match(r"^\d{4}$", code):
                    continue  # 只取4位數一般股票代號

                close = parse_float(row.get("close"))
                change = parse_float(row.get("spread"))  # FinMind的spread是價差(元)，非百分比
                trade_value = parse_float(row.get("Trading_money"))

                if close is None or change is None or trade_value is None or trade_value <= 0:
                    continue

                normalized.append({
                    "code": code, "name": None,  # FinMind不含股票名稱，之後用ISIN名稱對照表補上
                    "close": close, "change": change,
                    "trade_value": trade_value, "market": "上市/上櫃",
                })
            return normalized
        except Exception as e:
            print(f"{date_str} 抓取FinMind歷史資料失敗（第{attempt}次嘗試）：{e}")
            time.sleep(3 * attempt)

    print(f"{date_str} FinMind歷史資料共嘗試{max_retries}次仍失敗")
    return None


# ---------- 抓取資料 ----------
def fetch_stock_day_all(max_retries=3):
    import time

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(STOCK_DAY_ALL_URL, timeout=30)
            print(f"STOCK_DAY_ALL 狀態碼：{resp.status_code}（第{attempt}次嘗試）")
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception as je:
                print(f"警告：STOCK_DAY_ALL 回應不是合法JSON（{je}），回應前200字：{resp.text[:200]!r}")
                last_error = je
                time.sleep(2 * attempt)
                continue

            if not isinstance(data, list) or len(data) == 0:
                print("警告：STOCK_DAY_ALL 回傳空資料")
                last_error = ValueError("empty data")
                time.sleep(2 * attempt)
                continue

            return data
        except Exception as e:
            print(f"抓取 STOCK_DAY_ALL 失敗（第{attempt}次嘗試）：{e}")
            last_error = e
            time.sleep(2 * attempt)

    print(f"STOCK_DAY_ALL 共嘗試{max_retries}次仍失敗，本次執行中止。最後錯誤：{last_error}")
    return None


def fetch_taiex_index(max_retries=3):
    """
    抓取「發行量加權股價指數」(大盤)當日收盤指數與漲跌百分比。
    用 openapi.twse.com.tw 這個網域(跟STOCK_DAY_ALL同網域，已驗證穩定可用)，
    不用 www.twse.com.tw (那個網域對雲端IP有封鎖問題)。

    回傳值：
      - dict {"close": float, "pct_change": float}：成功
      - None：抓取失敗，或找不到對應資料列
    """
    import time

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(MI_INDEX_URL, timeout=30)
            print(f"MI_INDEX(大盤指數) 狀態碼：{resp.status_code}（第{attempt}次嘗試）")
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception as je:
                print(f"警告：MI_INDEX 回應不是合法JSON（{je}）")
                last_error = je
                time.sleep(2 * attempt)
                continue

            if not isinstance(data, list):
                print(f"警告：MI_INDEX 回傳格式異常，型態={type(data)}")
                last_error = ValueError("unexpected format")
                time.sleep(2 * attempt)
                continue

            for row in data:
                if row.get("指數") == "發行量加權股價指數":
                    close = parse_float(row.get("收盤指數"))
                    pct = parse_float(row.get("漲跌百分比"))
                    if close is not None and pct is not None:
                        return {"close": close, "pct_change": pct}
                    print(f"警告：找到大盤指數列，但欄位無法解析：{row}")
                    last_error = ValueError("cannot parse taiex row")
                    break

            print(f"警告：MI_INDEX 資料中找不到「發行量加權股價指數」這一列")
            last_error = ValueError("TAIEX row not found")
            time.sleep(2 * attempt)
        except Exception as e:
            print(f"抓取大盤指數失敗（第{attempt}次嘗試）：{e}")
            last_error = e
            time.sleep(2 * attempt)

    print(f"大盤指數共嘗試{max_retries}次仍失敗，本次「相對大盤強度」將無法計算。最後錯誤：{last_error}")
    return None


def fetch_tpex_daily_quotes(today, max_retries=3):
    """
    抓取 TPEx(上櫃) 每日收盤行情。若這次執行失敗或解析不到資料，
    回傳 None，上層會直接略過上櫃資料，不影響上市資料照常運作。

    加入重試機制：TPEx伺服器有時會在傳輸中途斷線("Response ended prematurely")，
    這通常是暫時性的防爬蟲/網路問題，重試幾次多半就能成功。
    """
    import time

    params = {"l": "zh-tw", "d": to_roc_date(today)}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.tpex.org.tw/",
        "Connection": "close",  # 避免keep-alive連線被伺服器中途斷開
    }

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(TPEX_DAILY_QUOTES_URL, params=params, headers=headers, timeout=30)
            print(f"TPEx請求網址：{resp.url}，狀態碼：{resp.status_code}（第{attempt}次嘗試）")
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception as je:
                print(f"警告：TPEx回應不是合法JSON（{je}），回應前200字：{resp.text[:200]!r}")
                last_error = je
                time.sleep(2 * attempt)
                continue

            if isinstance(data, dict):
                for key in ("aaData", "data", "tables"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break

            if not isinstance(data, list) or len(data) == 0:
                print(f"警告：TPEx每日收盤行情回傳空資料或格式異常，型態={type(data)}，內容片段={str(data)[:200]!r}")
                last_error = ValueError("empty or malformed data")
                time.sleep(2 * attempt)
                continue

            return data
        except Exception as e:
            print(f"抓取 TPEx 每日收盤行情失敗（第{attempt}次嘗試）：{e}")
            last_error = e
            time.sleep(2 * attempt)

    print(f"TPEx資料抓取共嘗試{max_retries}次仍失敗，本次結果將不含上櫃資料。最後錯誤：{last_error}")
    return None


# ---------- 正規化：把TWSE/TPEx原始格式統一成共用欄位 ----------
def normalize_twse_rows(raw_rows):
    normalized = []
    for row in raw_rows:
        code = row.get("Code")
        name = row.get("Name")
        close = parse_float(row.get("ClosingPrice"))
        change = parse_float(row.get("Change"))
        trade_value = parse_float(row.get("TradeValue"))
        if not code or close is None or change is None or trade_value is None:
            continue
        if trade_value <= 0:
            continue
        normalized.append({
            "code": code, "name": name, "close": close,
            "change": change, "trade_value": trade_value, "market": "上市",
        })
    return normalized


def normalize_tpex_rows(raw_rows):
    if not raw_rows:
        return []
    normalized = []
    unknown_logged = False
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        code = get_field(row, ["Code", "SecuritiesCompanyCode", "CompanyCode", "代號", "Symbol", "SecurityId"])
        name = get_field(row, ["Name", "CompanyName", "名稱", "StockName", "SecurityName"])
        close_raw = get_field(row, ["Close", "ClosingPrice", "收盤", "收盤價", "Close_x0020_"])
        change_raw = get_field(row, ["Change", "涨跌", "漲跌", "漲跌(元)", "Change_x0020_"])
        value_raw = get_field(row, ["TradingAmount", "TransactionAmount", "TradeValue", "成交金額", "成交金額(元)"])

        code_str = str(code).strip() if code is not None else None
        close = parse_float(close_raw)
        change = parse_float(change_raw)
        trade_value = parse_float(value_raw)

        if not code_str or close is None or change is None or trade_value is None:
            if not unknown_logged and row:
                print(f"警告：TPEx資料欄位無法完全辨識，該筆原始keys={list(row.keys())}")
                unknown_logged = True
            continue
        if not re.match(r"^\d{4}$", code_str):
            continue  # 只取4位數一般股票代號，排除權證/ETF/ETN
        if trade_value <= 0:
            continue

        normalized.append({
            "code": code_str, "name": name, "close": close,
            "change": change, "trade_value": trade_value, "market": "上櫃",
        })
    return normalized


# ---------- 判斷是否為新交易日（假日/國定假日排除） ----------
def is_new_trading_day(raw_rows, pool):
    """
    用台積電(2330)+聯發科(2454)雙重比對：
    只要任一檔的收盤價或成交量跟資料庫中「上次記錄」不同，就判定為新交易日。
    若兩檔都相同或都取不到資料，判定為非交易日（跳過）。
    """
    row_map = {row.get("Code"): row for row in raw_rows}
    prev_ref = pool.get("reference_check", {})
    new_ref = {}
    new_day = False

    for code in REFERENCE_CODES:
        row = row_map.get(code)
        if row is None:
            continue  # 該檔今天找不到資料（可能暫停交易），忽略，看另一檔
        close = parse_float(row.get("ClosingPrice"))
        volume = parse_float(row.get("TradeVolume"))
        if close is None or volume is None:
            continue
        new_ref[code] = {"close": close, "volume": volume}

        prev = prev_ref.get(code)
        if prev is None:
            # 第一次執行，沒有基準可比對，視為新交易日
            new_day = True
        elif prev.get("close") != close or prev.get("volume") != volume:
            new_day = True

    # 更新基準值（不管今天是否為新交易日，數值相同的話更新了也不影響下次比對）
    if new_ref:
        pool["reference_check"] = new_ref

    return new_day


# ---------- 建立當日候選清單 + 計算當日分數 ----------
def build_candidate_list(normalized_rows):
    """
    輸入已經過 normalize_twse_rows / normalize_tpex_rows 處理的統一格式清單，
    (每筆含 code/name/close/change/trade_value/market)，直接做篩選排序。
    """
    parsed = []
    for row in normalized_rows:
        close = row["close"]
        change = row["change"]
        prev_close = close - change
        pct_change = (change / prev_close * 100) if prev_close and prev_close > 0 else 0.0
        parsed.append({**row, "pct_change": pct_change})

    # 依成交金額排序取前 TOP_N（上市+上櫃合併排名）
    parsed.sort(key=lambda x: x["trade_value"], reverse=True)
    top_n = parsed[:TOP_N]

    # 篩選漲跌 >= 0（含平盤）
    candidates = [c for c in top_n if c["change"] >= 0]
    return candidates


CORE1_CHANGE_WEIGHT = 0.7   # 核心1當日分數：漲幅權重
CORE1_VALUE_SHARE_WEIGHT = 0.3   # 核心1當日分數：個股佔大盤成交比重權重


def compute_market_total_trade_value(combined_rows):
    """加總全市場(上市+上櫃合併，篩選前的完整清單)當日總成交金額，當作「大盤總成交金額」"""
    return sum(r["trade_value"] for r in combined_rows if r.get("trade_value"))


def compute_daily_scores(candidates, market_total_trade_value):
    """
    當日得分 = 當日漲幅(%) x 0.7 + (個股成交金額 / 大盤總成交金額 x 100) x 0.3

    第二項是「個股佔大盤當日總成交金額的比重(百分比數值)」，用相對於大盤的比例
    取代直接使用原始金額，避免大型股光靠量體基期大就主導分數(規模偏誤)。
    """
    if market_total_trade_value and market_total_trade_value > 0:
        for c in candidates:
            market_share_pct = c["trade_value"] / market_total_trade_value * 100
            c["market_share_pct"] = round(market_share_pct, 4)
            c["score"] = round(
                c["pct_change"] * CORE1_CHANGE_WEIGHT + market_share_pct * CORE1_VALUE_SHARE_WEIGHT, 4
            )
    else:
        # 大盤總成交金額異常(0或缺失)時的防呆：退回只看漲幅，避免除以0
        print("警告：大盤總成交金額無法計算或為0，本次核心1當日分數僅採用漲幅(市占比項略過)")
        for c in candidates:
            c["market_share_pct"] = None
            c["score"] = round(c["pct_change"] * CORE1_CHANGE_WEIGHT, 4)

    return candidates


# ---------- 更新滾動資料庫 ----------
def update_pool_with_today(pool, today, candidates):
    pool.setdefault("trading_days", [])
    pool.setdefault("stocks", {})

    if today not in pool["trading_days"]:
        pool["trading_days"].append(today)
    pool["trading_days"] = pool["trading_days"][-TRADING_DAYS_BUFFER:]

    for c in candidates:
        stock = pool["stocks"].setdefault(c["code"], {
            "name": c["name"],
            "history": {},
            "trade_value_history": {},
            "last_seen": None,
            "last_classification": None,
            "last_close": None,
            "last_pct_change": None,
            "market": None,
        })
        stock["name"] = c["name"]
        stock["history"][today] = c["score"]
        stock.setdefault("trade_value_history", {})[today] = c["trade_value"]
        stock["last_seen"] = today
        stock["last_close"] = c["close"]
        stock["last_pct_change"] = c["pct_change"]
        stock["market"] = c.get("market")


def prune_pool(pool):
    """20日滾動清理：逐股獨立判斷，最後上榜距今超過20交易日就整檔刪除"""
    trading_days = pool.get("trading_days", [])
    if not trading_days:
        return
    day_index = {d: i for i, d in enumerate(trading_days)}
    latest_idx = len(trading_days) - 1

    for code in list(pool["stocks"].keys()):
        stock = pool["stocks"][code]
        last_idx = day_index.get(stock.get("last_seen"))
        if last_idx is None:
            # 最後上榜日期已經超出緩衝範圍，直接視為過期
            del pool["stocks"][code]
            continue
        if (latest_idx - last_idx) > CORE2_DAYS:
            del pool["stocks"][code]
            continue
        # 順便把history內、不在目前trading_days緩衝範圍內的舊紀錄清掉
        stock["history"] = {d: s for d, s in stock["history"].items() if d in day_index}
        stock["trade_value_history"] = {
            d: v for d, v in stock.get("trade_value_history", {}).items() if d in day_index
        }

    # 熱度指標的每日檔數紀錄，也只保留在trading_days緩衝範圍內的
    if "market_breadth" in pool:
        pool["market_breadth"] = {d: c for d, c in pool["market_breadth"].items() if d in day_index}

    if "market_index_pct_change" in pool:
        pool["market_index_pct_change"] = {
            d: v for d, v in pool["market_index_pct_change"].items() if d in day_index
        }


# ---------- 熱度指標：全市場前50大成交金額中的上漲檔數 ----------
def compute_market_breadth_count(combined_rows):
    """
    全市場(上市+上櫃合併)依成交金額排序取前HEAT_BREADTH_TOP_N檔，
    計算其中「漲跌 >= 0（含平盤與上漲）」的檔數。
    這個定義跟系統其他地方(候選清單篩選規則)保持一致：平盤或上漲都算。
    這是獨立於候選清單/核心1/核心2選股邏輯之外的市場廣度指標。
    """
    valid = [r for r in combined_rows if r.get("trade_value") and r["trade_value"] > 0]
    valid.sort(key=lambda x: x["trade_value"], reverse=True)
    top_n = valid[:HEAT_BREADTH_TOP_N]
    up_count = sum(1 for r in top_n if r["change"] >= 0)
    return up_count


def update_market_breadth(pool, today, count):
    pool.setdefault("market_breadth", {})
    pool["market_breadth"][today] = count


def compute_heat_level(avg):
    for lower, level in HEAT_THRESHOLDS:
        if avg >= lower:
            return level
    return 1


def compute_heat_index(pool, days, labels):
    """
    取近N個交易日的「上漲檔數」平均值，對照燈號等級。
    資料不滿N日時，用現有天數計算(動態平均)；完全沒有資料則回傳None。
    """
    trading_days = pool.get("trading_days", [])
    window = trading_days[-days:]
    breadth = pool.get("market_breadth", {})
    values = [breadth[d] for d in window if d in breadth]

    if not values:
        return None

    avg = sum(values) / len(values)
    level = compute_heat_level(avg)

    return {
        "average": round(avg, 1),
        "level": level,
        "label": labels[level],
        "sample_days": len(values),
        "top_n": HEAT_BREADTH_TOP_N,
        "range": {
            "start": window[0] if window else None,
            "end": window[-1] if window else None,
            "days": len(window),
        },
    }


# ---------- 核心1：近5日累積分數 ----------
def compute_core1(pool):
    trading_days = pool.get("trading_days", [])
    window = trading_days[-CORE1_DAYS:]

    scored = []
    for code, stock in pool["stocks"].items():
        total = sum(stock["history"].get(d, 0) for d in window)
        if total > 0:
            scored.append({"code": code, "name": stock["name"], "score": total})

    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[:CORE1_TOPK]

    range_info = {
        "start": window[0] if window else None,
        "end": window[-1] if window else None,
        "days": len(window),
    }
    return top, range_info


# ---------- 核心2：近20日雙斜率趨勢 ----------
DECAY_RATE_CORE2 = 0.9  # 資金部位時間折現率：每距今1個交易日，權重乘上這個係數


def compute_core2(pool, core1_codes):
    """
    核心2：主力潛伏資金偵測。
    對每檔候選股「有出現的那幾天」的「實際成交金額」做時間折現
    (折現金額 = 成交金額 x DECAY_RATE_CORE2^距今交易日數)，
    再對折現後金額序列做線性迴歸算斜率。
    斜率同時吃進「出現得夠不夠近期/密集」與「資金量體夠不夠大」兩種資訊，
    斜率為正代表資金部位持續放大(不只是分數排名進步，是實際金額在增溫)。
    """
    trading_days = pool.get("trading_days", [])
    window = trading_days[-CORE2_DAYS:]
    n = len(window)
    if n == 0:
        return [], {"start": None, "end": None, "days": 0}

    day_x = {d: i + 1 for i, d in enumerate(window)}  # X軸：1~n的交易日序號
    today_idx = n

    candidates = []
    for code, stock in pool["stocks"].items():
        if code in core1_codes:
            continue

        trade_value_history = stock.get("trade_value_history", {})
        appear_dates = [d for d in window if d in trade_value_history]
        if len(appear_dates) < MIN_APPEARANCE_FOR_CORE2:
            continue

        xs = [day_x[d] for d in appear_dates]
        discounted_values = [
            trade_value_history[d] * (DECAY_RATE_CORE2 ** (today_idx - day_x[d]))
            for d in appear_dates
        ]
        slope = linear_slope(xs, discounted_values)

        if slope > 0:
            candidates.append({
                "code": code,
                "name": stock["name"],
                "discounted_slope": slope,
                "appearance_count": len(appear_dates),
            })

    candidates.sort(key=lambda x: x["discounted_slope"], reverse=True)
    top = candidates[:CORE2_TOPK]

    range_info = {"start": window[0], "end": window[-1], "days": n}
    return top, range_info


# ---------- 補充欄位：現價 / 今日漲幅 / 題材標籤 ----------
def enrich_with_extra_fields(entries, pool, theme_mapping, industry_mapping):
    for e in entries:
        stock = pool["stocks"].get(e["code"], {})
        e["price"] = stock.get("last_close")
        e["pct_change"] = stock.get("last_pct_change")
        e["market"] = stock.get("market")

        industry = industry_mapping.get(e["code"])   # 自動抓取的官方產業別
        extra_tags = theme_mapping.get(e["code"], [])  # 選填的補充標籤

        themes = []
        if industry:
            themes.append(industry)
        themes.extend(extra_tags)
        e["themes"] = themes
    return entries


def compute_sector_summary(entries, top_n=3):
    """
    依entries(核心1或核心2最終清單)裡每檔股票的官方產業別(themes的第一個標籤，
    沒有則算「未分類」)，統計族群分布，取前top_n大並附上佔比。
    這代表「資金目前主要流向哪些族群」，是核心清單的彙總視角，跟個股層級的
    themes欄位是分開的兩件事。
    """
    total = len(entries)
    if total == 0:
        return []

    counts = {}
    for e in entries:
        themes = e.get("themes") or []
        sector = themes[0] if themes else "未分類"
        counts[sector] = counts.get(sector, 0) + 1

    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [
        {"sector": name, "count": cnt, "percentage": round(cnt / total * 100, 1)}
        for name, cnt in ranked
    ]


# ---------- 優化指標：量能異常倍數 / 相對大盤強度 ----------
VOLUME_ANOMALY_WINDOW = 20  # 量能異常倍數的比較基準天數


def compute_volume_anomaly_ratio(stock, today, trading_days, window=VOLUME_ANOMALY_WINDOW):
    """
    量能異常倍數 = 今日成交金額 / 近window個交易日(不含今日)的平均成交金額。
    數值越大代表今天的成交金額比近期平均放大越多倍，用來抓「資金剛開始關注」的訊號。
    資料不足(該股歷史紀錄不足)時回傳 None，不強行計算。
    """
    history = stock.get("trade_value_history", {})
    today_value = history.get(today)
    if today_value is None:
        return None

    baseline_days = [d for d in trading_days[-(window + 1):] if d != today]
    baseline_values = [history[d] for d in baseline_days if d in history]

    if not baseline_values:
        return None  # 還沒有足夠的歷史資料可以當基準(例如剛好是這檔股票第一次上榜)

    baseline_avg = sum(baseline_values) / len(baseline_values)
    if baseline_avg <= 0:
        return None

    return round(today_value / baseline_avg, 2)


def enrich_with_optimization_metrics(entries, pool, taiex_pct_change, today, trading_days):
    """
    補上「相對大盤強度」與「量能異常倍數」這兩項優化指標。
    taiex_pct_change 為 None 時(大盤指數抓取失敗)，relative_strength 一併回傳 None，
    不會讓其他欄位或整個流程失敗。
    """
    for e in entries:
        if taiex_pct_change is not None and e.get("pct_change") is not None:
            e["relative_strength"] = round(e["pct_change"] - taiex_pct_change, 2)
        else:
            e["relative_strength"] = None

        stock = pool["stocks"].get(e["code"], {})
        e["volume_ratio"] = compute_volume_anomaly_ratio(stock, today, trading_days)
    return entries


# ---------- 升降標記 ----------
def compute_marks_and_update_classification(pool, core1_list, core2_list):
    marks = {}

    core1_codes = {c["code"] for c in core1_list}
    core2_codes = {c["code"] for c in core2_list}

    for code in core1_codes:
        prev = pool["stocks"][code].get("last_classification")
        if prev == "core2":
            marks[code] = "red_up"
        pool["stocks"][code]["last_classification"] = "core1"

    for code in core2_codes:
        prev = pool["stocks"][code].get("last_classification")
        if prev == "core1":
            marks[code] = "green_down"
        pool["stocks"][code]["last_classification"] = "core2"

    return marks


# ---------- 主流程 ----------
def main():
    override_date = os.environ.get("OVERRIDE_DATE", "").strip()
    if override_date:
        today = override_date
        print(f"=== 收到手動指定日期 OVERRIDE_DATE={today}，以此日期標記本次抓到的資料 ===")
    else:
        today = today_taipei_str()
        print(f"=== 開始執行，台灣時間日期：{today} ===")

    pool = load_pool()
    is_bootstrap = len(pool.get("trading_days", [])) == 0  # 資料庫完全空白 = 第一次啟動

    force = os.environ.get("FORCE_UPDATE", "").lower() == "true" or bool(override_date) or is_bootstrap

    # 安全閥：不管reference_check有沒有基準值可比對，週六日一律直接跳過。
    # 這是為了防止「reference_check剛好是空的(例如資料庫被重置過)+ TWSE API
    # 剛好回傳前一交易日的舊資料」這種組合，被誤判成「今天(週末)的新交易日」。
    # 但如果資料庫「完全是空的」(第一次啟動，還沒有任何一筆紀錄)，就自動放行，
    # 讓系統至少能抓到第一筆資料當作起點；之後只要有了第一筆正確資料，
    # 這道安全閥就會照常對之後的每一次執行生效，不會有反覆誤判的風險。
    weekday = datetime.fromisoformat(today).weekday()  # 0=Mon ... 5=Sat, 6=Sun
    if weekday >= 5 and not force:
        print(f"{today} 是週末（星期{'六' if weekday == 5 else '日'}），直接跳過，不處理")
        return
    if weekday >= 5 and is_bootstrap:
        print(f"{today} 是週末，但資料庫目前完全空白（第一次啟動），允許抓取第一筆資料當起點")

    raw_rows = fetch_stock_day_all()
    if raw_rows is None:
        print("無法取得資料，結束本次執行")
        return

    if not is_new_trading_day(raw_rows, pool) and not force:
        print(f"{today} 判定為非交易日（假日/國定假日/資料未更新），跳過本次處理")
        save_pool(pool)  # 基準值可能有更新，還是存一下
        return

    if force:
        reason = "資料庫為空白(第一次啟動)" if is_bootstrap else "FORCE_UPDATE 或 OVERRIDE_DATE 生效中"
        print(f"{today} 略過假日/重複資料/週末判斷（{reason}）")

    print(f"{today} 判定為交易日，開始處理")

    twse_normalized = normalize_twse_rows(raw_rows)
    tpex_raw = fetch_tpex_daily_quotes(today)
    tpex_normalized = normalize_tpex_rows(tpex_raw)
    tpex_raw_count = len(tpex_raw) if tpex_raw else 0
    print(f"上市正規化後 {len(twse_normalized)} 檔")
    print(f"上櫃原始回傳 {tpex_raw_count} 筆，正規化後 {len(tpex_normalized)} 檔"
          + ("" if tpex_raw is not None else "（上櫃資料本次抓取失敗，僅使用上市資料）"))

    combined_rows = twse_normalized + tpex_normalized

    market_total_trade_value = compute_market_total_trade_value(combined_rows)
    print(f"大盤總成交金額：{market_total_trade_value/1e8:,.1f} 億元")

    candidates = build_candidate_list(combined_rows)
    candidates = compute_daily_scores(candidates, market_total_trade_value)
    print(f"今日候選清單（上市+上櫃合併，成交金額前{TOP_N}+漲跌>=0）共 {len(candidates)} 檔")

    update_pool_with_today(pool, today, candidates)

    breadth_count = compute_market_breadth_count(combined_rows)
    update_market_breadth(pool, today, breadth_count)
    print(f"熱度指標：全市場成交金額前{HEAT_BREADTH_TOP_N}大中，今日上漲 {breadth_count} 檔")

    prune_pool(pool)

    core1_list, core1_range = compute_core1(pool)
    core1_codes = {c["code"] for c in core1_list}

    core2_list, core2_range = compute_core2(pool, core1_codes)

    marks = compute_marks_and_update_classification(pool, core1_list, core2_list)

    for c in core1_list:
        c["mark"] = marks.get(c["code"])
    for c in core2_list:
        c["mark"] = marks.get(c["code"])

    theme_mapping = load_theme_mapping()
    industry_mapping = get_industry_mapping(today)
    enrich_with_extra_fields(core1_list, pool, theme_mapping, industry_mapping)
    enrich_with_extra_fields(core2_list, pool, theme_mapping, industry_mapping)

    taiex_data = fetch_taiex_index()
    taiex_pct_change = taiex_data["pct_change"] if taiex_data else None
    if taiex_data is not None:
        print(f"大盤(加權指數)今日收盤：{taiex_data['close']}，漲跌：{taiex_data['pct_change']}%")
        pool.setdefault("market_index_pct_change", {})[today] = taiex_pct_change
    trading_days_snapshot = pool.get("trading_days", [])
    enrich_with_optimization_metrics(core1_list, pool, taiex_pct_change, today, trading_days_snapshot)
    enrich_with_optimization_metrics(core2_list, pool, taiex_pct_change, today, trading_days_snapshot)

    market_summary = {
        "taiex": taiex_data,  # {"close":..., "pct_change":...} 或 None
        "breadth": {"up_count": breadth_count, "top_n": HEAT_BREADTH_TOP_N},
    }

    core1_heat = compute_heat_index(pool, CORE1_DAYS, CORE1_HEAT_LABELS)
    core2_heat = compute_heat_index(pool, CORE2_DAYS, CORE2_HEAT_LABELS)
    if core1_heat:
        print(f"核心1熱度：平均{core1_heat['average']}檔／{HEAT_BREADTH_TOP_N} → {core1_heat['label']}")
    if core2_heat:
        print(f"核心2熱度：平均{core2_heat['average']}檔／{HEAT_BREADTH_TOP_N} → {core2_heat['label']}")

    core1_sectors = compute_sector_summary(core1_list, top_n=3)
    core2_sectors = compute_sector_summary(core2_list, top_n=3)
    if core1_sectors:
        summary_str = ", ".join(f"{s['sector']} {s['percentage']}%" for s in core1_sectors)
        print(f"核心1族群分布前3：{summary_str}")
    if core2_sectors:
        summary_str = ", ".join(f"{s['sector']} {s['percentage']}%" for s in core2_sectors)
        print(f"核心2族群分布前3：{summary_str}")

    save_pool(pool)

    result = {
        "update_date": today,
        "market_summary": market_summary,
        "core1": {"range": core1_range, "list": core1_list, "heat": core1_heat, "sector_summary": core1_sectors},
        "core2": {"range": core2_range, "list": core2_list, "heat": core2_heat, "sector_summary": core2_sectors},
    }
    save_result(result)

    print(f"核心1：{len(core1_list)} 檔，核心2：{len(core2_list)} 檔，處理完成")


if __name__ == "__main__":
    main()
