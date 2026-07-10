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
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# ---------- 路徑設定 ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)  # scripts/ 的上一層 = repo 根目錄
POOL_PATH = os.path.join(REPO_ROOT, "data", "stock_pool.json")
RESULT_PATH = os.path.join(REPO_ROOT, "docs", "result.json")
THEME_MAPPING_PATH = os.path.join(REPO_ROOT, "data", "theme_mapping.json")

STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

TOP_N = 30          # 成交金額排名取前幾檔
CORE1_DAYS = 5       # 核心1觀察天數
CORE2_DAYS = 20      # 核心2觀察天數
CORE1_TOPK = 15
CORE2_TOPK = 15
MIN_APPEARANCE_FOR_CORE2 = 2   # 核心2最低上榜次數門檻
TRADING_DAYS_BUFFER = 30       # trading_days 清單保留的緩衝天數（要大於20才能正確判斷滾動刪除）
REFERENCE_CODES = ["2330", "2454"]  # 用來判斷是否為新交易日的基準股


# ---------- 小工具 ----------
def parse_float(value):
    """把 TWSE 回傳的字串數字（可能有逗號、正負號、空字串、'--'）轉成 float，失敗回傳 None"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace(",", "").strip()
    if s in ("", "--", "X", "x", "N/A"):
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


# ---------- 資料庫讀寫 ----------
def load_pool():
    if not os.path.exists(POOL_PATH):
        return {"reference_check": {}, "trading_days": [], "stocks": {}}
    with open(POOL_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


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
    這份表需要人工維護，找不到檔案或找不到該股代號都回傳空清單，不影響其他功能。
    格式範例: {"2330": ["半導體", "AI伺服器供應鏈"], "2454": ["IC設計", "AI概念"]}
    """
    if not os.path.exists(THEME_MAPPING_PATH):
        return {}
    try:
        with open(THEME_MAPPING_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"讀取 theme_mapping.json 失敗，將略過題材標記：{e}")
        return {}


# ---------- 抓取資料 ----------
def fetch_stock_day_all():
    try:
        resp = requests.get(STOCK_DAY_ALL_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or len(data) == 0:
            print("警告：STOCK_DAY_ALL 回傳空資料")
            return None
        return data
    except Exception as e:
        print(f"抓取 STOCK_DAY_ALL 失敗：{e}")
        return None


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
def build_candidate_list(raw_rows):
    parsed = []
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

        prev_close = close - change
        pct_change = (change / prev_close * 100) if prev_close and prev_close > 0 else 0.0

        parsed.append({
            "code": code,
            "name": name,
            "close": close,
            "change": change,
            "pct_change": pct_change,
            "trade_value": trade_value,
        })

    # 依成交金額排序取前 TOP_N
    parsed.sort(key=lambda x: x["trade_value"], reverse=True)
    top_n = parsed[:TOP_N]

    # 篩選漲跌 >= 0（含平盤）
    candidates = [c for c in top_n if c["change"] >= 0]
    return candidates


def compute_daily_scores(candidates):
    """排名制：漲幅名次 + 成交金額名次 → 分數 = (N-漲幅名次+1) + (N-成交金額名次+1)"""
    n = len(candidates)
    if n == 0:
        return candidates

    by_change = sorted(candidates, key=lambda x: x["pct_change"], reverse=True)
    for i, c in enumerate(by_change):
        c["change_rank"] = i + 1

    by_value = sorted(candidates, key=lambda x: x["trade_value"], reverse=True)
    for i, c in enumerate(by_value):
        c["value_rank"] = i + 1

    for c in candidates:
        c["score"] = (n - c["change_rank"] + 1) + (n - c["value_rank"] + 1)

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
            "last_seen": None,
            "last_classification": None,
            "last_close": None,
            "last_pct_change": None,
        })
        stock["name"] = c["name"]
        stock["history"][today] = c["score"]
        stock["last_seen"] = today
        stock["last_close"] = c["close"]
        stock["last_pct_change"] = c["pct_change"]


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
def compute_core2(pool, core1_codes):
    trading_days = pool.get("trading_days", [])
    window = trading_days[-CORE2_DAYS:]
    n = len(window)
    if n == 0:
        return [], {"start": None, "end": None, "days": 0}

    day_x = {d: i + 1 for i, d in enumerate(window)}  # X軸：1~n的交易日序號

    candidates = []
    for code, stock in pool["stocks"].items():
        if code in core1_codes:
            continue

        appear_dates = [d for d in window if d in stock["history"]]
        if len(appear_dates) < MIN_APPEARANCE_FOR_CORE2:
            continue

        # 頻率斜率：連續20天的0/1序列
        freq_xs = list(range(1, n + 1))
        freq_ys = [1 if d in stock["history"] else 0 for d in window]
        freq_slope = linear_slope(freq_xs, freq_ys)

        # 強度斜率：只取有出現的那幾天，保留實際交易日間距(做法A)
        strength_xs = [day_x[d] for d in appear_dates]
        strength_ys = [stock["history"][d] for d in appear_dates]
        strength_slope = linear_slope(strength_xs, strength_ys)

        if freq_slope > 0 and strength_slope > 0:
            candidates.append({
                "code": code,
                "name": stock["name"],
                "freq_slope": freq_slope,
                "strength_slope": strength_slope,
            })

    m = len(candidates)
    if m == 0:
        range_info = {"start": window[0], "end": window[-1], "days": n}
        return [], range_info

    by_freq = sorted(candidates, key=lambda x: x["freq_slope"], reverse=True)
    for i, c in enumerate(by_freq):
        c["freq_rank"] = i + 1
    by_strength = sorted(candidates, key=lambda x: x["strength_slope"], reverse=True)
    for i, c in enumerate(by_strength):
        c["strength_rank"] = i + 1
    for c in candidates:
        c["combined_score"] = (m - c["freq_rank"] + 1) + (m - c["strength_rank"] + 1)

    candidates.sort(key=lambda x: x["combined_score"], reverse=True)
    top = candidates[:CORE2_TOPK]

    range_info = {"start": window[0], "end": window[-1], "days": n}
    return top, range_info


# ---------- 補充欄位：現價 / 今日漲幅 / 題材標籤 ----------
def enrich_with_extra_fields(entries, pool, theme_mapping):
    for e in entries:
        stock = pool["stocks"].get(e["code"], {})
        e["price"] = stock.get("last_close")
        e["pct_change"] = stock.get("last_pct_change")
        e["themes"] = theme_mapping.get(e["code"], [])
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
    today = today_taipei_str()
    print(f"=== 開始執行，台灣時間日期：{today} ===")

    pool = load_pool()

    raw_rows = fetch_stock_day_all()
    if raw_rows is None:
        print("無法取得資料，結束本次執行")
        return

    if not is_new_trading_day(raw_rows, pool):
        print(f"{today} 判定為非交易日（假日/國定假日/資料未更新），跳過本次處理")
        save_pool(pool)  # 基準值可能有更新，還是存一下
        return

    print(f"{today} 判定為交易日，開始處理")

    candidates = build_candidate_list(raw_rows)
    candidates = compute_daily_scores(candidates)
    print(f"今日候選清單（成交金額前{TOP_N}+漲跌>=0）共 {len(candidates)} 檔")

    update_pool_with_today(pool, today, candidates)
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
    enrich_with_extra_fields(core1_list, pool, theme_mapping)
    enrich_with_extra_fields(core2_list, pool, theme_mapping)

    save_pool(pool)

    result = {
        "update_date": today,
        "core1": {"range": core1_range, "list": core1_list},
        "core2": {"range": core2_range, "list": core2_list},
    }
    save_result(result)

    print(f"核心1：{len(core1_list)} 檔，核心2：{len(core2_list)} 檔，處理完成")


if __name__ == "__main__":
    main()
