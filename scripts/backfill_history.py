# -*- coding: utf-8 -*-
"""
backfill_history.py
一次性回補過去約20個交易日的歷史資料，讓核心2(近20日趨勢)跟熱度指標
不用等排程每天累積，馬上就有完整資料可以看。

資料來源：改用 FinMind 開放資料API（https://finmindtrade.com），一次呼叫
就能拿到「指定日期」全部股票(上市+上櫃+興櫃合併)的收盤行情，不用像日常排程
那樣分開查TWSE+TPEx兩個來源。這隻API是設計給程式化查詢用的，額度限制寬鬆
(未註冊300次/小時)，回補20天只需要20次呼叫，遠低於額度上限。

用法：手動在 GitHub Actions 執行一次即可(或本機執行後把 data/ 目錄下的
stock_pool.json、docs/result.json 一起commit上去)。

執行邏輯：
  1. 從「今天」往前推算候選日期(排除週六日)，逐一嘗試抓取
  2. 已經存在資料庫裡的日期會跳過，不重複抓取
  3. 遇到抓不到資料的日期，判斷為假日直接跳過，不中斷整個回補流程
  4. 補到累積滿20個「有效交易日」為止(或候選日期試完為止)
  5. 回補完成後，立即計算一次核心1/核心2/熱度指標，產生完整的result.json
"""

import sys
import os
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_data as ud

BACKFILL_TARGET_TRADING_DAYS = 5
LOOKBACK_CALENDAR_DAYS = 40   # 往前找幾個日曆天當候選(扣掉週末+假日後應該還是夠20個交易日)
REQUEST_INTERVAL_SECONDS = 2  # 每次請求之間的間隔，避免連續高頻請求被防爬蟲機制節流


def generate_candidate_dates(end_date, lookback_calendar_days):
    """
    從 end_date 往前推算候選日期(不含 end_date 本身)，只保留週一到週五，
    由「新到舊」排序回傳 —— 這樣才能優先抓到最近的交易日，
    一旦補滿 BACKFILL_TARGET_TRADING_DAYS 就停止，不會把預算浪費在40天前的舊資料上。
    (trading_days最終會依日期字串排序寫回，所以這裡的處理順序不影響最終儲存順序)
    """
    dates = []
    d = end_date
    for _ in range(lookback_calendar_days):
        d = d - timedelta(days=1)
        if d.weekday() < 5:  # 0=Mon ... 4=Fri
            dates.append(d.date().isoformat())
    return dates  # 新到舊


def main():
    today_str = ud.today_taipei_str()
    today_date = datetime.fromisoformat(today_str)

    candidate_dates = generate_candidate_dates(today_date, LOOKBACK_CALENDAR_DAYS)
    print(f"=== 開始回補歷史資料，今天日期：{today_str} ===")
    print(f"候選日期範圍：{candidate_dates[-1]}（最舊）～ {candidate_dates[0]}（最新），共{len(candidate_dates)}個候選日，由新到舊嘗試")

    pool = ud.load_pool()
    existing_days = set(pool.get("trading_days", []))
    print(f"回補前，資料庫已有 {len(existing_days)} 個交易日：{sorted(existing_days)}")

    # FinMind的股價資料不含股票名稱，先取得ISIN的代號->名稱對照表(跟產業別共用同一份快取)
    name_mapping = ud.get_stock_name_mapping(today_str)
    print(f"取得股票名稱對照表，共 {len(name_mapping)} 檔")

    filled_count = 0
    skipped_holiday = 0
    skipped_failed = 0

    for date_str in candidate_dates:
        if filled_count >= BACKFILL_TARGET_TRADING_DAYS:
            print(f"已補滿{BACKFILL_TARGET_TRADING_DAYS}個交易日，停止回補")
            break

        if date_str in existing_days:
            print(f"{date_str} 已存在於資料庫，跳過")
            continue

        combined = ud.fetch_finmind_historical_day(date_str)
        if combined is None:
            print(f"{date_str} FinMind抓取失敗(非假日判斷，是網路/API問題)，跳過此日")
            skipped_failed += 1
            time.sleep(REQUEST_INTERVAL_SECONDS)
            continue
        if len(combined) == 0:
            print(f"{date_str} 判斷為非交易日(假日)，跳過")
            skipped_holiday += 1
            time.sleep(REQUEST_INTERVAL_SECONDS)
            continue

        # 補上股票名稱(FinMind本身不含名稱)，找不到就用代號當名稱顯示，不影響排序計分
        for row in combined:
            row["name"] = name_mapping.get(row["code"], row["code"])

        print(f"{date_str} FinMind合併資料(上市+上櫃) {len(combined)} 檔")

        market_total_trade_value = ud.compute_market_total_trade_value(combined)

        candidates = ud.build_candidate_list(combined)
        candidates = ud.compute_daily_scores(candidates, market_total_trade_value)

        ud.update_pool_with_today(pool, date_str, candidates)
        ud.update_market_total_trade_value_history(pool, date_str, market_total_trade_value)

        breadth = ud.compute_market_breadth_count(combined)
        ud.update_market_breadth(pool, date_str, breadth)

        filled_count += 1
        print(f"{date_str} 回補完成：候選清單 {len(candidates)} 檔，前50大上漲(含平盤) {breadth} 檔")

        time.sleep(REQUEST_INTERVAL_SECONDS)

    # 重要：backfill是由新到舊往前推、再反轉成舊到新逐一處理，
    # 但trading_days欄位在update_pool_with_today內只是單純append，
    # 為了保險起見，這裡強制依日期字串排序(YYYY-MM-DD字串排序=時間排序)，
    # 確保後續核心1/核心2/熱度指標的window切片邏�輯不會拿到錯誤順序的資料。
    pool["trading_days"] = sorted(set(pool.get("trading_days", [])))

    ud.prune_pool(pool)
    ud.save_pool(pool)

    print(f"\n=== 回補統計 ===")
    print(f"新增 {filled_count} 個交易日，跳過假日 {skipped_holiday} 天，抓取失敗 {skipped_failed} 天")
    print(f"回補後，資料庫共有 {len(pool.get('trading_days', []))} 個交易日")

    if not pool.get("trading_days"):
        print("資料庫仍是空的，無法計算核心1/核心2，結束")
        return

    # 立即計算一次核心1/核心2/熱度指標，產生完整result.json，不用等下次排程
    core1_list, core1_range = ud.compute_core1(pool)
    core1_codes = {c["code"] for c in core1_list}
    core2_list, core2_range = ud.compute_core2(pool, core1_codes)

    marks = ud.compute_marks_and_update_classification(pool, core1_list, core2_list)
    for c in core1_list:
        c["mark"] = marks.get(c["code"])
    for c in core2_list:
        c["mark"] = marks.get(c["code"])

    theme_mapping = ud.load_theme_mapping()
    industry_mapping = ud.get_industry_mapping(today_str)
    ud.enrich_with_extra_fields(core1_list, pool, theme_mapping, industry_mapping)
    ud.enrich_with_extra_fields(core2_list, pool, theme_mapping, industry_mapping)

    # 回補情境沒有「歷史大盤指數」資料可用，相對大盤強度先留空(None)；
    # 量能異常倍數則可以正常計算，因為trade_value_history在回補過程中已經累積
    latest_trading_days = pool.get("trading_days", [])
    latest_date_for_metrics = latest_trading_days[-1] if latest_trading_days else today_str
    ud.enrich_with_optimization_metrics(core1_list, pool, None, latest_date_for_metrics, latest_trading_days)
    ud.enrich_with_optimization_metrics(core2_list, pool, None, latest_date_for_metrics, latest_trading_days)

    print(f"查詢核心1/核心2共 {len(core1_list) + len(core2_list)} 檔股票的處置狀態...")
    ud.enrich_with_disposition_info(core1_list, latest_date_for_metrics)
    ud.enrich_with_disposition_info(core2_list, latest_date_for_metrics)

    core1_heat = ud.compute_heat_index(pool, ud.CORE1_DAYS, ud.CORE1_HEAT_LABELS)
    core2_heat = ud.compute_heat_index(pool, ud.CORE2_DAYS, ud.CORE2_HEAT_LABELS)

    core1_sectors = ud.compute_sector_summary(core1_list, top_n=3)
    core2_sectors = ud.compute_sector_summary(core2_list, top_n=3)

    market_amount_stats = ud.compute_market_amount_stats(pool, latest_date_for_metrics)
    latest_breadth = pool.get("market_breadth", {}).get(latest_date_for_metrics)
    market_summary = {
        "taiex": None,  # 回補情境沒有歷史大盤指數資料
        "breadth": {"up_count": latest_breadth, "top_n": ud.HEAT_BREADTH_TOP_N},
        "amount_stats": market_amount_stats,
    }

    ud.save_pool(pool)

    latest_date = pool["trading_days"][-1]
    result = {
        "update_date": latest_date,
        "market_summary": market_summary,
        "core1": {"range": core1_range, "list": core1_list, "heat": core1_heat, "sector_summary": core1_sectors},
        "core2": {"range": core2_range, "list": core2_list, "heat": core2_heat, "sector_summary": core2_sectors},
    }
    ud.save_result(result)

    print(f"核心1：{len(core1_list)} 檔，核心2：{len(core2_list)} 檔")
    if core1_heat:
        print(f"核心1熱度：{core1_heat['label']}")
    if core2_heat:
        print(f"核心2熱度：{core2_heat['label']}")
    print("回補與計算全部完成")


if __name__ == "__main__":
    main()
