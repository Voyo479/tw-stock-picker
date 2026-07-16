# -*- coding: utf-8 -*-
"""
test_finmind_single_stock.py
獨立診斷腳本：測試FinMind「單一股票+日期範圍」查詢，
確認這個查詢模式在免費版(或你的FINMIND_TOKEN)下，是否真的可以正常使用，
不會像「全市場查詢」那樣被要求付費方案。

用法：透過GitHub Actions手動跑一次，或本機安裝requests後直接執行。
"""
import os
import requests

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"


def main():
    token = os.environ.get("FINMIND_TOKEN", "")
    print(f"是否有讀到FINMIND_TOKEN環境變數：{'是' if token else '否(將以未登入身份測試)'}")

    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": "2330",
        "start_date": "2026-06-01",
        "end_date": "2026-07-16",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    print(f"\n測試查詢：{params}")
    try:
        resp = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=20)
        print(f"狀態碼：{resp.status_code}")

        if resp.status_code == 200:
            payload = resp.json()
            rows = payload.get("data", [])
            print(f"✅ 成功！回傳 {len(rows)} 筆資料")
            if rows:
                print(f"第一筆：{rows[0]}")
                print(f"最後一筆：{rows[-1]}")
        else:
            print(f"❌ 非200狀態碼，回應內容：{resp.text[:500]}")
    except Exception as e:
        print(f"❌ 請求發生例外：{e}")


if __name__ == "__main__":
    main()
