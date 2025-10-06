import streamlit as st
import requests
import pandas as pd
import io
from datetime import datetime
import pytz

JST = pytz.timezone("Asia/Tokyo")

HEADERS = {"User-Agent": "Mozilla/5.0"}

# --- 新: イベント詳細取得API ---
def fetch_event_detail(event_id, room_id):
    url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            return {}
        data = res.json()
        ev = data.get("event", {})
        if not ev:
            return {}
        return {
            "event_name": ev.get("event_name"),
            "started_at": datetime.fromtimestamp(ev.get("started_at"), JST).strftime("%Y/%m/%d %H:%M"),
            "ended_at": datetime.fromtimestamp(ev.get("ended_at"), JST).strftime("%Y/%m/%d %H:%M"),
            "event_url": ev.get("event_url"),
            "event_image": ev.get("image")
        }
    except Exception:
        return {}

# --- メイン処理 ---
def fetch_and_merge_event_data():
    # 例: サンプルとして固定ルームリスト（本来はroom_list.csvを参照）
    managed_rooms = pd.read_csv("https://mksoul-pro.com/showroom/file/room_list.csv", dtype=str)

    records = []

    # 仮: チェック対象の event_id を固定
    EVENT_ID_START, EVENT_ID_END = 40291, 40292

    for event_id in range(EVENT_ID_START, EVENT_ID_END + 1):
        for _, row in managed_rooms.iterrows():
            rid = row["ルームID"]
            account_id = row["アカウントID"]
            room_name = row["ルーム名"]

            # --- イベント詳細を取得 ---
            detail = fetch_event_detail(event_id, rid)
            if not detail:
                continue  # 参加していない場合はスキップ

            # 仮: 順位やポイントは API(room_list) から取得するのが本筋
            # 今回はダミー（0）で埋めておく
            rank = 0
            point = 0
            level = 0

            records.append({
                "PR対象": "",
                "ライバー名": room_name,
                "アカウントID": account_id,
                "イベント名": detail.get("event_name"),
                "開始日時": detail.get("started_at"),
                "終了日時": detail.get("ended_at"),
                "順位": rank,
                "ポイント": point,
                "紐付け": "○",
                "URL": detail.get("event_url"),
                "レベル": level,
                "event_id": str(event_id),
                "ルームID": rid,
                "イベント画像（URL）": detail.get("event_image")
            })

    df = pd.DataFrame(records)
    return df

# --- Streamlit UI ---
def main():
    st.title("🎯 SHOWROOM イベント参加データベース更新")

    df = fetch_and_merge_event_data()

    if df.empty:
        st.warning("データが取得できませんでした。")
        return

    st.success("✅ イベントデータを取得しました！")
    st.dataframe(df)

    # CSV出力
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("📥 CSVダウンロード", data=csv_bytes, file_name="event_database.csv", mime="text/csv")

if __name__ == "__main__":
    main()
