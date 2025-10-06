import streamlit as st
import requests
import pandas as pd
import io
from datetime import datetime
import pytz
import time

JST = pytz.timezone("Asia/Tokyo")

HEADERS = {"User-Agent": "Mozilla/5.0"}

ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
# イベント詳細 API： contribution_ranking
# room_list API： /api/event/room_list?event_id={eid}&p={page}

def fetch_event_detail(event_id, room_id):
    """
    event と room_id 指定でイベント詳細を取得。
    成功すれば dict に event_name, started_at (日時形式), ended_at, event_url, image を返す。
    失敗すれば {} を返す。
    """
    url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return {}
        j = resp.json()
        ev = j.get("event", {})
        if not ev:
            return {}
        # 日時変換
        started = ev.get("started_at")
        ended = ev.get("ended_at")
        started_str = ""
        ended_str = ""
        try:
            if started is not None:
                started_str = datetime.fromtimestamp(int(started), JST).strftime("%Y/%m/%d %H:%M")
            if ended is not None:
                ended_str = datetime.fromtimestamp(int(ended), JST).strftime("%Y/%m/%d %H:%M")
        except Exception:
            pass

        return {
            "event_name": ev.get("event_name"),
            "started_at": started_str,
            "ended_at": ended_str,
            "event_url": ev.get("event_url"),
            "event_image": ev.get("image")
        }
    except Exception:
        return {}

def fetch_room_list_for_event(event_id):
    """
    event の room_list API を全ページ取得して返す。
    戻り値は list of dict。
    """
    entries = []
    max_pages = 30
    for p in range(1, max_pages + 1):
        url = f"https://www.showroom-live.com/api/event/room_list?event_id={event_id}&p={p}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                break
            j = resp.json()
            page_list = j.get("list", [])
            if not page_list:
                break
            entries.extend(page_list)
            # next_page で判定もできるなら利用
            if not j.get("next_page"):
                break
        except Exception:
            break
        time.sleep(0.03)
    return entries

def fetch_and_merge_event_data():
    # 管理ライバー一覧取得
    df_rooms = pd.read_csv(ROOM_LIST_URL, dtype=str)
    # ルームID, アカウントID, ルーム名 が含まれていると仮定
    df_rooms["ルームID"] = df_rooms["ルームID"].astype(str)

    records = []

    EVENT_ID_START = 40291
    #EVENT_ID_START = 30000
    EVENT_ID_END = 40292
    #EVENT_ID_END = 41000

    # 例：対象期間の cutoff（不要なら省く）
    cutoff_dt = datetime(2023, 8, 18, 18, 0, tzinfo=JST)

    for eid in range(EVENT_ID_START, EVENT_ID_END + 1):
        room_list = fetch_room_list_for_event(eid)
        if not room_list:
            continue

        for r in room_list:
            rid = str(r.get("room_id"))
            if rid is None:
                continue
            # 管理ライバーのみ処理
            if rid not in set(df_rooms["ルームID"]):
                continue

            # event_entry 情報があれば使う
            entry = r.get("event_entry", {})
            entried_at = entry.get("entried_at")
            # 日付制限があればここでチェック
            if entried_at:
                try:
                    ent_dt = datetime.fromtimestamp(int(entried_at), JST)
                    if ent_dt < cutoff_dt:
                        continue
                except Exception:
                    pass

            # 基本情報取得
            rank = r.get("rank")
            point = r.get("point") or 0
            # 一部ケースでは r.get("quest_level") ではなく entry["quest_level"]
            quest_level = entry.get("quest_level", 0)

            # 追加でイベント詳細 API を利用して補完
            detail = fetch_event_detail(eid, rid)

            rec = {
                "PR対象": "",
                "ライバー名": None,
                "アカウントID": None,
                "イベント名": detail.get("event_name"),
                "開始日時": detail.get("started_at"),
                "終了日時": detail.get("ended_at"),
                "順位": rank,
                "ポイント": point,
                "備考": "",
                "紐付け": "○",
                "URL": detail.get("event_url"),
                "レベル": quest_level,
                "event_id": str(eid),
                "ルームID": rid,
                "イベント画像（URL）": detail.get("event_image")
            }
            # ルーム名・アカウントIDを rooms データから補填
            row_room = df_rooms[df_rooms["ルームID"] == rid]
            if not row_room.empty:
                row0 = row_room.iloc[0]
                rec["ライバー名"] = row0.get("ルーム名")
                rec["アカウントID"] = row0.get("アカウントID")

            records.append(rec)

    df = pd.DataFrame(records)
    return df

# Streamlit UI
def main():
    st.title("SHOWROOM 管理ライバー イベントデータ取得")

    if st.button("更新して取得"):
        with st.spinner("取得中..."):
            df = fetch_and_merge_event_data()
            if df is None or df.empty:
                st.warning("データ取得できませんでした。")
                return
            st.success(f"{len(df)} 件取得しました。")
            st.dataframe(df)

            csv = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("CSV ダウンロード", data=csv, file_name="event_database.csv", mime="text/csv")

if __name__ == "__main__":
    main()
