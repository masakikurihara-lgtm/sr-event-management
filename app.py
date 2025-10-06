import streamlit as st
import pandas as pd
import requests
import time
import concurrent.futures
import pytz
from datetime import datetime
import io
import ftplib

# === 設定 ===
JST = pytz.timezone("Asia/Tokyo")

ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
FTP_FILE_PATH = "/mksoul-pro.com/showroom/file/event_database.csv"

EVENT_ID_START = 30000
EVENT_ID_END = 30500  # ⚙️テスト範囲を指定（広げる場合は段階的に）
MAX_WORKERS = 10  # 並列スレッド数
SAVE_INTERVAL = 20  # 20イベントごとに途中保存

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
}

# === FTP接続用 ===
def ftp_upload(file_path, content_bytes):
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["password"]
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(ftp_user, ftp_pass)
        with io.BytesIO(content_bytes) as f:
            ftp.storbinary(f"STOR {file_path}", f)

def ftp_download(file_path):
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["password"]
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(ftp_user, ftp_pass)
        buffer = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {file_path}", buffer.write)
            buffer.seek(0)
            return buffer.getvalue().decode("utf-8-sig")
        except Exception:
            return None


# === データ取得系関数 ===
def fetch_room_list_for_event(event_id):
    """イベントに参加しているルームを取得"""
    all_rooms = []
    for page in range(1, 31):  # 最大30ページ（約900件）
        url = f"https://www.showroom-live.com/api/event/room_list?event_id={event_id}&p={page}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            if res.status_code != 200:
                break
            data = res.json()
            rooms = data.get("list", [])
            if not rooms:
                break
            all_rooms.extend(rooms)
            time.sleep(0.05)
        except Exception:
            break
    return all_rooms


def fetch_event_detail(event_id, room_id):
    """イベント詳細を取得（contribution_ranking API）"""
    url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 200:
            data = res.json()
            event = data.get("event", {})
            return {
                "event_name": event.get("event_name"),
                "started_at": event.get("started_at"),
                "ended_at": event.get("ended_at"),
                "event_url": event.get("event_url"),
                "event_image": event.get("image"),
            }
    except Exception:
        pass
    return {}


# === メイン処理 ===
def fetch_and_merge_event_data():
    # 管理ルームリスト読込
    df_rooms = pd.read_csv(ROOM_LIST_URL, dtype=str)
    df_rooms["ルームID"] = df_rooms["ルームID"].astype(str)
    managed_rooms = df_rooms.set_index("ルームID")

    # 既存データ取得
    existing_csv = ftp_download(FTP_FILE_PATH)
    if existing_csv:
        df_existing = pd.read_csv(io.StringIO(existing_csv), dtype=str)
    else:
        df_existing = pd.DataFrame()

    all_records = []
    event_ids = list(range(EVENT_ID_START, EVENT_ID_END + 1))
    progress = st.progress(0)
    total = len(event_ids)

    def process_event(event_id):
        event_records = []
        room_list = fetch_room_list_for_event(event_id)
        for r in room_list:
            rid = str(r.get("room_id"))
            if rid not in managed_rooms.index:
                continue
            entry = r.get("event_entry", {})
            rank = r.get("rank") or "-"
            point = r.get("point") or 0
            quest_level = entry.get("quest_level", 0)
            detail = fetch_event_detail(event_id, rid)
            if not detail:
                continue

            # タイムスタンプを日付変換
            def fmt_time(ts):
                if not ts:
                    return ""
                try:
                    return datetime.fromtimestamp(int(ts), JST).strftime("%Y/%m/%d %H:%M")
                except Exception:
                    return ""

            rec = {
                "PR対象": "",
                "ライバー名": managed_rooms.loc[rid, "ルーム名"],
                "アカウントID": managed_rooms.loc[rid, "アカウントID"],
                "イベント名": detail.get("event_name"),
                "開始日時": fmt_time(detail.get("started_at")),
                "終了日時": fmt_time(detail.get("ended_at")),
                "順位": rank,
                "ポイント": point,
                "紐付け": "○",
                "URL": detail.get("event_url"),
                "レベル": quest_level,
                "event_id": str(event_id),
                "ルームID": rid,
                "イベント画像（URL）": detail.get("event_image"),
            }
            event_records.append(rec)
        return event_records

    # 並列実行
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_event, eid): eid for eid in event_ids}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_id)):
            try:
                eid = future_to_id[future]
                records = future.result()
                all_records.extend(records)
            except Exception:
                pass

            # 進捗更新
            progress.progress((i + 1) / total)

            # 途中保存（20件ごと）
            if (i + 1) % SAVE_INTERVAL == 0 and all_records:
                df_partial = pd.DataFrame(all_records)
                merged = pd.concat([df_existing, df_partial], ignore_index=True)
                merged.drop_duplicates(subset=["event_id", "ルームID"], keep="last", inplace=True)
                merged.sort_values("event_id", ascending=False, inplace=True)
                csv_bytes = merged.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                ftp_upload(FTP_FILE_PATH, csv_bytes)
                st.info(f"💾 途中保存完了 ({i+1}/{total})")

    # 完全保存
    if all_records:
        df_new = pd.DataFrame(all_records)
        merged = pd.concat([df_existing, df_new], ignore_index=True)
        merged.drop_duplicates(subset=["event_id", "ルームID"], keep="last", inplace=True)
        merged.sort_values("event_id", ascending=False, inplace=True)
        csv_bytes = merged.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        ftp_upload(FTP_FILE_PATH, csv_bytes)
        st.success("✅ データベース更新完了！")

        # ダウンロードボタン
        st.download_button(
            label="📥 ダウンロード（最新データ）",
            data=csv_bytes,
            file_name=f"event_database_{datetime.now(JST).strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    else:
        st.warning("該当データが取得できませんでした。")


# === Streamlit画面 ===
def main():
    st.title("🎯 SHOWROOM イベントデータベース構築ツール")
    st.caption("並列取得・進捗表示・途中保存対応版")

    if st.button("🚀 データ収集開始（実行）"):
        fetch_and_merge_event_data()


if __name__ == "__main__":
    main()