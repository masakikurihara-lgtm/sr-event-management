import streamlit as st
import requests
import pandas as pd
import io
import time
import pytz
import ftplib
from datetime import datetime

# ====== 定数定義 ======
JST = pytz.timezone("Asia/Tokyo")

ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
EVENT_ARCHIVE_URL = "https://mksoul-pro.com/showroom/file/sr-event-archive.csv"
EVENT_DATABASE_PATH = "/mksoul-pro.com/showroom/file/event_database.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

#EVENT_ID_START = 30000
EVENT_ID_START = 40291
#EVENT_ID_END = 41000
EVENT_ID_END = 40292
ENTRY_CUTOFF = datetime(2023, 8, 18, 18, 0, tzinfo=JST)

# ====== FTPヘルパー ======
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


# ====== イベント参加情報収集 ======
def fetch_and_merge_event_data():
    # --- 各種ファイル読み込み ---
    df_rooms = pd.read_csv(ROOM_LIST_URL, dtype=str)
    df_archive = pd.read_csv(EVENT_ARCHIVE_URL, dtype=str)

    room_ids = set(df_rooms["ルームID"].astype(str))
    target_records = []

    st.info("📡 イベント情報を取得中...")
    total_checked = 0

    for eid in range(EVENT_ID_START, EVENT_ID_END + 1):
        for p in range(1, 31):
            url = f"https://www.showroom-live.com/api/event/room_list?event_id={eid}&p={p}"
            res = requests.get(url, headers=HEADERS, timeout=10)
            total_checked += 1
            if res.status_code != 200:
                break
            data = res.json()
            entries = data.get("list", [])
            if not entries:
                break

            for r in entries:
                rid = str(r.get("room_id"))
                entry_data = r.get("event_entry", {})
                entried_at = entry_data.get("entried_at", 0)
                if not rid or rid not in room_ids:
                    continue
                # --- 日付フィルタ ---
                try:
                    entry_dt = datetime.fromtimestamp(entried_at, JST)
                    if entry_dt < ENTRY_CUTOFF:
                        continue
                except Exception:
                    continue

                target_records.append({
                    "room_id": rid,
                    "event_id": eid,
                    "rank": r.get("rank"),
                    "point": r.get("point") or 0,
                    "quest_level": entry_data.get("quest_level", 0),
                    "entried_at": entry_dt.strftime("%Y-%m-%d %H:%M:%S")
                })

            if not data.get("next_page"):
                break
            time.sleep(0.05)

    if not target_records:
        st.warning("該当イベントデータが見つかりませんでした。")
        return pd.DataFrame()

    df_api = pd.DataFrame(target_records)

    # --- イベント名などをマージ ---
    df_api["event_id"] = df_api["event_id"].astype(str)
    df_archive["event_id"] = df_archive["event_id"].astype(str)

    merged = (
        df_api.merge(df_rooms, left_on="room_id", right_on="ルームID", how="left")
              .merge(df_archive, on="event_id", how="left")
    )

    # --- 🔧 列名を最終出力用に変換 ---
    merged = merged.rename(columns={
        "room_name": "ライバー名",        # 念のため残す（なければスキップされる）
        "ルーム名": "ライバー名",
        "アカウントID": "アカウントID",
        "event_name": "イベント名",
        "started_at": "開始日時",
        "ended_at": "終了日時",
        "rank": "順位",
        "point": "ポイント",
        "quest_level": "レベル"
    })

    # --- 🔧 固定列（存在しない場合は追加） ---
    for col in ["PR対象", "紐付け", "URL"]:
        if col not in merged.columns:
            merged[col] = ""

    # --- 🔧 表示順を安全に制御 ---
    expected_cols = [
        "PR対象", "ライバー名", "アカウントID", "イベント名", "開始日時", "終了日時",
        "順位", "ポイント", "紐付け", "URL", "レベル", "event_id"
    ]
    available_cols = [c for c in expected_cols if c in merged.columns]
    merged = merged[available_cols]

    st.success("✅ マージと列整形が完了しました！")
    st.write("📊 merged.shape:", merged.shape)



# ====== Streamlit UI ======
def main():
    st.title("🎯 SHOWROOM イベント参加データベース更新")

    if st.button("🚀 データベースを更新"):
        with st.spinner("イベント情報を収集中...（数分かかる場合があります）"):
            df = fetch_and_merge_event_data()

            if not df.empty:
                st.success(f"✅ {len(df)}件のイベント参加データを取得しました。")

                # --- 既存DBを読み込んで統合 ---
                existing_csv = ftp_download(EVENT_DATABASE_PATH)
                if existing_csv:
                    df_existing = pd.read_csv(io.StringIO(existing_csv), dtype=str)
                    merged_df = pd.concat([df_existing, df], ignore_index=True)
                    merged_df.drop_duplicates(subset=["event_id", "アカウントID"], keep="last", inplace=True)
                else:
                    merged_df = df

                # --- アップロード ---
                csv_bytes = merged_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                ftp_upload(EVENT_DATABASE_PATH, csv_bytes)

                st.download_button(
                    "📥 ダウンロード（更新後CSV）",
                    data=csv_bytes,
                    file_name=f"event_database_{datetime.now(JST).strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

                st.dataframe(df)

            else:
                st.warning("該当データがありませんでした。")


if __name__ == "__main__":
    main()
