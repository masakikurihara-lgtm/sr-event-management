import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
import pytz

CSV_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"

# --- 共通設定 ---
JST = pytz.timezone("Asia/Tokyo")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-tool/1.0)"}

# --- ユーティリティ ---
def fmt_time_str(tstr):
    """ 'YYYY/MM/DD HH:MM'をdatetimeに変換 """
    try:
        return datetime.strptime(tstr, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
    except Exception:
        return None

def fetch_contribution_rank(event_id: str, room_id: str, top_n: int = 10):
    """貢献ランキングAPIからTOP10を取得"""
    url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
        if "ranking" in data:  # 古い仕様対応
            ranking = data["ranking"]
        elif "contribution_ranking" in data:
            ranking = data["contribution_ranking"]
        else:
            return []
        return [
            {
                "順位": r["rank"],
                "名前": r["name"],
                "ポイント": f"{r['point']:,}",
                "avatar": r.get("avatar_url", "")
            }
            for r in ranking[:top_n]
        ]
    except Exception:
        return []

# --- 開催中判定 ---
def is_ongoing(end_str):
    end_dt = fmt_time_str(end_str)
    if not end_dt:
        return False
    now = datetime.now(JST)
    return now < end_dt

# --- メインアプリ ---
st.set_page_config(page_title="ライバーイベント履歴ビューア", layout="wide")
st.title("🎤 SHOWROOM ライバーイベント履歴ビューア")

room_id = st.text_input("ルームIDを入力してください", "")
if st.button("表示する"):
    if not room_id.strip():
        st.warning("ルームIDを入力してください。")
        st.stop()

    # --- CSV読み込み ---
    df = pd.read_csv(CSV_URL, dtype=str)
    df = df[df["ルームID"].astype(str) == str(room_id)].copy()

    if df.empty:
        st.warning("該当するイベントデータが見つかりません。")
        st.stop()

    # --- 日付整形＆開催中判定 ---
    df["__start_dt"] = df["開始日時"].apply(fmt_time_str)
    df["__end_dt"] = df["終了日時"].apply(fmt_time_str)
    df["is_ongoing"] = df["__end_dt"].apply(is_ongoing)

    # --- 開始日時ソート（新しい順） ---
    df = df.sort_values("__start_dt", ascending=False)

    # --- ライバー名表示 ---
    name = df.iloc[0]["ライバー名"]
    link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{name}</a>'
    st.markdown(f"### 👤 {link_html} の参加イベント", unsafe_allow_html=True)
    st.write("")

    # --- 表示テーブル ---
    for _, row in df.iterrows():
        event_name = row["イベント名"]
        event_url = row["URL"]
        start = row["開始日時"]
        end = row["終了日時"]
        rank = row["順位"]
        point = row["ポイント"]
        level = row["レベル"]
        event_id = row["event_id"]
        ongoing = row["is_ongoing"]

        bg_color = "#fff8b3" if ongoing else "#ffffff"

        st.markdown(
            f"""
            <div style="background:{bg_color};padding:10px 15px;border-radius:6px;margin-bottom:6px;border:1px solid #ddd;">
                <b><a href="{event_url}" target="_blank" style="color:#0b57d0;">{event_name}</a></b><br>
                🕒 {start} ～ {end}　｜　🏆 順位: <b>{rank}</b>　💎 ポイント: <b>{point}</b>　🎯 レベル: <b>{level}</b>
            </div>
            """,
            unsafe_allow_html=True
        )

        # --- 貢献ランキング展開 ---
        with st.expander("▶ 貢献ランクを表示"):
            ranks = fetch_contribution_rank(event_id, room_id, top_n=10)
            if not ranks:
                st.info("貢献ランキング情報を取得できませんでした。")
            else:
                rank_df = pd.DataFrame(ranks)
                rank_df.index = rank_df.index + 1
                st.table(rank_df[["順位", "名前", "ポイント"]])

