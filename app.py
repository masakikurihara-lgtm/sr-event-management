import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

# ===== 設定 =====
JST = pytz.timezone("Asia/Tokyo")
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.1)"}

st.set_page_config(page_title="SHOWROOM イベント履歴ビューア", layout="wide")


# ===== 共通関数 =====
def http_get_json(url, params=None, retries=3, timeout=10, backoff=0.7):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 410):
                return None
            time.sleep(backoff * (i + 1))
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None


def fmt_time(ts):
    """Unix秒を 'YYYY/MM/DD HH:MM' に整形"""
    if pd.isna(ts) or ts == "":
        return ""
    try:
        ts = int(float(ts))
        if ts > 20000000000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, JST).strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(ts)


def load_event_db():
    """イベントデータベースCSVを取得"""
    try:
        r = requests.get(EVENT_DB_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
        return df
    except Exception as e:
        st.error(f"イベントデータベースの取得に失敗しました: {e}")
        return pd.DataFrame()


def get_latest_room_name(room_id):
    """最新ルーム名取得"""
    data = http_get_json(API_ROOM_PROFILE, params={"room_id": room_id})
    if data and "room_name" in data:
        return data["room_name"]
    return ""


def update_live_fields(event_id, room_id):
    """イベント開催中なら rank/point/quest_level 最新化"""
    data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": 1})
    if not data or "list" not in data:
        return None
    for e in data["list"]:
        if str(e.get("room_id")) == str(room_id):
            return {
                "順位": e.get("rank") or "-",
                "ポイント": e.get("point") or "0",
                "レベル": e.get("quest_level") or "0",
                "ライバー名": e.get("room_name") or "",
            }
    return None


# ===== メイン処理 =====
st.title("🎤 SHOWROOM イベント履歴ビューア")

room_id = st.text_input("ルームIDを入力してください", value="")

if st.button("表示する"):
    if not room_id.strip():
        st.warning("ルームIDを入力してください。")
        st.stop()

    df = load_event_db()
    if df.empty:
        st.stop()

    # 必須列確認
    required_cols = [
        "event_id",
        "URL",
        "ルームID",
        "イベント名",
        "開始日時",
        "終了日時",
        "順位",
        "ポイント",
        "レベル",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[df["ルームID"].astype(str) == str(room_id).strip()]
    if df.empty:
        st.warning("該当ルームのデータが見つかりません。")
        st.stop()

    # 最新ルーム名を取得
    live_name = get_latest_room_name(room_id)
    link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{live_name}</a>'
    st.markdown(
        f'<div style="font-size:22px; font-weight:bold; color:#2b5cff; margin:10px 0;">{link_html} の参加イベント</div>',
        unsafe_allow_html=True,
    )

    # 最新化処理（開催中イベント）
    now = datetime.now(JST)
    for idx, row in df.iterrows():
        try:
            end_ts = row["終了日時"]
            if end_ts:
                end_dt = datetime.strptime(fmt_time(end_ts), "%Y/%m/%d %H:%M")
                if now < end_dt:
                    upd = update_live_fields(row["event_id"], room_id)
                    if upd:
                        for k, v in upd.items():
                            if k in df.columns:
                                df.at[idx, k] = v
        except Exception:
            continue

    # 日付整形＆ソート
    df["開始日時"] = df["開始日時"].apply(fmt_time)
    df["終了日時"] = df["終了日時"].apply(fmt_time)

    # 両方とも降順
    df = df.sort_values(by=["開始日時"], ascending=False)

    # ===== 日付フィルタ（プルダウン降順） =====
    st.sidebar.header("📅 日付フィルタ")
    start_dates = sorted(df["開始日時"].dropna().unique().tolist(), reverse=True)
    end_dates = sorted(df["終了日時"].dropna().unique().tolist(), reverse=True)

    selected_start = st.sidebar.selectbox("開始日で絞り込み", ["すべて"] + start_dates)
    selected_end = st.sidebar.selectbox("終了日で絞り込み", ["すべて"] + end_dates)

    if selected_start != "すべて":
        df = df[df["開始日時"] == selected_start]
    if selected_end != "すべて":
        df = df[df["終了日時"] == selected_end]

    # ===== 表示テーブル =====
    def is_ongoing(row):
        try:
            end = datetime.strptime(row["終了日時"], "%Y/%m/%d %H:%M")
            return datetime.now(JST) < end
        except Exception:
            return False

    def make_html_table(df_show):
        cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
        html = """
        <style>
        .scroll-table {
            height: 480px;
            overflow-y: auto;
            border: 1px solid #ccc;
            border-radius: 6px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        thead th {
            position: sticky;
            top: 0;
            background-color: #1a66cc;
            color: white;
            text-align: center;
            padding: 8px;
        }
        td {
            padding: 8px;
            border-bottom: 1px solid #eee;
            text-align: center;
        }
        tr.highlight {
            background-color: #fff7cc;
        }
        </style>
        <div class='scroll-table'><table><thead><tr>
        """
        for c in cols:
            html += f"<th>{c}</th>"
        html += "</tr></thead><tbody>"

        for _, r in df_show.iterrows():
            ongoing = is_ongoing(r)
            tr_class = "highlight" if ongoing else ""
            ev_name = r["イベント名"] or ""
            url = r["URL"] or ""
            ev_html = f'<a href="{url}" target="_blank">{ev_name}</a>' if url else ev_name
            html += f"<tr class='{tr_class}'>"
            html += f"<td>{ev_html}</td>"
            html += f"<td>{r['開始日時']}</td>"
            html += f"<td>{r['終了日時']}</td>"
            html += f"<td>{r['順位']}</td>"
            html += f"<td>{r['ポイント']}</td>"
            html += f"<td>{r['レベル']}</td>"
            html += "</tr>"
        html += "</tbody></table></div>"
        return html

    st.markdown(make_html_table(df), unsafe_allow_html=True)
    st.caption("※黄色の行は開催中イベントです。")
