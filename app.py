import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

# API_CONTRIBUTION は使用しませんが、定義は残します
API_CONTRIBUTION = "https://www.showroom-live.com/api/event/contribution_ranking"
JST = pytz.timezone("Asia/Tokyo")
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.4)"}

st.set_page_config(page_title="SHOWROOM：参加イベント履歴ビューア", layout="wide")

# ---------- Utility (既存ロジックを保持) ----------
def http_get_json(url, params=None, retries=3, timeout=8, backoff=0.6):
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
    if ts is None or ts == "" or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    if isinstance(ts, str) and "/" in ts:
        return ts.strip()
    try:
        ts = int(float(ts))
        if ts > 20000000000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, JST).strftime("%Y/%m/%d %H:%M")
    except Exception:
        return ""

def parse_to_ts(val):
    if val is None or val == "":
        return None
    try:
        ts = int(float(val))
        if ts > 20000000000:
            ts = ts // 1000
        return ts
    except Exception:
        pass
    try:
        return int(datetime.strptime(val, "%Y/%m/%d %H:%M").timestamp())
    except Exception:
        return None

def load_event_db(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
    except Exception as e:
        st.error(f"イベントDB取得失敗: {e}")
        return pd.DataFrame()

    df.columns = [c.replace("_fmt", "").strip() for c in df.columns]
    for c in ["event_id", "URL", "ルームID", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "ライバー名"]:
        if c not in df.columns:
            df[c] = ""
    return df

def get_room_name(room_id):
    data = http_get_json(API_ROOM_PROFILE, params={"room_id": room_id})
    if data and isinstance(data, dict):
        return data.get("room_name") or data.get("name") or ""
    return ""

def get_event_stats_from_roomlist(event_id, room_id):
    data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": 1})
    if not data or "list" not in data:
        return None
    for entry in data["list"]:
        if str(entry.get("room_id")) == str(room_id):
            return {
                "rank": entry.get("rank") or entry.get("position"),
                "point": entry.get("point") or entry.get("event_point") or entry.get("total_point"),
                "quest_level": entry.get("quest_level") or entry.get("event_entry", {}).get("quest_level"),
            }
    return None

# fetch_contribution_rank は HTML テーブルでは使えません。削除または無視します。

# ---------- UI: 入力ボタンの安定化 ----------
if "do_show" not in st.session_state:
    st.session_state["do_show"] = False

st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

# 課題①：ボタンの配置変更 (入力エリアの下に配置)
room_input = st.text_input("表示するルームIDを入力", value="")
if st.button("表示する"): # 「リセット」ボタンは削除
    st.session_state["do_show"] = True

if not st.session_state["do_show"]:
    st.info("ルームIDを入力して「表示する」を押してください。")
    st.stop()

room_id = room_input.strip()
if room_id == "":
    st.warning("ルームIDを入力してください。")
    st.stop()

# ---------- データ取得・整形 ----------
with st.spinner("イベントDBを取得中..."):
    df_all = load_event_db(EVENT_DB_URL)
if df_all.empty:
    st.stop()

is_admin = (room_id == "mksp154851")
df = df_all if is_admin else df_all[df_all["ルームID"].astype(str) == str(room_id)].copy()
if df.empty:
    st.warning("該当データが見つかりません。")
    st.stop()

# ライバー名表示（ラベル）
room_name = get_room_name(room_id) if not is_admin else "（全データ表示中）"
link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{room_name}</a>'
st.markdown(f'<div style="font-size:22px;font-weight:700;color:#1a66cc;margin-bottom:12px;">{link_html} の参加イベント</div>', unsafe_allow_html=True)

# 日付整形＆ソート
df["開始日時"] = df["開始日時"].apply(fmt_time)
df["終了日時"] = df["終了日時"].apply(fmt_time)
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
df.sort_values("__start_ts", ascending=False, inplace=True)

# 開催中判定
now_ts = int(datetime.now(JST).timestamp())
df["is_ongoing"] = df["__end_ts"].apply(lambda x: True if (x and x > now_ts) else False)

# 最新化（開催中のものだけ自動で最新化）
if not is_admin:
    ongoing = df[df["is_ongoing"]].copy()
    for idx, row in ongoing.iterrows():
        event_id = row.get("event_id")
        stats = get_event_stats_from_roomlist(event_id, room_id)
        if stats:
            df.at[idx, "順位"] = stats.get("rank") or "-"
            df.at[idx, "ポイント"] = stats.get("point") or 0
            df.at[idx, "レベル"] = stats.get("quest_level") or 0
        time.sleep(0.25)

# 表示用列
# 貢献ランクボタンの列はレイアウト崩れ防止のため削除
disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL", "event_id"]
df_show = df[disp_cols + ["is_ongoing"]].copy()
df_show = df_show.reset_index(drop=True)

# ---------- HTMLテーブルの生成と表示 ----------

# 課題②：HTMLテーブルでレイアウト崩れを解消し、スクロールに収める
def make_scrollable_html_table(df):
    html = """
    <style>
    /* テーブル全体のスクロールコンテナ */
    .scroll-table {
        max-height: 520px; 
        overflow-y: auto; 
        border: 1px solid #ddd; 
        border-radius: 6px;
        width: 100%; /* 幅を確実に確保 */
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
        table-layout: fixed; /* カラム幅を固定 */
    }
    thead th {
        position: sticky;
        top: 0;
        background: #0b66c2;
        color: #fff;
        padding: 8px;
        text-align: center;
        border: 1px solid #0b66c2;
        z-index: 10;
    }
    tbody td {
        padding: 8px;
        border-bottom: 1px solid #f2f2f2;
        text-align: center;
        vertical-align: middle;
        word-wrap: break-word;
    }
    /* カラム幅の指定 */
    table col:nth-child(1) { width: 30%; } /* イベント名 */
    table col:nth-child(2) { width: 18%; } /* 開始日時 */
    table col:nth-child(3) { width: 18%; } /* 終了日時 */
    table col:nth-child(4) { width: 8%; } /* 順位 */
    table col:nth-child(5) { width: 16%; } /* ポイント */
    table col:nth-child(6) { width: 10%; } /* レベル */
    
    tr.ongoing {background: #fff8b3;}
    a.evlink {color: #0b57d0; text-decoration: none;}
    </style>
    <div class="scroll-table">
    <table>
        <colgroup>
            <col><col><col><col><col><col>
        </colgroup>
        <thead>
            <tr>
                <th>イベント名</th><th>開始日時</th><th>終了日時</th><th>順位</th><th>ポイント</th><th>レベル</th>
            </tr>
        </thead>
    <tbody>
    """
    
    for _, r in df.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url = r.get("URL") or ""
        name = r.get("イベント名") or ""
        
        # ポイントをカンマ区切りにし、欠損値やハイフンの場合はそのまま表示
        point_raw = r.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')

        link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name

        html += f'<tr class="{cls}">'
        html += f"<td>{link}</td>"
        html += f"<td>{r['開始日時']}</td>"
        html += f"<td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td>"
        html += f"<td>{point}</td>"
        html += f"<td>{r['レベル']}</td>"
        html += "</tr>"

    html += "</tbody></table></div>"
    return html

st.markdown(make_scrollable_html_table(df_show), unsafe_allow_html=True)
st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

# ---------- CSV出力 ----------
csv_bytes = df_show.drop(columns=["is_ongoing", "event_id", "URL"]).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")