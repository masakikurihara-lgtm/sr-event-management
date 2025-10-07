import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

JST = pytz.timezone("Asia/Tokyo")

EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_EVENT_AND_SUPPORT = "https://www.showroom-live.com/api/room/event_and_support"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.2)"}

st.set_page_config(page_title="SHOWROOM：参加イベント履歴ビューア", layout="wide")


# ---------- Utility ----------
def http_get_json(url, params=None, retries=2, timeout=8, backoff=0.5):
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
    """Unix秒または'YYYY/MM/DD HH:MM'文字列を共通化"""
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


def get_event_and_support(room_id):
    data = http_get_json(API_ROOM_EVENT_AND_SUPPORT, params={"room_id": room_id})
    if not data:
        return None
    rank = data.get("rank") or "-"
    point = data.get("point") or 0
    quest = data.get("quest_level") or 0
    return {"rank": rank, "point": point, "quest_level": quest}


# ---------- UI ----------
st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

with st.sidebar:
    room_input = st.text_input("表示するルームIDを入力", value="")
    st.caption("管理者用：mksp154851 で全件表示")
    if st.button("表示する"):
        do_show = True
    else:
        do_show = False

if not do_show:
    st.info("ルームIDを入力して「表示する」を押してください。")
    st.stop()

room_id = room_input.strip()
if room_id == "":
    st.warning("ルームIDを入力してください。")
    st.stop()

with st.spinner("イベントDBを取得中..."):
    df_all = load_event_db(EVENT_DB_URL)
if df_all.empty:
    st.stop()

is_admin = (room_id == "mksp154851")
df = df_all if is_admin else df_all[df_all["ルームID"].astype(str) == str(room_id)].copy()
if df.empty:
    st.warning("該当データが見つかりません。")
    st.stop()

# 最新ルーム名（ラベル表示）
room_name = get_room_name(room_id) if not is_admin else "（全データ表示中）"
link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{room_name}</a>'
st.markdown(f'<div style="font-size:20px;font-weight:700;color:#1a66cc;margin-bottom:8px;">{link_html} の参加イベント</div>', unsafe_allow_html=True)

# --- 日付整形＆ソート ---
df["開始日時"] = df["開始日時"].apply(fmt_time)
df["終了日時"] = df["終了日時"].apply(fmt_time)
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
df.sort_values("__start_ts", ascending=False, inplace=True)

# --- 日付フィルタ ---
st.sidebar.markdown("---")
st.sidebar.subheader("日付フィルタ")

start_dates = df["開始日時"].dropna().unique().tolist()
start_dates_sorted = sorted(start_dates, key=lambda x: parse_to_ts(x) or 0, reverse=True)
selected_start = st.sidebar.selectbox("開始日", ["すべて"] + start_dates_sorted)

end_dates = df["終了日時"].dropna().unique().tolist()
end_dates_sorted = sorted(end_dates, key=lambda x: parse_to_ts(x) or 0, reverse=True)
selected_end = st.sidebar.selectbox("終了日", ["すべて"] + end_dates_sorted)

if selected_start != "すべて":
    df = df[df["開始日時"] == selected_start]
if selected_end != "すべて":
    df = df[df["終了日時"] == selected_end]

if df.empty:
    st.info("条件に該当するデータはありません。")
    st.stop()

# --- 開催中判定 ---
now_ts = int(datetime.now(JST).timestamp())
df["is_ongoing"] = df["__end_ts"].apply(lambda x: x and x > now_ts)

# --- 開催中イベント最新化 ---
ongoing = df[df["is_ongoing"]]
if not ongoing.empty and not is_admin:
    st.info(f"開催中イベント {len(ongoing)} 件を最新化します...")
    new = get_event_and_support(room_id)
    if new:
        for idx in ongoing.index:
            df.at[idx, "順位"] = new["rank"]
            df.at[idx, "ポイント"] = new["point"]
            df.at[idx, "レベル"] = new["quest_level"]
        st.success("最新化完了。")

# --- 表示整形 ---
disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL"]
df_show = df[disp_cols].copy()

# --- テーブル出力（HTML） ---
def make_html_table(df):
    html = """
    <style>
    .scroll-table {height:520px;overflow-y:auto;border:1px solid #ddd;border-radius:6px;}
    table{width:100%;border-collapse:collapse;font-size:14px;}
    thead th{position:sticky;top:0;background:#0b66c2;color:#fff;padding:8px;text-align:center;}
    tbody td{padding:8px;border-bottom:1px solid #f2f2f2;text-align:center;}
    tr.ongoing{background:#fff7cc;}
    a.evlink{color:#0b57d0;text-decoration:none;}
    </style>
    <div class="scroll-table"><table><thead><tr>
    <th>イベント名</th><th>開始日時</th><th>終了日時</th><th>順位</th><th>ポイント</th><th>レベル</th>
    </tr></thead><tbody>
    """
    for _, r in df.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url = r.get("URL") or ""
        name = r.get("イベント名") or ""
        link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name
        html += f'<tr class="{cls}">'
        html += f"<td>{link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td><td>{r['順位']}</td><td>{r['ポイント']}</td><td>{r['レベル']}</td></tr>"
    html += "</tbody></table></div>"
    return html

st.markdown(make_html_table(df_show), unsafe_allow_html=True)
st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

# --- CSV出力 ---
csv_bytes = df_show.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")
