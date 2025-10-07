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
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.4)"}

st.set_page_config(page_title="SHOWROOM：参加イベント履歴ビューア", layout="wide")


# ---------- Utility ----------
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
    """event_id から room_list API を呼び出し、指定 room_id の rank/point/quest_level を返す"""
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


# ---------- UI ----------
st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

room_input = st.text_input("表示するルームIDを入力", value="")
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

# ---------- ライバー名表示 ----------
room_name = get_room_name(room_id) if not is_admin else "（全データ表示中）"
link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{room_name}</a>'
st.markdown(
    f'<div style="font-size:22px;font-weight:700;color:#1a66cc;margin-bottom:12px;">{link_html} の参加イベント</div>',
    unsafe_allow_html=True,
)

# ---------- 日付整形＆ソート ----------
df["開始日時"] = df["開始日時"].apply(fmt_time)
df["終了日時"] = df["終了日時"].apply(fmt_time)
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
df.sort_values("__start_ts", ascending=False, inplace=True)

# ---------- 開催中判定 ----------
now_ts = int(datetime.now(JST).timestamp())
df["is_ongoing"] = df["__end_ts"].apply(lambda x: x and x > now_ts)

# ---------- 開催中イベント最新化 ----------
if not is_admin:
    ongoing = df[df["is_ongoing"]]
    for idx, row in ongoing.iterrows():
        event_id = row.get("event_id")
        stats = get_event_stats_from_roomlist(event_id, room_id)
        if stats:
            df.at[idx, "順位"] = stats.get("rank") or "-"
            df.at[idx, "ポイント"] = stats.get("point") or 0
            df.at[idx, "レベル"] = stats.get("quest_level") or 0
        time.sleep(0.3)

# ---------- 表示整形 ----------
disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL"]
df_show = df[disp_cols + ["is_ongoing"]].copy()


# ---------- 貢献ランク取得 ----------
def fetch_contribution_rank(event_id: str, room_id: str, top_n: int = 10):
    """貢献ランキングTOP10を取得"""
    url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    data = http_get_json(url)
    if not data:
        return []
    ranking = data.get("ranking") or data.get("contribution_ranking") or []
    return [
        {
            "順位": r.get("rank"),
            "名前": r.get("name"),
            "ポイント": f"{r.get('point', 0):,}"
        }
        for r in ranking[:top_n]
    ]


# ---------- 表示構築 ----------
def make_html_table(df):
    """貢献ランク列付きHTMLテーブルを生成"""
    html = """
    <style>
    .scroll-table {height:520px;overflow-y:auto;border:1px solid #ddd;border-radius:6px;}
    table{width:100%;border-collapse:collapse;font-size:14px;}
    thead th{position:sticky;top:0;background:#0b66c2;color:#fff;padding:8px;text-align:center;}
    tbody td{padding:8px;border-bottom:1px solid #f2f2f2;text-align:center;vertical-align:middle;}
    tr.ongoing{background:#fff8b3;}
    a.evlink{color:#0b57d0;text-decoration:none;}
    </style>
    <div class="scroll-table"><table><thead><tr>
    <th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th><th>貢献ランク</th>
    </tr></thead><tbody>
    """
    for _, r in df.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url = r.get("URL") or ""
        name = r.get("イベント名") or ""
        event_id = r.get("event_id") or ""
        link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name

        html += f'<tr class="{cls}">'
        html += f"<td>{link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td><td>{r['ポイント']}</td><td>{r['レベル']}</td><td>"
        html += f"<form action='?show_rank={event_id}' method='post'><button type='submit' style='background:#0b57d0;color:white;border:none;padding:4px 8px;border-radius:4px;cursor:pointer;'>▶ 貢献ランクを表示</button></form>"
        html += "</td></tr>"
    html += "</tbody></table></div>"
    return html


# ---------- 表示 ----------
query_params = st.experimental_get_query_params()
show_rank_event = query_params.get("show_rank", [None])[0]

st.markdown(make_html_table(df_show), unsafe_allow_html=True)
st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

# ---------- ランキング表示 ----------
if show_rank_event:
    with st.spinner("貢献ランキングを取得中..."):
        rank_data = fetch_contribution_rank(show_rank_event, room_id)
    st.markdown(f"### 🎯 イベントID {show_rank_event} の貢献ランキング")
    if rank_data:
        st.dataframe(pd.DataFrame(rank_data))
    else:
        st.info("ランキング情報が取得できません。")

# ---------- CSV出力 ----------
csv_bytes = df_show.drop(columns=["is_ongoing"]).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")
