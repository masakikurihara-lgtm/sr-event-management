import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz
import re # URL解析のためにreモジュールを追加

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
        try:
            # データベースから取得したゼロ埋めされていない文字列をパース（柔軟に対応するため、strptimeを使用）
            dt_obj = datetime.strptime(ts.strip(), "%Y/%m/%d %H:%M")
            # ゼロ埋め形式で再整形して返す
            return dt_obj.strftime("%Y/%m/%d %H:%M")
        except ValueError:
            # 時刻部分がない場合も考慮して、日付のみの形式も試す
            try:
                 dt_obj = datetime.strptime(ts.strip(), "%Y/%m/%d")
                 return dt_obj.strftime("%Y/%m/%d 00:00") # 時刻がない場合は 00:00 を付与
            except ValueError:
                 return ts.strip() # パースに失敗した場合は元の文字列を返す
    try:
        ts = int(float(ts))
        if ts > 20000000000:
            ts = ts // 1000
        # ★★★ 修正箇所: 月日と時刻をゼロ埋め形式 (%Y/%m/%d %H:%M) に変更 ★★★
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
        # 時刻込みの形式を優先してパース
        return int(datetime.strptime(val, "%Y/%m/%d %H:%M").timestamp())
    except Exception:
        # 日付のみの形式も試す
        try:
            return int(datetime.strptime(val, "%Y/%m/%d").timestamp())
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

# 貢献ランク取得関数は、今回は直接リンクを開くため使用しませんが、既存ロジックとして残します。
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
# df_allのルームID列をroom_idと同じ型(str)に変換してからフィルタリング
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

# ---------- 貢献ランクURL生成ロジック ----------
def generate_contribution_url(event_url, room_id):
    """
    イベントURLからURLキーを取得し、貢献ランキングのURLを生成する。
    例: https://www.showroom-live.com/event/mattari_fireworks249 -> mattari_fireworks249
    生成: https://www.showroom-live.com/event/contribution/mattari_fireworks249?room_id=ROOM_ID
    """
    if not event_url:
        return None
    # URLの最後の階層部分（URLキー）を正規表現で抽出
    match = re.search(r'/event/([^/]+)/?$', event_url)
    if match:
        url_key = match.group(1)
        return f"https://www.showroom-live.com/event/contribution/{url_key}?room_id={room_id}"
    return None


# ---------- 表示構築（HTMLテーブル）----------
def make_html_table(df, room_id):
    """貢献ランク列付きHTMLテーブルを生成し、リンクを別タブで開くように修正"""
    html = """
    <style>
    /* レイアウトの安定化とスクロール機能のCSS */
    .scroll-table {
        max-height: 520px;
        overflow-y: auto;
        border: 1px solid #ddd;
        border-radius: 6px;
        text-align: center;
        width: 100%;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
        table-layout: fixed;
    }
    thead th {
        position: sticky;
        top: 0;
        background: #0b66c2;
        color: #fff;
        padding: 5px;
        text-align: center;
        border: 1px solid #0b66c2;
        z-index: 10;
    }
    tbody td {
        padding: 5px;
        border-bottom: 1px solid #f2f2f2;
        text-align: center;
        vertical-align: middle;
        word-wrap: break-word;
    }
    /* カラム幅の指定 */
    table col:nth-child(1) { width: 46%; } /* イベント名 */
    table col:nth-child(2) { width: 11%; } /* 開始日時 */
    table col:nth-child(3) { width: 11%; } /* 終了日時 */
    table col:nth-child(4) { width: 6%; }  /* 順位 */
    table col:nth-child(5) { width: 9%; } /* ポイント */
    table col:nth-child(6) { width: 6%; }  /* レベル */
    table col:nth-child(7) { width: 11%; } /* 貢献ランク */
    
    tr.ongoing{background:#fff8b3;}
    a.evlink{color:#0b57d0;text-decoration:none;}

    /* 貢献ランクボタン風リンクのCSS */
    .rank-btn-link {
        background:#0b57d0;
        color:white !important; /* !importantでテーブルのリンク色を上書き */
        border:none;
        padding:4px 6px;
        border-radius:4px;
        cursor:pointer;
        text-decoration:none; /* 下線を消す */
        display: inline-block; /* ボタンのように振る舞う */
        white-space: nowrap; /* テキストの折り返しを防ぐ */
        font-size: 12px;
    }
    </style>
    <div class="scroll-table"><table>
    <colgroup>
        <col><col><col><col><col><col><col>
    </colgroup>
    <thead><tr>
    <th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th><th>貢献ランク</th>
    </tr></thead><tbody>
    """
    for _, r in df.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url = r.get("URL") or ""
        name = r.get("イベント名") or ""
        # ポイントをカンマ区切りにし、欠損値やハイフンの場合はそのまま表示
        point_raw = r.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')
        
        event_link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name
        
        # 貢献ランクURLを生成
        contrib_url = generate_contribution_url(url, room_id)
        
        if contrib_url:
            # <a>タグをボタン風に装飾し、target="_blank" で別タブで開く
            button_html = f'<a href="{contrib_url}" target="_blank" class="rank-btn-link">▶ 貢献ランクを確認</a>'
        else:
            button_html = "<span>URLなし</span>" # URLが取得できない場合はボタンを表示しない

        html += f'<tr class="{cls}">'
        html += f"<td>{event_link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td><td>{point}</td><td>{r['レベル']}</td><td>{button_html}</td>"
        html += "</tr>"
        
    html += "</tbody></table></div>"
    return html


# ---------- 表示 ----------

# HTMLテーブルで表示することで、レイアウトの安定化とスクロール機能を両立
st.markdown(make_html_table(df_show, room_id), unsafe_allow_html=True)
st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

# 貢献ランクの展開機能はHTMLテーブルの制約により削除

# ---------- CSV出力 ----------
csv_bytes = df_show.drop(columns=["is_ongoing"]).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")