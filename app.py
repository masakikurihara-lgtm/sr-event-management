import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

# API_CONTRIBUTION のインポートを確認
API_CONTRIBUTION = "https://www.showroom-live.com/api/event/contribution_ranking"
# URL定義
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
    # 必要列の保証
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

def fetch_contribution_rank(event_id: str, room_id: str, top_n: int = 10):
    url = f"{API_CONTRIBUTION}?event_id={event_id}&room_id={room_id}"
    data = http_get_json(url)
    if not data:
        return []
    ranking = data.get("ranking") or data.get("contribution_ranking") or []
    out = []
    for r in ranking[:top_n]:
        out.append({
            "順位": r.get("rank"),
            "名前": r.get("name"),
            "ポイント": f"{r.get('point', 0):,}"
        })
    return out

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
        # 小休止（過負荷回避）
        time.sleep(0.25)

# 表示用列
disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL", "event_id"]
df_show = df[disp_cols + ["is_ongoing"]].copy()
df_show = df_show.reset_index(drop=True)

# 課題②：表のレイアウト崩れ修正
# ヘッダと行の colums の幅比率を完全に一致させる (合計13)
# [イベント名:3, 開始日時:2, 終了日時:2, 順位:1, ポイント:2, レベル:1, 貢献ランクボタン:2]
COL_RATIOS = [3, 2, 2, 1, 2, 1, 2] 

# ---------- CSS（レイアウト調整） ----------
st.markdown("""
<style>
/* Streamlitのデフォルトの余白を一部調整し、テーブルの崩れを最小限に抑える */
.stContainer [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] > div > [data-testid="stHorizontalBlock"] {
    margin-bottom: 0px !important; /* 行間の余白を調整 */
}

/* ヘッダのカスタムCSS */
.row-header {
    display: flex; 
    background: #0b66c2; 
    color: #fff; 
    padding: 8px 12px; 
    font-weight: 700;
    line-height: 1.5;
}
/* ヘッダの各項目の幅比率を COL_RATIOS=[3, 2, 2, 1, 2, 1, 2] に合わせて設定 */
.row-header > div {
    text-align: center;
    overflow: hidden; 
}
.row-header > div:nth-child(1) { flex: 3; } /* イベント名 */
.row-header > div:nth-child(2) { flex: 2; } /* 開始日時 */
.row-header > div:nth-child(3) { flex: 2; } /* 終了日時 */
.row-header > div:nth-child(4) { flex: 1; } /* 順位 */
.row-header > div:nth-child(5) { flex: 2; } /* ポイント */
.row-header > div:nth-child(6) { flex: 1; } /* レベル */
.row-header > div:nth-child(7) { flex: 2; } /* 貢献ランク */


/* st.columns の中の要素をセンタリング */
/* Streamlitの内部CSSを上書きして、列内のコンテンツを中央揃えにする */
[data-testid="stColumn"] > div > div,
[data-testid="stColumn"] > div { 
    text-align: center;
    word-break: break-word; 
    padding-bottom: 8px; /* 列内のコンテンツの上下余白 */
    padding-top: 8px; 
}

/* 行のカスタムCSS: st.columns の外側で背景色とボーダーを設定 */
.row-item {
    border-bottom: 1px solid #eee;
}
.row-item.ongoing {
    background-color: #fff8b3; 
}
/* st.columns の padding を調整して、見た目をスッキリさせる */
[data-testid="stHorizontalBlock"] > [data-testid^="stColumn"] {
    padding-left: 0.5rem;
    padding-right: 0.5rem;
}

/* その他のスタイル */
.evlink {color:#0b57d0;text-decoration:none;}
.container-scroll {max-height:520px; overflow-y:auto; border:1px solid #ddd; border-radius:6px;}
.contribution-box {padding:8px 12px; background:#fafafa; border-left:3px solid #0b66c2; margin-bottom:8px; text-align:left !important;}
.rank-table-item {
    display: flex;
    justify-content: space-between;
    padding: 2px 0;
    font-size: 14px;
    border-bottom: 1px dotted #ccc;
}
.rank-table-item:last-child {
    border-bottom: none;
}
</style>
""", unsafe_allow_html=True)

# ---------- 表示（ヘッダ） ----------
st.markdown('<div class="row-header">'
    '<div>イベント名</div>'
    '<div>開始日時</div>'
    '<div>終了日時</div>'
    '<div>順位</div>'
    '<div>ポイント</div>'
    '<div>レベル</div>'
    '<div>貢献ランク</div>'
    '</div>', 
    unsafe_allow_html=True
)

# ---------- 表示（行：st.columns と st.button で開閉を実現） ----------
if "expanded_rows" not in st.session_state:
    st.session_state["expanded_rows"] = {}

# コンテナでスクロール可能に
st.markdown('<div class="container-scroll">', unsafe_allow_html=True)

# 行データを表示するコンテナ
row_container = st.container()

with row_container:
    for i, row in df_show.iterrows():
        ev_name = row.get("イベント名") or ""
        url = row.get("URL") or ""
        event_id = row.get("event_id") or ""
        start = row.get("開始日時") or ""
        end = row.get("終了日時") or ""
        rank = row.get("順位") or ""
        # ポイントをカンマ区切りにし、欠損値やハイフンの場合はそのまま表示
        point_raw = row.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')
        level = row.get("レベル") or ""
        link = f'<a class="evlink" href="{url}" target="_blank">{ev_name}</a>' if url else ev_name
        
        btn_key = f"contrib_{event_id}_{room_id}_{i}"
        is_expanded = st.session_state["expanded_rows"].get(btn_key, False)
        cls = " ongoing" if row.get("is_ongoing") else ""
        
        # 行全体を囲む div を出力し、行の背景色を設定
        st.markdown(f'<div class="row-item{cls}">', unsafe_allow_html=True)
        
        # st.columns を使用して、ヘッダと全く同じ幅比率でコンテンツを配置
        cols = st.columns(COL_RATIOS)
        
        # st.columns の中にコンテンツを配置
        with cols[0]:
            st.markdown(link, unsafe_allow_html=True)
        with cols[1]:
            st.markdown(start)
        with cols[2]:
            st.markdown(end)
        with cols[3]:
            st.markdown(str(rank))
        with cols[4]:
            st.markdown(point)
        with cols[5]:
            st.markdown(str(level))
            
        with cols[6]:
            # st.button で開閉式のトグルボタンを実装
            btn_label = "非表示 ▲" if is_expanded else "貢献ランクを表示 ▶"
            # ボタンを押した際に Session State の値を反転
            if st.button(btn_label, key=btn_key, use_container_width=True):
                st.session_state["expanded_rows"][btn_key] = not is_expanded

        # 行の背景色を閉じるための div を出力 (ボタンの直後に配置)
        st.markdown('</div>', unsafe_allow_html=True)

        # 展開部 (貢献ランキング)
        if is_expanded:
            # 展開部は行全体を使いたいので、st.columns の外で st.container() を使用
            with st.container():
                ranks = fetch_contribution_rank(event_id, room_id)
                if ranks:
                    # 表示（簡易テーブル）
                    st.markdown('<div class="contribution-box">', unsafe_allow_html=True)
                    st.markdown(f"**貢献ランク（上位{len(ranks)}）**")
                    # 貢献ランクの表示は st.markdown でレイアウトを調整
                    for r in ranks:
                        # 貢献ランクの表示は左寄せの方が読みやすいので、CSSで調整
                        st.markdown(f'<div class="rank-table-item"><span>{r["順位"]}. {r["名前"]}</span><span>{r["ポイント"]}</span></div>', unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.info("ランキング情報が取得できませんでした。", icon="ℹ️")

st.markdown('</div>', unsafe_allow_html=True) # .container-scroll を閉じる

st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

# CSV ダウンロード（表示用データ）
csv_bytes = df_show.drop(columns=["is_ongoing", "event_id", "URL"]).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")