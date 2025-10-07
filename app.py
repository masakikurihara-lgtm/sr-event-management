import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime, timedelta
import pytz
import re # URL解析のためにreモジュールを追加

JST = pytz.timezone("Asia/Tokyo")

# --- 定数 ---
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.4)"}
ADMIN_ROOM_ID = "mksp154851" # 管理者ID

st.set_page_config(page_title="SHOWROOM：参加イベント履歴ビューア", layout="wide")

# --------------------
# ★★★ 追記: フィルタリング基準日（2023年9月1日 00:00:00 JST）のタイムスタンプ ★★★
FILTER_START_TS = int(datetime(2023, 9, 1, 0, 0, 0, tzinfo=JST).timestamp())
# --------------------

# ---------- ポイントハイライト用のカラー定義（ライバー用） ----------
HIGHLIGHT_COLORS = {
    1: "background-color: #ff7f7f;", # 1位
    2: "background-color: #ff9999;", # 2位
    3: "background-color: #ffb2b2;", # 3位
    4: "background-color: #ffcccc;", # 4位
    5: "background-color: #ffe5e5;", # 5位
}

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
        ts_strip = ts.strip()
        # まず時刻付き（ゼロ埋めなし）の形式でパースを試みる
        try:
            dt_obj = datetime.strptime(ts_strip, "%Y/%m/%d %H:%M")
            return dt_obj.strftime("%Y/%m/%d %H:%M")
        except ValueError:
            # 時刻がない形式（ゼロ埋めなし）でパースを試みる
            try:
                dt_obj = datetime.strptime(ts_strip, "%Y/%m/%d")
                return dt_obj.strftime("%Y/%m/%d 00:00")
            except ValueError:
                # どの形式でもパースできない場合は、元の文字列を返す
                return ts_strip 
    try:
        ts = int(float(ts))
        if ts > 20000000000:
            ts = ts // 1000
        # タイムスタンプからの変換は元々ゼロ埋め形式
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
            # 日付のみの場合、00:00:00としてタイムスタンプを返す
            return int(datetime.strptime(val.split(" ")[0], "%Y/%m/%d").timestamp())
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

def generate_contribution_url(event_url, room_id):
    """イベントURLから貢献ランキングのURLを生成する。"""
    if not event_url:
        return None
    match = re.search(r'/event/([^/]+)/?$', event_url)
    if match:
        url_key = match.group(1)
        return f"https://www.showroom-live.com/event/contribution/{url_key}?room_id={room_id}"
    return None

# ----------------------------------------------------------------------
# ★★★ 修正/新規追加: セッションステートの初期化とコールバック関数 ★★★
# ----------------------------------------------------------------------
if 'sort_by_point' not in st.session_state:
    st.session_state.sort_by_point = False
if 'room_input_value' not in st.session_state:
    st.session_state.room_input_value = ""
if 'show_data' not in st.session_state:
    st.session_state.show_data = False # データ表示トリガー

# --- 管理者用ステートの追加 ---
if 'admin_filter_start_date' not in st.session_state:
    st.session_state.admin_filter_start_date = "全期間"
if 'admin_filter_end_date' not in st.session_state:
    st.session_state.admin_filter_end_date = "全期間"
# 最新化ボタンが押された回数を記録
if 'admin_data_refresh' not in st.session_state:
    st.session_state.admin_data_refresh = 0
# 最後に最新化を実行した回数を記録（リロード時に重複実行を防ぐため）
if 'last_admin_refresh_count' not in st.session_state:
    st.session_state.last_admin_refresh_count = 0


def toggle_sort_by_point():
    """ソート状態を切り替えるコールバック関数 (ライバー用)"""
    st.session_state.sort_by_point = not st.session_state.sort_by_point
    st.session_state.show_data = True

def trigger_show_data():
    """「表示する」ボタンが押されたときのコールバック関数"""
    st.session_state.show_data = True

def save_room_id():
    """ルームID入力欄の値が変更されたときにセッションに保存する"""
    # st.text_input(key='room_id_input')でアクセス
    st.session_state.room_input_value = st.session_state.room_id_input

def trigger_admin_refresh():
    """管理者用「最新化」ボタンのコールバック関数"""
    # カウントを増やすことで、メインロジック内で処理をトリガーする
    st.session_state.admin_data_refresh += 1
# ----------------------------------------------------------------------


# ---------- UI ----------
st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

# ----------------------------------------------------------------------
# ★★★ 修正: st.text_inputにkeyとon_changeを追加し、valueをセッションから取得 ★★★
# ----------------------------------------------------------------------
st.text_input(
    "表示するルームIDを入力", 
    value=st.session_state.room_input_value, 
    key="room_id_input", 
    on_change=save_room_id
)

# ----------------------------------------------------------------------
# ★★★ 修正: 「表示する」ボタンにon_clickを設定し、st.session_state.show_dataを制御 ★★★
# ----------------------------------------------------------------------
if st.button("表示する", on_click=trigger_show_data):
    pass
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# ★★★ 修正: データ表示の制御ロジックをst.session_state.show_dataに基づき変更 ★★★
# ----------------------------------------------------------------------
room_id = st.session_state.room_input_value.strip()
is_admin = (room_id == ADMIN_ROOM_ID)

# データ表示を行う条件
do_show = st.session_state.show_data and room_id != ""

if not do_show:
    if room_id == "":
        st.info("ルームIDを入力して「表示する」を押してください。")
    st.stop()

# ----------------------------------------------------------------------
# ここから下の処理は、do_show = True の場合にのみ実行される

with st.spinner("イベントDBを取得中..."):
    df_all = load_event_db(EVENT_DB_URL)
if df_all.empty:
    st.stop()

df = df_all.copy()

# ----------------------------------------------------------------------
# ★★★ 修正箇所1: ライバー名表示のカスタムCSS定義をグローバルに追加 ★★★
# ----------------------------------------------------------------------
st.markdown("""
<style>
/* ルーム名ラベルのCSS (st.info風) */
.room-label-box {
    background-color: #f0f2f6; /* st.infoの薄い青背景に近い色 */
    border: 1px solid #c9d0d8; /* st.infoの薄い枠線に近い色 */
    border-left: 5px solid #0b66c2; /* st.infoの左側の青い縦線 */
    padding: 10px 15px;
    margin-bottom: 15px;
    border-radius: 6px;
    /* リンクの色を継承させるため */
    color: #0b66c2;
    font-size: 17px; /* 大きすぎないフォントサイズ */
}
/* ルーム名のリンクに太字と下線を適用 */
.room-label-box a {
    color: inherit; /* 親要素の色を継承 */
    font-weight: 700; /* ルーム名のみ太字 */
    text-decoration: underline; /* ルーム名に下線 */
}
</style>
""", unsafe_allow_html=True)
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# ★★★ 修正箇所2: ライバー名表示をカスタムCSSに置き換え（デグレード回避） ★★★
# ----------------------------------------------------------------------
room_name = get_room_name(room_id) if not is_admin else "（全データ表示中）"

if is_admin:
    st.info(f"**管理者モード：全ライバーのイベント参加状況**")
else:
    link_url = f"https://www.showroom-live.com/room/profile?room_id={room_id}"
    label_html = f"""
    <div class="room-label-box">
        🎤 
        <a href="{link_url}" target="_blank">
            {room_name}
        </a> 
        の参加イベント履歴
    </div>
    """
    st.markdown(label_html, unsafe_allow_html=True)
# ----------------------------------------------------------------------


# ---------- 日付整形＆TS変換 ----------
df["開始日時"] = df["開始日時"].apply(fmt_time)
df["終了日時"] = df["終了日時"].apply(fmt_time)
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)

# ----------------------------------------------------------------------
# ★★★ 管理者/ライバーごとのフィルタリング＆ソートロジック ★★★
# ----------------------------------------------------------------------
now_ts = int(datetime.now(JST).timestamp())
now_date = datetime.now(JST).strftime("%Y/%m/%d")

if is_admin:
    # **管理者モードのデータ処理**
    
    # 1. 終了日時の絞り込み基準設定 (現在時刻の10日前 00:00:00 JST)
    TEN_DAYS_AGO = datetime.now(JST).date() - timedelta(days=10)
    FILTER_END_DATE_TS = int(datetime(TEN_DAYS_AGO.year, TEN_DAYS_AGO.month, TEN_DAYS_AGO.day, 0, 0, 0, tzinfo=JST).timestamp())
    
    # 2. フィルタリング (終了日時が10日前以降のイベント全量)
    df = df[df["__end_ts"].notna() & (df["__end_ts"] >= FILTER_END_DATE_TS)].copy()
    
    if df.empty:
        st.warning("管理者モードのフィルタリング条件に一致するデータが見つかりません。")
        st.stop()
        
    # 3. 終了日時の当日の行にハイライトを付けるためのフラグ
    def check_end_today(end_ts):
        if end_ts is None or end_ts == "":
            return False
        # 終了日時を日付文字列に変換
        end_date_str = datetime.fromtimestamp(end_ts, JST).strftime("%Y/%m/%d")
        return end_date_str == now_date

    df["is_end_today"] = df["__end_ts"].apply(check_end_today)
    
    # 4. デフォルトソート: 終了日時が新しいもの（降順）
    df.sort_values("__end_ts", ascending=False, inplace=True)
    
    # 5. フィルタリングUIの準備 (セレクトボックスの選択肢)
    unique_start_dates = sorted(df["開始日時"].apply(lambda x: x.split(" ")[0]).unique().tolist())
    unique_end_dates = sorted(df["終了日時"].apply(lambda x: x.split(" ")[0]).unique().tolist())
    
    all_dates_start = ["全期間"] + unique_start_dates
    all_dates_end = ["全期間"] + unique_end_dates
    
    # 6. セッションステートに保持されたフィルタを適用
    filter_start_date_str = st.session_state.admin_filter_start_date
    filter_end_date_str = st.session_state.admin_filter_end_date

    if filter_start_date_str != "全期間":
        filter_start_ts = parse_to_ts(filter_start_date_str)
        if filter_start_ts is not None:
            # 開始日時が選択された日付の00:00:00以降
            df = df[df["__start_ts"] >= filter_start_ts].copy()

    if filter_end_date_str != "全期間":
        # 終了日付の23:59:59を取得してフィルタリングの終端とする
        try:
            end_date_obj = datetime.strptime(filter_end_date_str, "%Y/%m/%d")
            # 23:59:59 のタイムスタンプを取得
            filter_end_ts = int((end_date_obj + timedelta(days=1) - timedelta(seconds=1)).timestamp())
            df = df[df["__end_ts"] <= filter_end_ts].copy()
        except ValueError:
            pass
    
    # 7. 開催中判定
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: x and x > now_ts)
    
    # 8. 「最新化」ボタンが押された場合
    if st.session_state.admin_data_refresh > st.session_state.last_admin_refresh_count:
        with st.spinner("開催中イベントのデータ最新化中..."):
            ongoing = df[df["is_ongoing"]]
            for idx, row in ongoing.iterrows():
                event_id = row.get("event_id")
                current_room_id = row.get("ルームID") 
                if not current_room_id or not event_id:
                    continue
                
                stats = get_event_stats_from_roomlist(event_id, current_room_id)
                if stats:
                    # df.loc[] を使用して代入
                    df.loc[idx, "順位"] = stats.get("rank") or "-"
                    df.loc[idx, "ポイント"] = stats.get("point") or 0
                    df.loc[idx, "レベル"] = stats.get("quest_level") or 0
                time.sleep(0.3)
        # 処理完了後、カウントを更新
        st.session_state.last_admin_refresh_count = st.session_state.admin_data_refresh
    
else:
    # **ライバーモードのデータ処理** (既存ロジック)
    
    # 1. ユーザーIDでフィルタリング
    df = df[df["ルームID"].astype(str) == str(room_id)].copy()
    
    # 2. フィルタリング (2023年9月1日以降)
    df = df[df["__start_ts"].notna() & (df["__start_ts"] >= FILTER_START_TS)].copy()

    # 3. デフォルトソート: 開始日時降順
    df.sort_values("__start_ts", ascending=False, inplace=True)
    
    # 4. 開催中判定
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: x and x > now_ts)
    
    # 5. 開催中イベント最新化
    ongoing = df[df["is_ongoing"]]
    for idx, row in ongoing.iterrows():
        event_id = row.get("event_id")
        stats = get_event_stats_from_roomlist(event_id, room_id)
        if stats:
            df.at[idx, "順位"] = stats.get("rank") or "-"
            df.at[idx, "ポイント"] = stats.get("point") or 0
            df.at[idx, "レベル"] = stats.get("quest_level") or 0
        time.sleep(0.3)
        
    # 6. ポイントハイライトロジック
    df['__point_num'] = pd.to_numeric(df['ポイント'], errors='coerce')
    df_valid_points = df.dropna(subset=['__point_num']).copy()
    df_valid_points['__rank'] = df_valid_points['__point_num'].rank(method='dense', ascending=False)
    df['__highlight_style'] = ''
    for rank, style in HIGHLIGHT_COLORS.items():
        if not df_valid_points.empty:
            target_indices = df_valid_points[df_valid_points['__rank'] == rank].index
            if not target_indices.empty:
                df.loc[target_indices, '__highlight_style'] = style
    
    # 7. ポイントソートの適用
    if st.session_state.sort_by_point:
        df.sort_values(
            ['__point_num', '__start_ts'],
            ascending=[False, False], 
            na_position='last', 
            inplace=True
        )

# ---------- 表示整形 ----------
if is_admin:
    # 管理者用表示項目
    # HTML生成のために必要な列を追加
    disp_cols = ["ライバー名", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
    df_show = df[disp_cols + ["is_ongoing", "is_end_today", "URL", "ルームID"]].copy() 
    # ライバー名のリンクURLを生成
    df_show["ライバー名_URL"] = df_show["ルームID"].apply(lambda x: f"https://www.showroom-live.com/room/profile?room_id={x}")
    df_show.drop(columns=["ルームID"], inplace=True)
else:
    # ライバー用表示項目
    disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL"]
    df_show = df[disp_cols + ["is_ongoing", "__highlight_style", "URL", "ルームID"]].copy()

# ----------------------------------------------------------------------
# ★★★ 表示構築（HTMLテーブル）関数の分離と定義 ★★★
# ----------------------------------------------------------------------
# ライバー向け（貢献ランクボタンとポイントハイライトあり）
def make_html_table_user(df_show, room_id):
    """ライバー向け表示: 貢献ランクボタンとポイントハイライトあり"""
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
        color:white !important; 
        border:none;
        padding:4px 6px;
        border-radius:4px;
        cursor:pointer;
        text-decoration:none; 
        display: inline-block; 
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
    for _, r in df_show.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url = r.get("URL") or ""
        name = r.get("イベント名") or ""
        point_raw = r.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')
        
        event_link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name
        
        contrib_url = generate_contribution_url(url, room_id)
        if contrib_url:
            button_html = f'<a href="{contrib_url}" target="_blank" class="rank-btn-link">貢献ランクを確認</a>'
        else:
            button_html = "<span>URLなし</span>"

        highlight_style = r.get('__highlight_style', '')
        point_td = f"<td style=\"{highlight_style}\">{point}</td>"


        html += f'<tr class="{cls.strip()}">'
        html += f"<td>{event_link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td>{point_td}<td>{r['レベル']}</td><td>{button_html}</td>"
        html += "</tr>"
        
    html += "</tbody></table></div>"
    return html

# 管理者向け（ライバー名と終了日当日ハイライトあり、貢献ランクボタンなし）
def make_html_table_admin(df_show):
    """管理者向け表示: ライバー名付き、終了日当日ハイライトあり、貢献ランクボタンなし"""
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
    table col:nth-child(1) { width: 17%; } /* ライバー名 */
    table col:nth-child(2) { width: 33%; } /* イベント名 */
    table col:nth-child(3) { width: 10%; } /* 開始日時 */
    table col:nth-child(4) { width: 10%; } /* 終了日時 */
    table col:nth-child(5) { width: 8%; }  /* 順位 */
    table col:nth-child(6) { width: 14%; } /* ポイント */
    table col:nth-child(7) { width: 8%; }  /* レベル */
    
    tr.ongoing{background:#fff8b3;}
    a.evlink{color:#0b57d0;text-decoration:none;}

    /* 終了日当日のハイライト (赤枠) */
    tr.end-today{border: 2px solid red !important; border-collapse: separate;}
    
    </style>
    <div class="scroll-table"><table>
    <colgroup>
        <col><col><col><col><col><col><col>
    </colgroup>
    <thead><tr>
    <th>ライバー名</th><th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th>
    </tr></thead><tbody>
    """
    for _, r in df_show.iterrows():
        # 行クラス: 開催中 or 終了日当日
        cls = "ongoing" if r.get("is_ongoing") else ""
        if r.get("is_end_today"):
            # 終了日当日ハイライトを適用
            cls += " end-today"

        # ライバー名リンク
        room_name = r.get("ライバー名") or "不明"
        room_url = r.get("ライバー名_URL") or "#"
        room_link = f'<a class="evlink" href="{room_url}" target="_blank">{room_name}</a>'
        
        # イベント名リンク
        event_url = r.get("URL") or ""
        event_name = r.get("イベント名") or ""
        event_link = f'<a class="evlink" href="{event_url}" target="_blank">{event_name}</a>' if event_url else event_name
        
        # ポイントをカンマ区切りに
        point_raw = r.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')

        html += f'<tr class="{cls.strip()}">'
        html += f"<td>{room_link}</td><td>{event_link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td><td>{point}</td><td>{r['レベル']}</td>"
        html += "</tr>"
        
    html += "</tbody></table></div>"
    return html


# ---------- 表示 ----------
if is_admin:
    # **管理者モード**
    
    # フィルタリングUI
    col_start, col_end, col_refresh = st.columns([1, 1, 1])
    
    # 開始日時フィルタ
    with col_start:
        selected_start_date = st.selectbox(
            "開始日時で絞り込み", 
            options=all_dates_start,
            # index=all_dates_start.index(st.session_state.admin_filter_start_date)がエラーになるのを防ぐ
            index=all_dates_start.index(st.session_state.admin_filter_start_date) if st.session_state.admin_filter_start_date in all_dates_start else 0,
            key="admin_filter_start_date_sb",
            help="選択された日付の00:00:00以降のイベントを表示します。"
        )
        st.session_state.admin_filter_start_date = selected_start_date

    # 終了日時フィルタ
    with col_end:
        selected_end_date = st.selectbox(
            "終了日時で絞り込み", 
            options=all_dates_end,
             # index=all_dates_end.index(st.session_state.admin_filter_end_date)がエラーになるのを防ぐ
            index=all_dates_end.index(st.session_state.admin_filter_end_date) if st.session_state.admin_filter_end_date in all_dates_end else 0,
            key="admin_filter_end_date_sb",
            help="選択された日付の23:59:59までのイベントを表示します。"
        )
        st.session_state.admin_filter_end_date = selected_end_date

    # 最新化ボタン
    with col_refresh:
        # 見た目のためにSpacerを入れる
        st.markdown("<div style='margin-top: 25px;'></div>", unsafe_allow_html=True)
        st.button("🔄 最新化 (開催中イベント)", on_click=trigger_admin_refresh, key="admin_refresh_button")
        
    # HTMLテーブルで表示
    st.markdown(make_html_table_admin(df_show), unsafe_allow_html=True) 
    st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。**赤枠行は終了日時が当日**のイベントです。")
    
else:
    # **ライバーモード**
    
    # ----------------------------------------------------------------------
    # ★★★ ソートボタンの表示 ★★★
    # ----------------------------------------------------------------------
    button_label = (
        "📅 デフォルト表示に戻す (開始日時降順)"
        if st.session_state.sort_by_point
        else "🏆 ポイントの高い順にソート"
    )

    st.button(
        button_label, 
        on_click=toggle_sort_by_point, 
        key="sort_toggle_button"
    )
    # ----------------------------------------------------------------------

    # HTMLテーブルで表示することで、レイアウトの安定化とスクロール機能を両立
    st.markdown(make_html_table_user(df_show, room_id), unsafe_allow_html=True)
    st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")
    

# ---------- CSV出力 ----------
# CSVダウンロード時には追加した内部列を削除
if is_admin:
    # 管理者用CSV（ハイライト、URL、ルームIDなどを削除）
    csv_df = df_show.drop(columns=["is_ongoing", "is_end_today", "URL", "ライバー名_URL"])
else:
    # ライバー用CSV（ハイライト、is_ongoingなどを削除）
    csv_df = df_show.drop(columns=["is_ongoing", "__highlight_style", "URL", "ルームID"])

csv_bytes = csv_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv")