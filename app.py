import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime, timedelta
import pytz
import re # URL解析のためにreモジュールを追加
import numpy as np # pandasでNaNを扱うために追記

JST = pytz.timezone("Asia/Tokyo")

EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.4)"}

st.set_page_config(page_title="SHOWROOM：参加イベント履歴ビューア", layout="wide")

# --------------------
# フィルタリング基準日（2023年9月1日 00:00:00 JST）のタイムスタンプ
FILTER_START_TS = int(datetime(2023, 9, 1, 0, 0, 0, tzinfo=JST).timestamp())

# 管理者モードのフィルタリング基準 (現在から10日前)
FILTER_END_DATE_TS_DEFAULT = int((datetime.now(JST) - timedelta(days=10)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
# --------------------

# ---------- ポイントハイライト用のカラー定義 ----------
HIGHLIGHT_COLORS = {
    1: "background-color: #ff7f7f;", # 1位
    2: "background-color: #ff9999;", # 2位
    3: "background-color: #ffb2b2;", # 3位
    4: "background-color: #ffcccc;", # 4位
    5: "background-color: #ffe5e5;", # 5位
}
# ★★★ 管理者用: 終了日時当日のハイライトカラー ★★★
END_TODAY_HIGHLIGHT = "background-color: #ffb2b2;" # 赤系

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
        dt_obj_naive = datetime.strptime(val, "%Y/%m/%d %H:%M")
        # ★★★ 修正: JSTとしてローカライズしてからタイムスタンプを取得 ★★★
        return int(JST.localize(dt_obj_naive).timestamp())
    except Exception:
        # 日付のみの形式も試す (00:00:00 JSTとして処理)
        try:
            dt_obj_naive = datetime.strptime(val, "%Y/%m/%d")
            # ★★★ 修正: JSTとしてローカライズしてからタイムスタンプを取得 ★★★
            return int(JST.localize(dt_obj_naive).timestamp())
        except Exception:
            return None


def load_event_db(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        # ★★★ 修正: dtype=str の代わりに、object型で読み込み、欠損値を' 'に置換 ★★★
        # これは、後の処理でpandasの意図しない型変換を防ぐための防御的なコーディングです。
        df = pd.read_csv(io.StringIO(txt), dtype=object, keep_default_na=False)
        # pd.read_csv(io.StringIO(txt), dtype=str)
    except Exception as e:
        # st.error(f"イベントDB取得失敗: {e}") # ライバーモードの挙動に合わせ、エラー表示はしない
        return pd.DataFrame()

    df.columns = [c.replace("_fmt", "").strip() for c in df.columns]
    for c in ["event_id", "URL", "ルームID", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "ライバー名"]:
        if c not in df.columns:
            # 存在しない列は空文字列で初期化
            df[c] = ""
        # 欠損値（空の文字列を含む）をNaNに変換し、NaNを空文字列に戻すことで処理を統一
        df[c] = df[c].replace('', np.nan).fillna('')
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

# 貢献ランク取得関数は、今回は直接リンクを開くため既存ロジックとして残します。
def fetch_contribution_rank(*args, **kwargs):
    # 既存のロジックから変更なし
    return []

# ---------- 貢献ランクURL生成ロジック ----------
def generate_contribution_url(event_url, room_id):
    """
    イベントURLからURLキーを取得し、貢献ランキングのURLを生成する。
    例: https://www.showroom-live.com/event/mattari_fireworks249 -> mattari_fireworks249
    生成: https://www.showroom-live.com/event/contribution/mattari_fireworks249?room_id=ROOM_ID
    """
    # ★★★ 修正: pd.isna(event_url) をチェックに追加（防御的） ★★★
    if pd.isna(event_url) or not event_url:
        return None
    # URLの最後の階層部分（URLキー）を正規表現で抽出
    match = re.search(r'/event/([^/]+)/?$', event_url)
    if match:
        url_key = match.group(1)
        return f"https://www.showroom-live.com/event/contribution/{url_key}?room_id={room_id}"
    return None

# ----------------------------------------------------------------------
# ★★★ セッションステートの初期化とコールバック関数 ★★★
# ----------------------------------------------------------------------
if 'sort_by_point' not in st.session_state:
    st.session_state.sort_by_point = False
if 'room_input_value' not in st.session_state:
    st.session_state.room_input_value = ""
if 'show_data' not in st.session_state:
    st.session_state.show_data = False # データ表示トリガー

# ★★★ 管理者モード用セッションステート ★★★
if 'admin_full_data' not in st.session_state:
    st.session_state.admin_full_data = False
if 'admin_start_date' not in st.session_state:
    st.session_state.admin_start_date = None
if 'admin_end_date' not in st.session_state:
    st.session_state.admin_end_date = None
# ★★★ 管理者モード用 ルーム名キャッシュ ★★★
if 'room_name_cache' not in st.session_state:
    st.session_state.room_name_cache = {}
# ★★★ 最新化トリガーフラグ ★★★
if 'refresh_trigger' not in st.session_state:
    st.session_state.refresh_trigger = False


def toggle_sort_by_point():
    """ソート状態を切り替えるコールバック関数"""
    st.session_state.sort_by_point = not st.session_state.sort_by_point
    st.session_state.show_data = True

def trigger_show_data():
    """「表示する」ボタンが押されたときのコールバック関数"""
    st.session_state.show_data = True

def save_room_id():
    """ルームID入力欄の値が変更されたときにセッションに保存する"""
    st.session_state.room_input_value = st.session_state.room_id_input

def refresh_data():
    """最新化ボタンのコールバック"""
    st.session_state.refresh_trigger = True
    st.session_state.show_data = True # 最新化も表示トリガーとする

def toggle_full_data():
    """
    全量表示チェックボックスの値をセッションステートに強制的に同期させるコールバック関数。
    キー名 'admin_full_data_checkbox_internal' の値を 'admin_full_data' にコピーする。
    """
    st.session_state.admin_full_data = st.session_state.admin_full_data_checkbox_internal
# ----------------------------------------------------------------------


# ---------- UI ----------
st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

st.text_input(
    "表示するルームIDを入力", 
    value=st.session_state.room_input_value, 
    key="room_id_input", 
    on_change=save_room_id
)

if st.button("表示する", on_click=trigger_show_data, key="show_data_button"):
    pass 

room_id = st.session_state.room_input_value.strip()
is_admin = (room_id == "mksp154851")
do_show = st.session_state.show_data and room_id != ""

if not do_show:
    if room_id == "":
        # st.info("ルームIDを入力して「表示する」を押してください。") # ライバーモードの挙動に合わせ、infoを削除
        pass
    st.stop()

# ----------------------------------------------------------------------
# データ取得
# ----------------------------------------------------------------------
# 管理者モードは毎回CSVを再ロード（または最新化ボタン押下時）
if 'df_all' not in st.session_state or is_admin or st.session_state.get('refresh_trigger', False):
    # ライバーモードの挙動に合わせ、spinnerを削除
    df_all = load_event_db(EVENT_DB_URL)
    st.session_state.df_all = df_all # セッションに保存

if st.session_state.df_all.empty:
    st.stop()

df_all = st.session_state.df_all.copy() # コピーを使用して、元のセッションデータを汚染しないようにする

# ----------------------------------------------------------------------
# データのフィルタリングと整形 (管理者/ライバーで分岐)
# ----------------------------------------------------------------------

if is_admin:
    # --- 管理者モードのデータ処理 ---
    # st.info(f"**管理者モード**") # ← 削除 (ユーザー要望)

    # 1. 日付整形とタイムスタンプ追加 (全量)
    df = df_all.copy()
    df["開始日時"] = df["開始日時"].apply(fmt_time)
    df["終了日時"] = df["終了日時"].apply(fmt_time)
    df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
    df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
    
    # 2. 開催中判定
    now_ts = int(datetime.now(JST).timestamp())
    today_ts = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts)
    
    # 終了日時が当日（今日0時〜明日0時の間）の判定
    df["is_end_today"] = df["__end_ts"].apply(lambda x: pd.notna(x) and today_ts <= x < (today_ts + 86400))


    # ★★★ 修正 (5. 開催中イベント最新化) - 自動最新化/ボタン最新化をここで実行 ★★★
    if is_admin or st.session_state.get('refresh_trigger', False):
        ongoing = df[df["is_ongoing"]] # df (フィルタ前の全データ) を使用
        
        # with st.spinner("開催中イベントの順位/ポイントを最新化中..."): # ← 削除 (ユーザー要望)
        for idx, row in ongoing.iterrows():
            event_id = row.get("event_id")
            room_id_to_update = row.get("ルームID")
            stats = get_event_stats_from_roomlist(event_id, room_id_to_update)
            if stats:
                st.session_state.df_all.at[idx, "順位"] = stats.get("rank") or "-"
                st.session_state.df_all.at[idx, "ポイント"] = stats.get("point") or 0
                st.session_state.df_all.at[idx, "レベル"] = stats.get("quest_level") or 0
            time.sleep(0.1) # API負荷軽減
        
        st.session_state.refresh_trigger = False
        # st.toast("開催中イベントの最新化が完了しました。", icon="✅") # ← 削除 (ユーザー要望)
        
        # ★★★ 修正: st.session_state.df_all の更新を反映するため、df を再作成 ★★★
        df_all = st.session_state.df_all.copy()
        df = df_all.copy()
        
        # 再度フラグ/TSを付ける (必須)
        df["開始日時"] = df["開始日時"].apply(fmt_time)
        df["終了日時"] = df["終了日時"].apply(fmt_time)
        df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
        df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
        now_ts = int(datetime.now(JST).timestamp())
        today_ts = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts)
        df["is_end_today"] = df["__end_ts"].apply(lambda x: pd.notna(x) and today_ts <= x < (today_ts + 86400))
    # ★★★ 修正ブロック終了 ★★★


    # 4. フィルタリングの適用（デフォルトフィルタリングまで）
    df_filtered = df.copy()

    # 2023年9月1日以降に開始のイベントに限定（ライバーモードと同じ基準）
    df_filtered = df_filtered[
        # __start_ts が有効な値で、かつ FILTER_START_TS 以上であること
        (df_filtered["__start_ts"].apply(lambda x: pd.notna(x) and x >= FILTER_START_TS))
        | (df_filtered["__start_ts"].isna()) # タイムスタンプに変換できない行も一応含める
    ].copy()

    # デフォルトフィルタリング（全量表示がOFFの場合）
    if not st.session_state.admin_full_data:
        # 終了日時が10日前以降のイベントに絞り込み
        df_filtered = df_filtered[
            (df_filtered["__end_ts"].apply(lambda x: pd.notna(x) and x >= FILTER_END_DATE_TS_DEFAULT))
            | (df_filtered["__end_ts"].isna()) # タイムスタンプに変換できない行も一応含める
        ].copy()

    # 終了日時フィルタリング用の選択肢生成
    unique_end_dates = sorted(
        list(set(df_filtered["終了日時"].apply(lambda x: x.split(' ')[0] if x else '')) - {''}), 
        reverse=True
    )
    
    # 開始日時フィルタリング用の選択肢生成
    unique_start_dates = sorted(
        list(set(df_filtered["開始日時"].apply(lambda x: x.split(' ')[0] if x else '')) - {''}), 
        reverse=True
    )

    # 3. UIコンポーネント (フィルタ、最新化ボタン)
    # ★★★ 修正: 横並びを廃止し、折りたためるセクション内で縦に配置する (レスポンシブ対応) ★★★
    with st.expander("⚙️ 個別機能・絞り込みオプション"):
        
        # 1. 最新化ボタン
        st.button(
            "🔄 開催中イベントの最新化", 
            on_click=refresh_data, 
            key="admin_refresh_button"
        )
        
        # 2. 全量表示トグル
        st.checkbox(
            "全量表示（期間フィルタ無効）", 
            value=st.session_state.admin_full_data,
            key="admin_full_data_checkbox_internal",
            on_change=toggle_full_data
        )

        st.markdown("---") # 区切り線

        # 3. 終了日時フィルタリング
        selected_end_date = st.selectbox(
            "終了日時で絞り込み",
            options=["全期間"] + unique_end_dates,
            key='admin_end_date_filter',
        )

        # 4. 開始日時フィルタリング
        selected_start_date = st.selectbox(
            "開始日時で絞り込み",
            options=["全期間"] + unique_start_dates,
            key='admin_start_date_filter',
        )
        
        
        
        # ============================================================
        # 🧭 管理者専用：イベントDB更新機能（既存履歴ビューアと独立動作）
        # ============================================================
        if is_admin:
            st.markdown("---")
            st.markdown("### 🧩 イベントデータベース更新機能（管理者専用）")

            import ftplib, traceback, socket, concurrent.futures
            from typing import List, Dict, Any

            API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
            API_CONTRIBUTION = "https://www.showroom-live.com/api/event/contribution_ranking"
            ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
            ARCHIVE_URL = "https://mksoul-pro.com/showroom/file/sr-event-archive.csv"
            HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-bot/1.0)"}
            JST = pytz.timezone("Asia/Tokyo")

            # ------------------------------------------------------------
            # 既存ロジック移植（堅牢なGET / FTPアップロード）
            # ------------------------------------------------------------
            def http_get_json(url, params=None, retries=3, timeout=12, backoff=0.6):
                for i in range(retries):
                    try:
                        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
                        if r.status_code == 429:
                            time.sleep(backoff * (i + 2))
                            continue
                        if r.status_code == 200:
                            return r.json()
                        if r.status_code in (404, 410):
                            return None
                    except (requests.RequestException, socket.timeout):
                        time.sleep(backoff * (i + 1))
                return None

            def ftp_upload_bytes(file_path: str, content_bytes: bytes, retries: int = 2):
                ftp_info = st.secrets.get("ftp", {})
                host = ftp_info.get("host")
                user = ftp_info.get("user")
                password = ftp_info.get("password")
                if not host or not user:
                    raise RuntimeError("FTP情報が st.secrets['ftp'] に存在しません。")
                for i in range(retries):
                    try:
                        with ftplib.FTP(host, timeout=30) as ftp:
                            ftp.login(user, password)
                            with io.BytesIO(content_bytes) as bf:
                                bf.seek(0)
                                ftp.storbinary(f"STOR {file_path}", bf)
                        return True
                    except Exception:
                        time.sleep(1 + i)
                raise

            def fmt_time(ts):
                try:
                    if ts is None:
                        return ""
                    ts = int(ts)
                    if ts > 20000000000:
                        ts //= 1000
                    return datetime.fromtimestamp(ts, JST).strftime("%Y/%m/%d %H:%M")
                except Exception:
                    return ""

            # ------------------------------------------------------------
            # イベントDB範囲確認（DB内最新ID / 開催予定API最新ID）
            # ------------------------------------------------------------
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📊 DB内の最新イベントIDを確認", key="check_db_latest_id"):
                    try:
                        df_db = load_event_db(EVENT_DB_URL)
                        latest = pd.to_numeric(df_db["event_id"], errors="coerce").max()
                        st.success(f"現在のevent_database.csvの最新ID: {int(latest)}")
                    except Exception as e:
                        st.error(f"取得失敗: {e}")

            with col2:
                if st.button("🌐 SHOWROOM開催予定イベントの最新IDを確認", key="check_api_latest_id"):
                    try:
                        latest_id = 0
                        for p in range(1, 6):
                            data = http_get_json("https://www.showroom-live.com/api/event/search",
                                                 params={"status": 3, "page": p})
                            if not data or "event_list" not in data:
                                break
                            ids = [int(ev["event_id"]) for ev in data["event_list"] if "event_id" in ev]
                            if ids:
                                latest_id = max(latest_id, max(ids))
                            time.sleep(0.2)
                        if latest_id:
                            st.success(f"SHOWROOM開催予定イベントの最新ID: {latest_id}")
                        else:
                            st.warning("取得できませんでした。")
                    except Exception as e:
                        st.error(f"API取得失敗: {e}")

            st.markdown("---")
            st.markdown("#### 🚀 データベース更新実行")

            start_id = st.number_input("スキャン開始イベントID", min_value=1, value=40290, step=1)
            end_id = st.number_input("スキャン終了イベントID", min_value=start_id, value=start_id + 10, step=1)
            max_workers = st.number_input("並列処理数", min_value=1, max_value=30, value=3)
            save_interval = st.number_input("途中保存間隔（件）", min_value=50, value=200, step=50)
            ftp_path = st.text_input("FTP保存パス", value="/mksoul-pro.com/showroom/file/event_database.csv")

            # ------------------------------------------------------------
            # 実行ボタン
            # ------------------------------------------------------------
            # ------------------------------------------------------------
            # 改良版：イベントDB更新処理（Duplicate ID対策込み）
            # ------------------------------------------------------------

            # ✅ 一時入力（セッションステートを使ってリロード時も保持）
            if "tmp_start_id" not in st.session_state:
                st.session_state.tmp_start_id = 40290
            if "tmp_end_id" not in st.session_state:
                st.session_state.tmp_end_id = 40300
            if "scan_start_id" not in st.session_state:
                st.session_state.scan_start_id = st.session_state.tmp_start_id
            if "scan_end_id" not in st.session_state:
                st.session_state.scan_end_id = st.session_state.tmp_end_id

            # --- ID入力欄（別keyを明示して重複回避） ---
            tmp_start_id = st.number_input(
                "スキャン開始イベントID",
                min_value=1,
                value=st.session_state.tmp_start_id,
                step=1,
                key="input_start_id_unique"
            )
            tmp_end_id = st.number_input(
                "スキャン終了イベントID",
                min_value=tmp_start_id,
                value=max(st.session_state.tmp_end_id, tmp_start_id + 1),
                step=1,
                key="input_end_id_unique"
            )

            col_confirm, col_run = st.columns([1, 2])
            with col_confirm:
                if st.button("📝 ID範囲を確定", key="confirm_id_range"):
                    st.session_state.tmp_start_id = tmp_start_id
                    st.session_state.tmp_end_id = tmp_end_id
                    st.session_state.scan_start_id = tmp_start_id
                    st.session_state.scan_end_id = tmp_end_id
                    st.success(f"範囲を確定しました: {tmp_start_id}〜{tmp_end_id}")

            start_id = st.session_state.scan_start_id
            end_id = st.session_state.scan_end_id

            # --- その他設定（重複防止のためkeyをすべて指定） ---
            max_workers = st.number_input(
                "並列処理数", 
                min_value=1, 
                max_value=30, 
                value=3, 
                key="num_workers_unique"
            )
            save_interval = st.number_input(
                "途中保存間隔（件）", 
                min_value=50, 
                value=200, 
                step=50, 
                key="save_interval_unique"
            )
            ftp_path = st.text_input(
                "FTP保存パス", 
                value="/mksoul-pro.com/showroom/file/event_database.csv",
                key="ftp_path_unique"
            )

            with col_run:
                run_clicked = st.button("🔄 イベントDB更新開始", key="run_db_update_unique")

            # --- 実行処理 ---
            if run_clicked:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                st.info(f"イベントID {start_id}〜{end_id} の範囲でデータ収集を開始します。")
                st.caption("※処理開始後すぐに進捗バーが表示されます。")

                progress = st.progress(0)
                managed_rooms = pd.read_csv(ROOM_LIST_URL, dtype=str)

                def process_event(event_id):
                    """イベント単位で room_list を処理"""
                    recs = []
                    entries = []
                    page = 1
                    while True:
                        data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": page})
                        if not data or "list" not in data:
                            break
                        entries.extend(data["list"])
                        if not data.get("next_page"):
                            break
                        page += 1
                        time.sleep(0.03)

                    if not entries:
                        return []

                    managed_ids = set(managed_rooms["ルームID"].astype(str))
                    matched = [e for e in entries if str(e.get("room_id")) in managed_ids]
                    if not matched:
                        return []

                    detail = None
                    for e in matched:
                        rid = str(e.get("room_id"))
                        data2 = http_get_json(API_CONTRIBUTION, params={"event_id": event_id, "room_id": rid})
                        if data2 and "event" in data2:
                            detail = data2["event"]
                            break

                    for e in matched:
                        rid = str(e.get("room_id"))
                        rank = e.get("rank") or e.get("position") or "-"
                        point = e.get("point") or e.get("total_point") or 0
                        quest = e.get("event_entry", {}).get("quest_level") if isinstance(e.get("event_entry"), dict) else e.get("quest_level") or 0
                        recs.append({
                            "PR対象": "",
                            "ライバー名": e.get("room_name", ""),
                            "アカウントID": e.get("account_id", ""),
                            "イベント名": detail.get("event_name") if detail else "",
                            "開始日時": fmt_time(detail.get("started_at")) if detail else "",
                            "終了日時": fmt_time(detail.get("ended_at")) if detail else "",
                            "順位": rank,
                            "ポイント": point,
                            "備考": "",
                            "紐付け": "○",
                            "URL": detail.get("event_url") if detail else "",
                            "レベル": quest,
                            "event_id": str(event_id),
                            "ルームID": rid,
                            "イベント画像（URL）": (detail.get("image") if detail else "")
                        })
                    return recs

                ids_to_process = list(range(int(start_id), int(end_id) + 1))
                all_records = []
                total = len(ids_to_process)
                done = 0

                with ThreadPoolExecutor(max_workers=int(max_workers)) as ex:
                    futures = {ex.submit(process_event, eid): eid for eid in ids_to_process}
                    for fut in as_completed(futures):
                        eid = futures[fut]
                        try:
                            recs = fut.result()
                            if recs:
                                all_records.extend(recs)
                        except Exception as e:
                            st.error(f"event_id={eid}: {e}")
                        done += 1
                        progress.progress(done / total)

                if not all_records:
                    st.warning("収集結果は空です。")
                else:
                    df_new = pd.DataFrame(all_records)
                    try:
                        existing_df = load_event_db(EVENT_DB_URL)
                    except Exception:
                        existing_df = pd.DataFrame()

                    merged_df = existing_df.copy()
                    for _, new_row in df_new.iterrows():
                        key = (str(new_row["event_id"]), str(new_row["ルームID"]))
                        mask = (merged_df["event_id"].astype(str) == key[0]) & (merged_df["ルームID"].astype(str) == key[1])
                        if mask.any():
                            idx = merged_df[mask].index[0]
                            for c in ["順位", "ポイント", "レベル", "備考"]:
                                val = new_row[c]
                                if pd.notna(val) and str(val).strip() != "":
                                    merged_df.at[idx, c] = val
                        else:
                            merged_df = pd.concat([merged_df, pd.DataFrame([new_row])], ignore_index=True)

                    merged_df["event_id_num"] = pd.to_numeric(merged_df["event_id"], errors="coerce")
                    merged_df.sort_values(["event_id_num", "ルームID"], ascending=[False, True], inplace=True)
                    merged_df.drop(columns=["event_id_num"], inplace=True)

                    csv_bytes = merged_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    try:
                        ftp_upload_bytes(ftp_path, csv_bytes)
                        st.success(f"✅ 更新完了: {len(merged_df)} 件を保存しました。")
                    except Exception as e:
                        st.warning(f"FTPアップロード失敗: {e}")
                        st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_database.csv")


                        
    # 4. プルダウンフィルタの適用
    if selected_end_date != "全期間":
        df_filtered = df_filtered[df_filtered["終了日時"].str.startswith(selected_end_date)].copy()
    if selected_start_date != "全期間":
        df_filtered = df_filtered[df_filtered["開始日時"].str.startswith(selected_start_date)].copy()
        
    # 4.5. ライバー名の最新化 (APIから取得し、キャッシュ)
    unique_room_ids = [rid for rid in df_filtered["ルームID"].unique() if rid and str(rid) != '']
    room_ids_to_fetch = [rid for rid in unique_room_ids if str(rid) not in st.session_state.room_name_cache]

    if room_ids_to_fetch:
        # ライバーモードの挙動に合わせ、spinnerを削除
        for room_id_val in room_ids_to_fetch:
            room_id_str = str(room_id_val)
            name = get_room_name(room_id_str)
            if name:
                st.session_state.room_name_cache[room_id_str] = name
            time.sleep(0.05) # API負荷軽減

    df_filtered["__display_liver_name"] = df_filtered.apply(
        lambda row: st.session_state.room_name_cache.get(str(row["ルームID"])) or row["ライバー名"], 
        axis=1
    )
    # -------------------------------------------------------------------


    # 6. ソート (終了日時が新しいものが上)
    df_filtered.sort_values("__end_ts", ascending=False, na_position='last', inplace=True)
    
    # 7. 表示整形
    disp_cols = ["ライバー名", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
    df_show = df_filtered[disp_cols + ["is_ongoing", "is_end_today", "URL", "ルームID", "__display_liver_name"]].copy()

    if df_show.empty:
        st.warning("フィルタリング条件に合うデータが見つかりません。")
        st.stop()
        
elif room_id != "":
    # --- ライバーモードのデータ処理（既存ロジックを維持）---
    
    # 1. フィルタリング (ルームID)
    df = df_all[df_all["ルームID"].astype(str) == str(room_id)].copy()
    if df.empty:
        room_name = get_room_name(room_id)
        st.warning(f"ルームID: {room_id} (ルーム名: {room_name}) のデータが見つかりません。")
        st.stop()
        
    # 2. 日付整形とタイムスタンプ追加
    df["開始日時"] = df["開始日時"].apply(fmt_time)
    df["終了日時"] = df["終了日時"].apply(fmt_time)
    df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
    df["__end_ts"] = df["終了日時"].apply(parse_to_ts)
    df = df.sort_values("__start_ts", ascending=False)
    
    # 3. 2023年9月1日以降のイベントにフィルタリング
    df = df[df["__start_ts"] >= FILTER_START_TS].copy()
    
    # 4. 開催中判定
    now_ts = int(datetime.now(JST).timestamp())
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts)

    # 5. 開催中イベント最新化 (ライバーモードは実行時に自動最新化)
    ongoing = df[df["is_ongoing"]]
    for idx, row in ongoing.iterrows():
        event_id = row.get("event_id")
        stats = get_event_stats_from_roomlist(event_id, room_id)
        if stats:
            # ライバーモードはローカルの df を更新
            df.at[idx, "順位"] = stats.get("rank") or "-"
            df.at[idx, "ポイント"] = stats.get("point") or 0
            df.at[idx, "レベル"] = stats.get("quest_level") or 0
        time.sleep(0.1)
    
    # 6. ポイントランキングを計算し、ハイライトCSSを決定するロジック
    df['__point_num'] = pd.to_numeric(df['ポイント'], errors='coerce')
    df_valid_points = df.dropna(subset=['__point_num']).copy()
    df_valid_points['__rank'] = df_valid_points['__point_num'].rank(method='dense', ascending=False)
    df['__highlight_style'] = ''
    for rank, style in HIGHLIGHT_COLORS.items():
        if not df_valid_points.empty:
            target_indices = df_valid_points[df_valid_points['__rank'] == rank].index
            if not target_indices.empty:
                df.loc[target_indices, '__highlight_style'] = style
    
    # 7. ソートの適用
    if st.session_state.sort_by_point:
        df.sort_values(
            ['__point_num', '__start_ts'], 
            ascending=[False, False], 
            na_position='last', 
            inplace=True
        )

    # 8. UI/表示整形
    # ライバー名表示のカスタムCSS定義 (既存ロジックを維持)
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
        color: #0b66c2;
        font-size: 17px;
    }
    .room-label-box a {
        color: inherit;
        font-weight: 700;
        text-decoration: underline;
    }
    </style>
    """, unsafe_allow_html=True)
    
    room_name = get_room_name(room_id)
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

    disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
    df_show = df[disp_cols + ["is_ongoing", "__highlight_style", "URL", "ルームID"]].copy()

# ----------------------------------------------------------------------
# HTMLテーブル生成関数 (ライバーモード用 - 修正なし)
# ----------------------------------------------------------------------
def make_html_table_user(df, room_id):
    """ライバー用HTMLテーブルを生成（貢献ランクボタン風リンクあり、ポイントハイライトあり、開催中黄色ハイライト）"""
    html = """
    <style>
    .scroll-table { max-height: 520px; overflow-y: auto; border: 1px solid #ddd; border-radius: 6px; text-align: center; width: 100%; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; table-layout: fixed; }
    thead th { position: sticky; top: 0; background: #0b66c2; color: #fff; padding: 5px; text-align: center; border: 1px solid #0b66c2; z-index: 10; }
    tbody td { padding: 5px; border-bottom: 1px solid #f2f2f2; text-align: center; vertical-align: middle; word-wrap: break-word; }
    table col:nth-child(1) { width: 46%; } table col:nth-child(2) { width: 11%; } table col:nth-child(3) { width: 11%; } 
    table col:nth-child(4) { width: 6%; } table col:nth-child(5) { width: 9%; } table col:nth-child(6) { width: 6%; } 
    table col:nth-child(7) { width: 11%; } 
    tr.ongoing{background:#fff8b3;}
    a.evlink{color:#0b57d0;text-decoration:underline;}
    .rank-btn-link { background:#0b57d0; color:white !important; border:none; padding:4px 6px; border-radius:4px; cursor:pointer; text-decoration:none; display: inline-block; font-size: 12px; }
    
    table tbody td:nth-child(1) {
        text-align: left;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    a.evlink {
        color:#0b57d0;
        text-decoration:underline;
        display: block;
        width: 100%;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    </style>
    <div class="scroll-table"><table>
    <colgroup><col><col><col><col><col><col><col></colgroup>
    <thead><tr>
    <th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th><th>貢献ランク</th>
    </tr></thead><tbody>
    """
    for _, r in df.iterrows():
        cls = "ongoing" if r.get("is_ongoing") else ""
        url_value = r.get("URL")
        url = url_value if pd.notna(url_value) and url_value else ""
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


        html += f'<tr class="{cls}">'
        html += f"<td>{event_link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td>{point_td}<td>{r['レベル']}</td><td>{button_html}</td>"
        html += "</tr>"
        
    html += "</tbody></table></div>"
    return html

# ----------------------------------------------------------------------
# HTMLテーブル生成関数 (管理者モード用 - 修正なし)
# ----------------------------------------------------------------------
def make_html_table_admin(df):
    """管理者用HTMLテーブルを生成（ライバー名列あり、ポイントハイライトなし、終了当日ハイライトあり）"""
    
    # END_TODAY_HIGHLIGHTからカラーコードを抽出し、CSSの二重定義を回避
    end_today_color_code = END_TODAY_HIGHLIGHT.replace('background-color: ', '').replace(';', '')
    
    # URL/貢献ランク列を削除した7列構成
    html = f"""
    <style>
    .scroll-table {{ max-height: 520px; overflow-y: auto; border: 1px solid #ddd; border-radius: 6px; text-align: center; width: 100%; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; table-layout: fixed; }}
    thead th {{ position: sticky; top: 0; background: #0b66c2; color: #fff; padding: 5px; text-align: center; border: 1px solid #0b66c2; z-index: 10; }}
    tbody td {{ padding: 5px; border-bottom: 1px solid #f2f2f2; text-align: center; vertical-align: middle; word-wrap: break-word; }}
    /* 管理者用: カラム幅の指定（URL列削除に合わせて調整） */
    table col:nth-child(1) {{ width: 16%; }} /* ライバー名 */
    table col:nth-child(2) {{ width: 38%; }} /* イベント名 */
    table col:nth-child(3) {{ width: 11%; }} /* 開始日時 */
    table col:nth-child(4) {{ width: 11%; }} /* 終了日時 */
    table col:nth-child(5) {{ width: 6%; }}  /* 順位 */
    table col:nth-child(6) {{ width: 12%; }} /* ポイント */
    table col:nth-child(7) {{ width: 6%; }}  /* レベル */
    
    /* 修正: background-colorプロパティを正しく適用 */
    tr.end_today{{background-color:{end_today_color_code};}} /* 終了日時当日ハイライト */
    tr.ongoing{{background:#fff8b3;}} /* 開催中黄色ハイライト */
    a.evlink{{color:#0b57d0;text-decoration:underline;}}
    .rank-btn-link {{ background:#0b57d0; color:white !important; border:none; padding:4px 6px; border-radius:4px; cursor:pointer; text-decoration:none; display: inline-block; font-size: 12px; }}
    .liver-link {{ color:#0b57d0; text-decoration:underline; }}

    /* ライバー名 (1列目) とイベント名 (2列目) の省略表示設定 */
    table tbody td:nth-child(1),
    table tbody td:nth-child(2) {{ 
        text-align: left;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}
    a.evlink, .liver-link {{
        color:#0b57d0;
        text-decoration:underline;
        display: block;
        width: 100%;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}
    
    </style>
    <div class="scroll-table"><table>
    <colgroup><col><col><col><col><col><col><col></colgroup>
    <thead><tr>
    <th>ライバー名</th><th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th>
    </tr></thead><tbody>
    """
    for _, r in df.iterrows():
        # ハイライトクラス決定: 終了当日が優先、そうでなければ開催中
        cls = "end_today" if r.get("is_end_today") else ("ongoing" if r.get("is_ongoing") else "")

        url_value = r.get("URL")
        room_id_value = r.get("ルームID")
        
        url = url_value if pd.notna(url_value) and url_value else ""
        room_id = room_id_value if pd.notna(room_id_value) and room_id_value else ""

        name = r.get("イベント名") or ""
        liver_name = r.get("__display_liver_name") or r.get("ライバー名") or ""
        
        point_raw = r.get('ポイント')
        point = f"{float(point_raw):,.0f}" if pd.notna(point_raw) and str(point_raw) not in ('-', '') else str(point_raw or '')
        
        event_link = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name
        
        # ライバー名リンク (別タブ)
        liver_link_url = f"https://www.showroom-live.com/room/profile?room_id={room_id}"
        liver_link = f'<a class="liver-link" href="{liver_link_url}" target="_blank">{liver_name}</a>' if room_id else liver_name

        html += f'<tr class="{cls}">'
        html += f"<td>{liver_link}</td><td>{event_link}</td><td>{r['開始日時']}</td><td>{r['終了日時']}</td>"
        html += f"<td>{r['順位']}</td><td>{point}</td><td>{r['レベル']}</td>"
        html += "</tr>"
        
    html += "</tbody></table></div>"
    return html


# ----------------------------------------------------------------------
# ★★★ 表示（管理者/ライバーで分岐） ★★★
# ----------------------------------------------------------------------
if is_admin:
    # 管理者モードの表示
    st.markdown(make_html_table_admin(df_show), unsafe_allow_html=True)
    
    end_today_color = END_TODAY_HIGHLIGHT.replace('background-color: ', '').replace(';', '')
    st.caption(f"黄色行は開催中（終了日時が未来）のイベントです。赤っぽい行（{end_today_color}）は終了日時が今日当日のイベントです。")
    
    # CSVダウンロード
    cols_to_drop = [c for c in ["is_ongoing", "is_end_today", "__point_num", "URL", "ルームID", "__display_liver_name"] if c in df_show.columns]
    csv_bytes = df_show.drop(columns=cols_to_drop).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history_admin.csv", key="admin_csv_download")

else:
    # ライバーモードの表示 (既存ロジック)
    
    # ソートボタンの表示
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
    
    st.markdown(make_html_table_user(df_show, room_id), unsafe_allow_html=True)
    st.caption("黄色行は現在開催中（終了日時が未来）のイベントです。")

    # CSV出力
    cols_to_drop = [c for c in ["is_ongoing", "__highlight_style", "URL", "ルームID"] if c in df_show.columns]
    csv_bytes = df_show.drop(columns=cols_to_drop).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv", key="user_csv_download")