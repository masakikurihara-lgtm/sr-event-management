import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime, timedelta
import pytz
import re # URL解析のためにreモジュールを追加
import numpy as np # pandasでNaNを扱うために追記
import logging

JST = pytz.timezone("Asia/Tokyo")

EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"  #認証用
EVENT_DB_ADD_URL = "https://mksoul-pro.com/showroom/file/event_database_add.csv"
ROOM_LIST_ADD_URL = "https://mksoul-pro.com/showroom/file/room_list_add.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.4)"}

if "authenticated" not in st.session_state:  #認証用
    st.session_state.authenticated = False  #認証用

st.set_page_config(page_title="SHOWROOM 参加イベントビューア", layout="wide")

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


# =========================================================
# イベント情報（順位・ポイント・レベル）取得関数（全ページ対応版）
# =========================================================
def get_event_stats_from_roomlist(event_id, room_id):
    """
    指定イベント内の特定ルームの順位・ポイント・レベルを取得する。
    全ページをスキャンして該当ルームを検索する。
    """
    page = 1
    found_entry = None

    while True:
        data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": page})
        if not data or "list" not in data:
            break

        entries = data.get("list", [])
        if not entries:
            break

        # 対象ルームを検索
        for entry in entries:
            rid = str(entry.get("room_id"))
            if rid == str(room_id):
                found_entry = entry
                break

        # 見つかったら即終了
        if found_entry:
            break

        # 次ページ判定
        if found_entry or len(entries) < 50 or not data.get("next_page"):
            break

        page += 1
        time.sleep(0.05)  # API負荷軽減

    if not found_entry:
        return None

    # 結果を整形して返す
    return {
        "rank": found_entry.get("rank") or found_entry.get("position"),
        "point": (
            found_entry.get("point")
            or found_entry.get("event_point")
            or found_entry.get("total_point")
        ),
        "quest_level": (
            found_entry.get("quest_level")
            or (found_entry.get("event_entry", {}) or {}).get("quest_level")
        ),
    }



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
    st.session_state.room_input_value = st.session_state.room_id_input
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
st.markdown(
    "<h1 style='font-size:28px; text-align:left; color:#1f2937;'>🎤 SHOWROOM 参加イベントビューア</h1>",
    unsafe_allow_html=True
)
#st.markdown("<h1 style='font-size:2.5em;'>🎤 SHOWROOM 参加イベントビューア</h1>", unsafe_allow_html=True)
#st.title("🎤 SHOWROOM 参加イベントビューア")
st.write("")


# ▼▼ 認証ステップ ▼▼
if not st.session_state.authenticated:
    st.markdown("##### 🔑 認証コードを入力してください")
    input_room_id = st.text_input(
        "認証コードを入力してください:",
        placeholder="",
        type="password",
        key="room_id_input"
    )

    # 認証ボタン
    if st.button("認証する"):
        if input_room_id:  # 入力が空でない場合のみ
            try:
                response = requests.get(ROOM_LIST_URL, timeout=5)
                response.raise_for_status()
                room_df = pd.read_csv(io.StringIO(response.text), header=None)

                valid_codes = set(str(x).strip() for x in room_df.iloc[:, 0].dropna())

                if input_room_id.strip() in valid_codes:
                    st.session_state.authenticated = True
                    st.success("✅ 認証に成功しました。ツールを利用できます。")
                    st.rerun()  # 認証成功後に再読み込み
                else:
                    st.error("❌ 認証コードが無効です。正しい認証コードを入力してください。")
            except Exception as e:
                st.error(f"認証リストを取得できませんでした: {e}")
        else:
            st.warning("認証コードを入力してください。")

    # 認証が終わるまで他のUIを描画しない
    st.stop()
# ▲▲ 認証ステップここまで ▲▲


st.text_input(
    "表示するルームIDを入力してください:", 
    value=st.session_state.room_input_value, 
    key="room_id_input",
    type="password"
    #on_change=save_room_id
)

if st.button("表示する", on_click=trigger_show_data, key="show_data_button"):
    pass 

room_id = st.session_state.room_input_value.strip()
is_admin = (room_id == "mksp154851")
do_show = st.session_state.show_data and room_id != ""


# =========================================================
# 【追加】登録ユーザー判定 ("touroku"で始まる入力)
# =========================================================
is_touroku = room_id.startswith("touroku")

if is_touroku:
    # 「touroku」を除いたルームIDに変換
    room_id = room_id.replace("touroku", "", 1)
    # 登録ユーザー用DB/ルームリストを使用
    EVENT_DB_ACTIVE_URL = EVENT_DB_ADD_URL
    ROOM_LIST_ACTIVE_URL = ROOM_LIST_ADD_URL
else:
    # 既存（管理者・通常）ルートを維持
    EVENT_DB_ACTIVE_URL = EVENT_DB_URL
    ROOM_LIST_ACTIVE_URL = ROOM_LIST_URL


if not do_show:
    if room_id == "":
        # st.info("ルームIDを入力して「表示する」を押してください。") # ライバーモードの挙動に合わせ、infoを削除
        pass
    st.stop()

# ----------------------------------------------------------------------
# データ取得
# ----------------------------------------------------------------------

# 🎯 常に最新CSVを取得する（セッションキャッシュを無効化）
if st.session_state.get("refresh_trigger", False) or "df_all" not in st.session_state:
    df_all = load_event_db(EVENT_DB_ACTIVE_URL)
    st.session_state.df_all = df_all
    st.session_state.refresh_trigger = False
else:
    df_all = st.session_state.df_all.copy()

if st.session_state.df_all.empty:
    st.stop()

df_all = st.session_state.df_all.copy()  # コピーを使用

# ----------------------------------------------------------------------
# 管理者モード専用: 読み込み直後に「終了日時が10日前以降」で打ち切り
# ----------------------------------------------------------------------
if is_admin and not st.session_state.admin_full_data:
    filtered_rows = []
    cutoff_ts = FILTER_END_DATE_TS_DEFAULT  # 10日前の0時基準
    for _, row in df_all.iterrows():
        end_ts = parse_to_ts(row.get("終了日時"))
        # 空なら暫定的に残す
        if not end_ts or pd.isna(end_ts):
            filtered_rows.append(row)
            continue
        # 終了日時が10日前以降なら残す
        if end_ts >= cutoff_ts:
            filtered_rows.append(row)
        else:
            # CSVが終了日時降順になっているため、ここで終了
            break
    df_all = pd.DataFrame(filtered_rows)

# ----------------------------------------------------------------------
# 以下、既存の分岐処理に続く（ライバーモードへの影響なし）
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# データのフィルタリングと整形 (管理者/ライバーで分岐)
# ----------------------------------------------------------------------

if is_admin:
    # --- 管理者モードのデータ処理 ---
    import time
    t0 = time.time()  # ← 計測開始

    df = df_all.copy()

    # ✅ 「全量表示OFF」のときのみ10日前フィルタを適用
    if not st.session_state.get("admin_full_data", False):
        cutoff_ts = FILTER_END_DATE_TS_DEFAULT  # 10日前の基準TS
        rows_recent = []
        for _, row in df.iterrows():
            end_ts = parse_to_ts(row.get("終了日時"))
            # 空 or 10日前以降のみ残す（CSVが降順ソート済みのため、古くなったらbreak）
            if not end_ts or pd.isna(end_ts) or end_ts >= cutoff_ts:
                rows_recent.append(row)
            else:
                break
        df = pd.DataFrame(rows_recent)

    # ✅ デバッグ出力：残った件数を確認
    # st.info(f"デバッグ: フィルタ後の件数 = {len(df)} 件")


    # ✅ 残った70件程度にのみ fmt_time / parse_to_ts を実行
    df["開始日時"] = df["開始日時"].apply(fmt_time)
    df["終了日時"] = df["終了日時"].apply(fmt_time)
    df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
    df["__end_ts"] = df["終了日時"].apply(parse_to_ts)

    # ✅ 処理時間の計測結果を表示
    elapsed = time.time() - t0
    # st.info(f"デバッグ: 管理者モード初期処理完了 ({len(df)} 件, {elapsed:.2f} 秒)")

    # --- デバッグステップ2: 各処理時間をログ出力 ---
    t1 = time.time()
    now_ts = int(datetime.now(JST).timestamp())
    today_ts = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts - 3600)
    df["is_end_today"] = df["__end_ts"].apply(lambda x: pd.notna(x) and today_ts <= x < (today_ts + 86400))
    # st.info(f"デバッグ: 開催中判定完了 ({time.time() - t1:.2f} 秒)")

    # ★★★ 修正 (5. 開催中イベント最新化 高速化版) ★★★
    start_time = time.time()
    ongoing = df[df["is_ongoing"]]
    # st.info(f"デバッグ: 開催中イベント数 = {len(ongoing)}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def update_event_stats(row):
        event_id = row.get("event_id")
        room_id_to_update = row.get("ルームID")
        stats = get_event_stats_from_roomlist(event_id, room_id_to_update)
        if stats:
            return (row.name, stats)
        return None

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:  # 並列8スレッド
        futures = [executor.submit(update_event_stats, row) for _, row in ongoing.iterrows()]
        for future in as_completed(futures):
            res = future.result()
            if res:
                idx, stats = res
                # ✅ df_all と df の両方を同期更新
                for target_df in [st.session_state.df_all, df]:
                    target_df.at[idx, "順位"] = stats.get("rank") or "-"
                    target_df.at[idx, "ポイント"] = stats.get("point") or 0
                    target_df.at[idx, "レベル"] = stats.get("quest_level") or 0

    # ✅ 処理結果をセッション全体に反映
    st.session_state.df_all.update(df)

    elapsed = time.time() - start_time
    # st.info(f"デバッグ: 開催中イベント最新化完了 ({elapsed:.2f} 秒)")

    # --- 以下フィルタリング・UI生成部 ---
    t3 = time.time()
    df_filtered = df.copy()
    df_filtered = df_filtered[
        (df_filtered["__start_ts"].apply(lambda x: pd.notna(x) and x >= FILTER_START_TS))
        | (df_filtered["__start_ts"].isna())
    ].copy()

    if not st.session_state.admin_full_data:
        df_filtered = df_filtered[
            (df_filtered["__end_ts"].apply(lambda x: pd.notna(x) and x >= FILTER_END_DATE_TS_DEFAULT))
            | (df_filtered["__end_ts"].isna())
        ].copy()

    # st.info(f"デバッグ: 絞り込み後 = {len(df_filtered)} 件 ({time.time() - t3:.2f} 秒)")

    # 終了日時フィルタリング用の選択肢生成
    #unique_end_dates = sorted(
    #    list(set(df_filtered["終了日時"].apply(lambda x: x.split(' ')[0] if x else '')) - {''}), 
    #    reverse=True
    #)
    
    # 開始日時フィルタリング用の選択肢生成
    #unique_start_dates = sorted(
    #    list(set(df_filtered["開始日時"].apply(lambda x: x.split(' ')[0] if x else '')) - {''}), 
    #    reverse=True
    #)

    # ---------------------------------------------
    # 終了日時フィルタリング用の選択肢生成
    # ★★★ 最終修正: applyとsetを避け、Pandasのstrメソッドを使用して高速化する ★★★
    # ---------------------------------------------
    t4 = time.time() # デバッグ開始
    
    df_dates = df_filtered["終了日時"].astype(str)
    # 日時文字列（例: '2025-10-10 10:00:00'）から日付部分 '2025-10-10' を抽出
    unique_end_dates = sorted(
        list(df_dates.str.split(' ', n=1, expand=True)[0].unique()), # n=1で高速化
        reverse=True
    )
    # 空文字列をセットから除外
    unique_end_dates = [d for d in unique_end_dates if d != '']
    
    # st.info(f"デバッグ: 終了日時選択肢生成完了 ({time.time() - t4:.2f} 秒)")


    # ---------------------------------------------
    # 開始日時フィルタリング用の選択肢生成
    # ★★★ 最終修正: 同様にPandasのstrメソッドを使用して高速化する ★★★
    # ---------------------------------------------
    t5 = time.time() # デバッグ開始

    df_dates = df_filtered["開始日時"].astype(str)
    # 日時文字列（例: '2025-10-10 10:00:00'）から日付部分 '2025-10-10' を抽出
    unique_start_dates = sorted(
        list(df_dates.str.split(' ', n=1, expand=True)[0].unique()), # n=1で高速化
        reverse=True
    )
    # 空文字列をセットから除外
    unique_start_dates = [d for d in unique_start_dates if d != '']
    
    # st.info(f"デバッグ: 開始日時選択肢生成完了 ({time.time() - t5:.2f} 秒)")


    
    # ... (以降のUI描画ブロック) ...


    # ✅ UI描画ブロックをspinnerで囲む
    # with st.spinner("🎨 イベント一覧を描画中...（約15秒）"):
    # 3. UIコンポーネント (フィルタ、最新化ボタン)
    with st.expander("⚙️ 個別機能・絞り込みオプション"):

        

        # ============================================================
        # 🧭 管理者専用：イベントDB更新機能（既存履歴ビューアと独立動作）
        # ============================================================
        if is_admin:
            #st.markdown("---")
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
            # イベントDB範囲確認（既存機能）
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
                            time.sleep(0.1)
                        if latest_id:
                            st.success(f"SHOWROOM開催予定イベントの最新ID: {latest_id}")
                        else:
                            st.warning("取得できませんでした。")
                    except Exception as e:
                        st.error(f"API取得失敗: {e}")

            st.markdown("---")
            st.markdown("#### 🚀 データベース更新実行")

            start_id = st.number_input("スキャン開始イベントID", min_value=1, value=40500, step=1)
            end_id = st.number_input("スキャン終了イベントID", min_value=start_id, value=start_id + 500, step=1)
            max_workers = st.number_input("並列処理数", min_value=1, max_value=30, value=4)
            save_interval = st.number_input("途中保存間隔（件）", min_value=50, value=300, step=50)


            # ------------------------------------------------------------
            # ✨追加：特定ルーム限定更新機能
            # ------------------------------------------------------------
            st.markdown("---")
            st.markdown("#### 🎯 特定ルームID限定でイベントDB更新（オプション）")
            target_room_input = st.text_input("ルームIDを指定（カンマ区切りで複数指定可）", placeholder="例: 123456,789012")

            # ------------------------------------------------------------
            # 実行ボタン（全体更新 or 限定更新）
            # ------------------------------------------------------------
            run_col1, run_col2 = st.columns(2)

            # --- 共通ユーティリティ：event_list API を全ページ走査して対象 entries を返す
            def fetch_all_pages_entries(event_id, filter_ids=None):
                """
                event_id の room_list API を全ページ走査して、filter_ids に含まれる room_id の entries を返す。
                filter_ids が None の場合は全 entries を返す。
                """
                entries = []
                page = 1
                seen_pages = set()

                while True:
                    data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": page})
                    if not data or "list" not in data:
                        break

                    # 無限ループ防止：同じページを2回読んだら終了
                    if page in seen_pages:
                        break
                    seen_pages.add(page)

                    page_entries = data["list"]
                    if filter_ids:
                        page_entries = [e for e in page_entries if str(e.get("room_id")) in filter_ids]

                    entries.extend(page_entries)

                    # ✅ 終了条件（最重要）
                    if (
                        not data.get("next_page") or
                        str(data.get("current_page")) == str(data.get("last_page"))
                    ):
                        break

                    page += 1
                    time.sleep(0.03)

                return entries



            # --- 共通関数（全ルーム更新用）: event_id -> recs を返す（管理者用）
            def process_event_full(event_id, managed_ids, target_room_ids=None):
                recs = []

                # 対象ルーム集合の決定
                if target_room_ids:
                    filter_ids = managed_ids & set(target_room_ids)
                else:
                    filter_ids = managed_ids

                # ✅ 全ページから該当ルームを取得（filter_idsが空でも全件読む）
                entries = fetch_all_pages_entries(event_id, filter_ids if len(filter_ids) > 0 else None)

                if not entries:
                    return []

                # イベント詳細をルームごとに取得
                details = {}
                unique_room_ids = {str(e.get("room_id")) for e in entries}
                for rid in unique_room_ids:
                    data2 = http_get_json(API_CONTRIBUTION, params={"event_id": event_id, "room_id": rid})
                    if data2 and isinstance(data2, dict) and "event" in data2:
                        details[rid] = data2["event"]
                    time.sleep(0.03)

                # レコード生成
                for e in entries:
                    rid = str(e.get("room_id"))
                    rank = e.get("rank") or e.get("position") or "-"
                    point = e.get("point") or e.get("total_point") or 0
                    quest = e.get("event_entry", {}).get("quest_level") if isinstance(e.get("event_entry"), dict) else e.get("quest_level") or 0
                    detail = details.get(rid)
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



            # --- 共通関数（登録ユーザー用）: event_id -> recs を返す（add 用） ---
            def process_event_add(event_id, add_room_ids):
                recs = []
                # fetch only add_room_ids entries across pages
                entries = fetch_all_pages_entries(event_id, filter_ids=add_room_ids if add_room_ids else set())
                if not entries:
                    return []

                # get details per room
                details = {}
                unique_room_ids = { str(e.get("room_id")) for e in entries }
                for rid in unique_room_ids:
                    data2 = http_get_json(API_CONTRIBUTION, params={"event_id": event_id, "room_id": rid})
                    if data2 and isinstance(data2, dict) and "event" in data2:
                        details[rid] = data2["event"]
                    time.sleep(0.03)

                for e in entries:
                    rid = str(e.get("room_id"))
                    rank = e.get("rank") or e.get("position") or "-"
                    point = e.get("point") or e.get("total_point") or 0
                    quest = e.get("event_entry", {}).get("quest_level") if isinstance(e.get("event_entry"), dict) else e.get("quest_level") or 0
                    detail = details.get(rid)
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


            # =========================================================
            # 全ルーム更新実行ボタン（最終修正版）
            # =========================================================
            with run_col1:
                ftp_path = "/mksoul-pro.com/showroom/file/event_database.csv"
                st.markdown("")
                st.markdown(f"<div style='color:gray; font-size:12px;'>📂 FTP保存先: {ftp_path}</div>", unsafe_allow_html=True)
                st.markdown("")

                if st.button("🔄 イベントDB更新開始", key="run_db_update"):
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    st.info("データ収集を開始します。")
                    progress = st.progress(0)
                    managed_rooms = pd.read_csv(ROOM_LIST_URL, dtype=str)
                    managed_ids = set(managed_rooms["ルームID"].astype(str))

                    # 指定ルーム入力の解釈（既存機能の維持）
                    target_room_ids_str = [r.strip() for r in target_room_input.split(",") if r.strip()]
                    target_room_ids = set(target_room_ids_str) if target_room_ids_str else None
                    
                    if target_room_ids:
                        st.info(f"✅ 対象ルームを指定して更新します: {', '.join(target_room_ids)}")
                    else:
                        st.info("📡 全ルーム対象で更新します。")

                    # ■■■ 修正：事前スキャンを撤廃し、全イベントIDに対して直接データ取得を実行 ■■■
                    all_records = []
                    event_id_range = list(range(int(start_id), int(end_id) + 1))
                    total = len(event_id_range)
                    done = 0

                    with ThreadPoolExecutor(max_workers=int(max_workers)) as ex:
                        # 全てのイベントIDに対し、データ収集関数を直接呼び出す
                        futures = {ex.submit(process_event_full, eid, managed_ids, target_room_ids): eid for eid in event_id_range}
                        for fut in as_completed(futures):
                            eid = futures[fut]
                            try:
                                # 関数が返したレコード（対象者がいなければ空リスト）を追加
                                recs = fut.result()
                                if recs:  # データが取得できた場合のみ追加
                                    all_records.extend(recs)
                            except Exception as e:
                                st.error(f"event_id={eid} の処理でエラー: {e}")
                            done += 1
                            progress.progress(done / total)
                    # ■■■ 修正ここまで ■■■

                    if not all_records:
                        st.warning("📭 指定条件に一致するイベントデータがありませんでした。")
                        st.stop()

                    # --- 結果マージ・保存処理（変更なし） ---
                    df_new = pd.DataFrame(all_records)
                    try:
                        existing_df = load_event_db(EVENT_DB_URL)
                    except Exception:
                        existing_df = pd.DataFrame()

                    merged_df = existing_df.copy()
                    for col in ["event_id", "ルームID"]:
                        if col in merged_df.columns:
                            merged_df[col] = merged_df[col].astype(str)
                    df_new["event_id"] = df_new["event_id"].astype(str)
                    df_new["ルームID"] = df_new["ルームID"].astype(str)

                    updated_rows = 0
                    added_rows = 0

                    for _, new_row in df_new.iterrows():
                        eid = str(new_row["event_id"])
                        rid = str(new_row["ルームID"])
                        mask = (merged_df["event_id"] == eid) & (merged_df["ルームID"] == rid) if ("event_id" in merged_df.columns and "ルームID" in merged_df.columns) else pd.Series([False]*len(merged_df))
                        if mask.any():
                            idx = mask.idxmax()
                            for col in ["順位", "ポイント", "レベル", "イベント名", "開始日時", "終了日時", "URL"]:
                                merged_df.at[idx, col] = new_row.get(col, merged_df.at[idx, col])
                            updated_rows += 1
                        else:
                            merged_df = pd.concat([merged_df, pd.DataFrame([new_row])], ignore_index=True)
                            added_rows += 1
                    
                    # --- 不要行削除ロジック（修正版） ---
                    scanned_event_ids = set(map(str, event_id_range))
                    new_pairs = set(
                        df_new[["event_id", "ルームID"]]
                        .apply(lambda r: (str(r["event_id"]), str(r["ルームID"])), axis=1)
                        .tolist()
                    )

                    before_count = len(merged_df)

                    def keep_row(row):
                        eid = str(row.get("event_id"))
                        rid = str(row.get("ルームID"))

                        # 🔹 特定ルーム指定時 → 指定ルームのみ削除判定対象
                        if target_room_ids and rid not in target_room_ids:
                            return True  # 他ルームのデータは保持

                        # 🔹 イベントID範囲外 → 常に保持
                        if eid not in scanned_event_ids:
                            return True

                        # 🔹 範囲内のルームで new_pairs に含まれない場合 → 削除対象
                        return (eid, rid) in new_pairs

                    if not merged_df.empty and "event_id" in merged_df.columns and "ルームID" in merged_df.columns:
                        keep_mask = merged_df.apply(keep_row, axis=1)
                        merged_df = merged_df[keep_mask].reset_index(drop=True)

                    deleted_rows = before_count - len(merged_df)
                    
                    if not merged_df.empty and "event_id" in merged_df.columns and "ルームID" in merged_df.columns:
                        keep_mask = merged_df.apply(keep_row, axis=1)
                        merged_df = merged_df[keep_mask].reset_index(drop=True)

                    deleted_rows = before_count - len(merged_df)

                    # --- ソート・保存（終了日時を第一条件に追加） ---
                    # 既存のevent_id_num計算を維持
                    merged_df["event_id_num"] = pd.to_numeric(merged_df["event_id"], errors="coerce")

                    # 📌 修正点 1: 終了日時をタイムスタンプに変換して一時列(__end_ts)に追加（ソート用）
                    merged_df["__end_ts"] = merged_df["終了日時"].apply(parse_to_ts)

                    # 📌 修正点 2: 終了日時（__end_ts）を最優先の降順ソートキーにする
                    # ソート順: [終了日時(降順), イベントID(降順), ルームID(昇順)]
                    merged_df.sort_values(
                        ["__end_ts", "event_id_num", "ルームID"], 
                        ascending=[False, False, True], 
                        inplace=True
                    )

                    # 📌 修正点 3: ソートに使用した一時列を削除
                    merged_df.drop(columns=["event_id_num", "__end_ts"], inplace=True)

                    csv_bytes = merged_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    try:
                        ftp_upload_bytes(ftp_path, csv_bytes)
                        st.success(f"✅ 更新完了: 更新 {updated_rows}件 / 新規追加 {added_rows}件 / 削除 {deleted_rows}件 / 合計 {len(merged_df)} 件を保存しました。")
                    except Exception as e:
                        st.warning(f"FTPアップロード失敗: {e}")
                        st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_database.csv")


            # =========================================================
            # 登録ユーザー用DB更新ボタン（最終修正版）
            # =========================================================
            with run_col2:
                EVENT_DB_ADD_PATH = "/mksoul-pro.com/showroom/file/event_database_add.csv"
                st.markdown("")
                st.markdown(f"<div style='color:gray; font-size:12px;'>📂 FTP保存先: {EVENT_DB_ADD_PATH}</div>", unsafe_allow_html=True)
                st.markdown("")

                if st.button("🧩 登録ユーザーDB更新開始", key="run_add_db_update"):
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    st.info("登録ユーザーのイベントデータ更新を開始します。")
                    progress = st.progress(0)

                    ROOM_LIST_ADD_URL = "https://mksoul-pro.com/showroom/file/room_list_add.csv"
                    
                    df_add_rooms = pd.read_csv(ROOM_LIST_ADD_URL, dtype=str)
                    add_room_ids = set(df_add_rooms["ルームID"].astype(str).tolist())

                    # ■■■ 修正：事前スキャンを撤廃し、全イベントIDに対して直接データ取得を実行 ■■■
                    all_records = []
                    event_id_range = list(range(int(start_id), int(end_id) + 1))
                    total = len(event_id_range)
                    done = 0

                    with ThreadPoolExecutor(max_workers=int(max_workers)) as ex:
                        futures = {ex.submit(process_event_add, eid, add_room_ids): eid for eid in event_id_range}
                        for fut in as_completed(futures):
                            eid = futures[fut]
                            try:
                                recs = fut.result()
                                if recs: # データが取得できた場合のみ追加
                                    all_records.extend(recs)
                            except Exception as e:
                                st.error(f"event_id={eid} の処理でエラー: {e}")
                            done += 1
                            progress.progress(done / total)
                    # ■■■ 修正ここまで ■■■

                    if not all_records:
                        st.warning("📭 登録ユーザーの該当データがありませんでした。")
                        st.stop()

                    # --- 結果マージ・保存処理（変更なし） ---
                    df_new = pd.DataFrame(all_records)
                    try:
                        existing_df = load_event_db(EVENT_DB_ADD_URL)
                    except Exception:
                        existing_df = pd.DataFrame()

                    merged_df = existing_df.copy()
                    for col in ["event_id", "ルームID"]:
                        if col in merged_df.columns:
                            merged_df[col] = merged_df[col].astype(str)
                    df_new["event_id"] = df_new["event_id"].astype(str)
                    df_new["ルームID"] = df_new["ルームID"].astype(str)

                    updated_rows = 0
                    added_rows = 0
                    
                    for _, new_row in df_new.iterrows():
                        eid = str(new_row["event_id"])
                        rid = str(new_row["ルームID"])
                        mask = (merged_df["event_id"] == eid) & (merged_df["ルームID"] == rid) if ("event_id" in merged_df.columns and "ルームID" in merged_df.columns) else pd.Series([False]*len(merged_df))
                        if mask.any():
                            idx = mask.idxmax()
                            for col in ["順位", "ポイント", "レベル", "イベント名", "開始日時", "終了日時", "URL"]:
                                merged_df.at[idx, col] = new_row.get(col, merged_df.at[idx, col])
                            updated_rows += 1
                        else:
                            merged_df = pd.concat([merged_df, pd.DataFrame([new_row])], ignore_index=True)
                            added_rows += 1

                    # --- 不要行削除ロジック（変更なし） ---
                    scanned_event_ids = set(map(str, event_id_range))
                    new_pairs = set(df_new[["event_id", "ルームID"]].apply(lambda r: (str(r["event_id"]), str(r["ルームID"])), axis=1).tolist())

                    before_count = len(merged_df)
                    def keep_row_add(row):
                        eid = str(row.get("event_id"))
                        rid = str(row.get("ルームID"))
                        if eid not in scanned_event_ids:
                            return True
                        return (eid, rid) in new_pairs
                    
                    if not merged_df.empty and "event_id" in merged_df.columns and "ルームID" in merged_df.columns:
                        keep_mask = merged_df.apply(keep_row_add, axis=1)
                        merged_df = merged_df[keep_mask].reset_index(drop=True)
                    
                    deleted_rows = before_count - len(merged_df)

                    # --- ソート・保存（変更なし） ---
                    merged_df["event_id_num"] = pd.to_numeric(merged_df["event_id"], errors="coerce")
                    merged_df.sort_values(["event_id_num", "ルームID"], ascending=[False, True], inplace=True)
                    merged_df.drop(columns=["event_id_num"], inplace=True)

                    csv_bytes = merged_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    try:
                        ftp_upload_bytes(EVENT_DB_ADD_PATH, csv_bytes)
                        st.success(f"✅ 更新完了: 更新 {updated_rows}件 / 新規追加 {added_rows}件 / 削除 {deleted_rows}件 / 合計 {len(merged_df)} 件を保存しました。")
                    except Exception as e:
                        st.warning(f"FTPアップロード失敗: {e}")
                        st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_database_add.csv")



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

        st.markdown("---") # 区切り線
        
                # 1. 最新化ボタン
        st.button(
            "🔄 終了前イベントの最新化", 
            on_click=refresh_data, 
            key="admin_refresh_button"
        )

        st.markdown("---") # 区切り線
        
        # 2. 全量表示トグル
        st.checkbox(
            "全量表示（期間フィルタ無効）", 
            value=st.session_state.admin_full_data,
            key="admin_full_data_checkbox_internal",
            on_change=toggle_full_data
        )

        st.markdown("") #空白行 
        
                                
    # 4. プルダウンフィルタの適用
    if selected_end_date != "全期間":
        df_filtered = df_filtered[df_filtered["終了日時"].str.startswith(selected_end_date)].copy()
    if selected_start_date != "全期間":
        df_filtered = df_filtered[df_filtered["開始日時"].str.startswith(selected_start_date)].copy()
        
    # 4.5. ライバー名の最新化 (APIから取得し、キャッシュ)
    unique_room_ids = [rid for rid in df_filtered["ルームID"].unique() if rid and str(rid) != '']
    room_ids_to_fetch = [rid for rid in unique_room_ids if str(rid) not in st.session_state.room_name_cache]

    if room_ids_to_fetch:
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # st.info(f"デバッグ: ライバー名キャッシュ更新開始 ({len(room_ids_to_fetch)} 件)")
        t_liver_start = time.time()

        def fetch_room_name(room_id_str):
            """個別ルーム名取得（APIラッパ）"""
            name = get_room_name(room_id_str)
            if name:
                return (room_id_str, name)
            return None

        results = []
        # 並列処理で最大8件ずつAPI呼び出し
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch_room_name, str(rid)) for rid in room_ids_to_fetch]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    rid, name = res
                    st.session_state.room_name_cache[rid] = name

        elapsed_liver = time.time() - t_liver_start
        # st.info(f"デバッグ: ライバー名キャッシュ更新完了 ({len(st.session_state.room_name_cache)} 件, {elapsed_liver:.2f} 秒)")
    # else:
        # st.info("デバッグ: ライバー名キャッシュ更新はスキップ（全件キャッシュ済み）")



    df_filtered["__display_liver_name"] = df_filtered.apply(
        lambda row: st.session_state.room_name_cache.get(str(row["ルームID"])) or row["ライバー名"], 
        axis=1
    )
    # -------------------------------------------------------------------

    # 6. ソート (終了日時 → イベントID → ポイント の降順)
    # 「ポイント」は数値化してからソートする
    df_filtered["__point_num"] = pd.to_numeric(df_filtered["ポイント"], errors="coerce").fillna(0)

    df_filtered.sort_values(
        ["__end_ts", "event_id", "__point_num"],  # 第3条件にポイント列を追加
        ascending=[False, False, False],          # すべて降順
        na_position='last',
        inplace=True
    )
    
    # 7. 表示整形（イベントID・ルームIDを末尾に追加）
    disp_cols = ["ライバー名", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "event_id", "ルームID"]

    # event_id が存在しない場合の防御
    if "event_id" not in df_filtered.columns:
        df_filtered["event_id"] = ""

    df_show = df_filtered[disp_cols + ["is_ongoing", "is_end_today", "URL", "__display_liver_name"]].copy()

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
    #df = df.sort_values("__start_ts", ascending=False)
    df = df.sort_values("__end_ts", ascending=False)
    
    # 3. 2023年9月1日以降のイベントにフィルタリング
    df = df[df["__start_ts"] >= FILTER_START_TS].copy()
    
    # 4. 開催中判定
    now_ts = int(datetime.now(JST).timestamp())
    # 修正前: df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts)
    df["is_ongoing"] = df["__end_ts"].apply(lambda x: pd.notna(x) and x > now_ts - 3600) # ★★★ 修正後 ★★★

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
        margin-bottom: 0px;
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
    
    
    # ===============================
    # 🔍 プロフィール情報取得と表示
    # ===============================
    try:
        prof_res = requests.get(f"https://www.showroom-live.com/api/room/profile?room_id={room_id}", headers=HEADERS, timeout=6)
        if prof_res.status_code == 200:
            prof_json = prof_res.json()
            room_level = prof_json.get("room_level", "-")
            show_rank = prof_json.get("show_rank_subdivided", "-")
            follower_num = prof_json.get("follower_num", "-")
            live_cont_days = prof_json.get("live_continuous_days", "-")

            # テーブル形式で表示
            st.markdown("""
            <style>
            .profile-table {
                border-collapse: collapse;
                width: 60%;
                margin-bottom: 20px;
                font-size: 14px;
                text-align: center;
            }
            .profile-table th, .profile-table td {
                border: 1px solid #ddd;
                padding: 8px 10px;
                text-align: center !important;
            }
            .profile-table th {
                background-color: #0b66c2;
                color: white;
            }            

            /* ===========================================
               📱 スマートフォン（767px以下）対応CSS
               =========================================== */
            @media screen and (max-width: 767px) {
                /* プロフィールテーブル */
                .profile-table {
                    width: 100% !important;
                    font-size: 12px !important;
                }
                .profile-table th, .profile-table td {
                    padding: 6px !important;
                }

                /* イベントテーブル全体を横スクロール可能に */
                .scroll-table {
                    overflow-x: auto !important;
                    width: 100% !important;
                    display: block;
                    -webkit-overflow-scrolling: touch; /* iPhone慣性スクロール */
                }
                .scroll-table table {
                    width: 1080px !important; /* テーブル幅を固定して横スクロール */
                }

                /* テキストが詰まりすぎないよう微調整 */
                table {
                    font-size: 12px !important;
                }

                /* スマホではボタンを少し大きく */
                .rank-btn-link {
                    padding: 6px 8px !important;
                    font-size: 13px !important;
                }
            }
            
            /* ===========================================
               💻 タブレット（768〜1024px）調整
               =========================================== */
            @media screen and (min-width: 768px) and (max-width: 1024px) {
                .profile-table { width: 80% !important; font-size: 13px !important; }
                .scroll-table table { width: 1280px !important; }
                table { font-size: 13px !important; }
            }
            </style>            
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <table class="profile-table">
                <thead><tr>
                    <th>ルームレベル</th>
                    <th>SHOWランク</th>
                    <th>フォロワー数</th>
                    <th>まいにち配信</th>
                </tr></thead>
                <tbody><tr>
                    <td>{room_level}</td>
                    <td>{show_rank}</td>
                    <td>{follower_num}</td>
                    <td>{live_cont_days} 日</td>
                </tr></tbody>
            </table>
            """, unsafe_allow_html=True)
            #<td>{follower_num:,}</td> # カンマ区切りの記述
    except Exception as e:
        st.warning(f"プロフィール情報を取得できませんでした: {e}")

    

    disp_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
    df_show = df[disp_cols + ["is_ongoing", "__highlight_style", "URL", "ルームID"]].copy()

# ----------------------------------------------------------------------
# HTMLテーブル生成関数 (ライバーモード用 - 修正なし)
# ----------------------------------------------------------------------
def make_html_table_user(df, room_id):
    """ライバー用HTMLテーブルを生成（貢献ランクボタン風リンクあり、ポイントハイライトあり、開催中黄色ハイライト）"""
    html = """
    <style>
    .scroll-table {
    max-height: 520px;
    overflow-y: auto;
    overflow-x: auto;      /* 👈 横スクロールを許可 */
    border: 1px solid #ddd;
    border-radius: 6px;
    text-align: center;
    width: 100%;
    -webkit-overflow-scrolling: touch; /* 👈 iPhoneなどの慣性スクロール対応 */
    }
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
            button_html = f'<a href="{contrib_url}" target="_blank" class="rank-btn-link">貢献ランク</a>'
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
# 必要な関数を *先に定義*
# ----------------------------------------------------------------------

import html
import re
import traceback

def clean_df(df):
    import re
    df = df.copy()

    for c in df.columns:
        # データ型を必ず文字列にしてから処理
        # s: re.sub(r"[\x00-\x1F\x7F\uFFFD\u3000]", "", s)
        df[c] = df[c].astype(str).apply(
            lambda s: re.sub(r"[\x00-\x1F\x7F\uFFFD\u3000]", "", s) # 変更なし
        )
    return df


# ----------------------------------------------------------------------
# HTMLテーブル生成関数 (管理者モード用 - 改良版: 安全化 & 最終サニタイズ)
# ----------------------------------------------------------------------
import html
import re

def make_html_table_admin(df):

    # END_TODAY_HIGHLIGHT から色を抽出
    end_today_color_code = END_TODAY_HIGHLIGHT.replace('background-color: ', '').replace(';', '')


    # HTML ヘッダ（CSS）
    html_output = f"""
    <style>
    .scroll-table {{ max-height: 520px; overflow-y: auto; overflow-x: auto; border: 1px solid #ddd; border-radius: 6px; text-align: center; width: 100%; -webkit-overflow-scrolling: touch; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; table-layout: fixed; }}
    thead th {{ position: sticky; top: 0; background: #0b66c2; color: #fff; padding: 5px; text-align: center; border: 1px solid #0b66c2; z-index: 10; }}
    tbody td {{ padding: 5px; border-bottom: 1px solid #f2f2f2; text-align: center; vertical-align: middle; word-wrap: break-word; }}
    table col:nth-child(1) {{ width: 22%; }} /* ライバー名 */
    table col:nth-child(2) {{ width: 22%; }} /* イベント名 */
    table col:nth-child(3) {{ width: 11%; }} /* 開始日時 */
    table col:nth-child(4) {{ width: 11%; }} /* 終了日時 */
    table col:nth-child(5) {{ width: 5%; }}  /* 順位 */
    table col:nth-child(6) {{ width: 8%; }} /* ポイント */
    table col:nth-child(7) {{ width: 5%; }}  /* レベル */
    table col:nth-child(8) {{ width: 8%; }}  /* イベントID */
    table col:nth-child(9) {{ width: 8%; }}  /* ルームID */
    tr.end_today{{background-color:{end_today_color_code};}} /* 終了日時当日ハイライト */
    tr.ongoing{{background:#fff8b3;}} /* 開催中黄色ハイライト */
    a.evlink{{color:#0b57d0;text-decoration:underline;}}
    .rank-btn-link {{ background:#0b57d0; color:white !important; border:none; padding:4px 6px; border-radius:4px; cursor:pointer; text-decoration:none; display: inline-block; font-size: 12px; }}
    .liver-link {{ color:#0b57d0; text-decoration:underline; }}
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
    <colgroup><col><col><col><col><col><col><col><col><col></colgroup>
    <thead><tr>
    <th>ライバー名</th><th>イベント名</th><th>開始日時</th><th>終了日時</th>
    <th>順位</th><th>ポイント</th><th>レベル</th><th>イベントID</th><th>ルームID</th>
    </tr></thead><tbody>
    """

    # 安全化ユーティリティ
    def safe_text(s):
        if s is None:
            return ""
        s = str(s) # ここで確実に文字列に変換
        # 制御文字・DEL・壊れ文字(U+FFFD)・全角スペース(U+3000)を除去
        # ここで全角スペースを除去
        s = re.sub(r"[\x00-\x1F\x7F\uFFFD\u3000]", "", s)
        return html.escape(s) # HTMLエスケープ

    # テーブル行生成（壊れても例外をログ化して継続）
    for _, r in df.iterrows():
        try:
            cls = "end_today" if r.get("is_end_today") else ("ongoing" if r.get("is_ongoing") else "")

            url = r.get("URL") or ""
            room_id_raw = r.get("ルームID") or ""

            name = safe_text(r.get("イベント名"))
            liver_name = safe_text(r.get("__display_liver_name") or r.get("ライバー名"))
            start_time = safe_text(r.get("開始日時"))
            end_time = safe_text(r.get("終了日時"))
            rank = safe_text(r.get("順位"))
            level = safe_text(r.get("レベル"))
            event_id = safe_text(r.get("event_id"))
            room_id_disp = safe_text(room_id_raw)

            # ポイント整形
            point_raw = r.get("ポイント")
            if pd.notna(point_raw) and str(point_raw) not in ("-", ""):
                try:
                    point = f"{float(point_raw):,.0f}"
                except Exception:
                    point = safe_text(point_raw)
            else:
                point = safe_text(point_raw)

            event_link = f'<a class="evlink" href="{html.escape(url)}" target="_blank">{name}</a>' if url else name
            liver_link_url = f"https://www.showroom-live.com/room/profile?room_id={room_id_disp}"
            liver_link = f'<a class="liver-link" href="{liver_link_url}" target="_blank">{liver_name}</a>' if room_id_disp else liver_name

            html_output += f'<tr class="{cls}">'
            html_output += f"<td>{liver_link}</td><td>{event_link}</td><td>{start_time}</td><td>{end_time}</td>"
            html_output += f"<td>{rank}</td><td>{point}</td><td>{level}</td><td>{event_id}</td><td>{room_id_disp}</td>"
            html_output += "</tr>"

        except Exception as e:
            # 個別行で失敗しても処理は続ける（原因の特定ログを出す）
            st.error(f"HTML生成エラー: {e}")

    # 最終的な全体サニタイズ：念のため不正コードポイントを削除してから返却
    # （タグ名の中に混入する可能性のある文字を根絶）
    html_output = re.sub(r"[\x00-\x1F\x7F\uFFFD]", "", html_output)
    html_output += "</tbody></table></div>"
    return html_output



# ----------------------------------------------------------------------
# ★★★ 表示（管理者/ライバーで分岐） ★★★
# ----------------------------------------------------------------------
if is_admin:
    # 管理者モードの表示
    # --- HTML出力前に不正文字を除去 ---
    import re
    def clean_text(s):
        if not isinstance(s, str):
            return s
        # 制御文字や壊れたUnicode文字を削除
        s = re.sub(r'[\x00-\x1F\x7F-\x9F\uFFFD]', '', s)
        # 改行やタブを空白に変換
        s = s.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        return s

    # df_show 全体をクリーン化（文字列列のみ）
    for col in df_show.select_dtypes(include=[object]).columns:
        df_show[col] = df_show[col].apply(clean_text)

    st.markdown(make_html_table_admin(df_show), unsafe_allow_html=True)
    
    end_today_color = END_TODAY_HIGHLIGHT.replace('background-color: ', '').replace(';', '')
    #st.caption(f"2023年9月以降に開始された参加イベントを表示しています。黄色行は開催中（終了日時が未来）のイベントです。赤っぽい行（{end_today_color}）は終了日時が本日のイベントです。")
    st.caption(f"")
    
    # CSVダウンロード
    cols_to_drop = [c for c in ["is_ongoing", "is_end_today", "__point_num", "URL", "ルームID", "__display_liver_name"] if c in df_show.columns]
    csv_bytes = df_show.drop(columns=cols_to_drop).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history_admin.csv", key="admin_csv_download")
    


    # ==========================================================
    # 🧩 管理者モード追加機能：ユーザーID登録・確認セクション
    # ==========================================================
    st.markdown("---")
    st.markdown("### 🧩 ユーザーID登録・確認（管理者専用オプション）")

    ADD_ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list_add.csv"

    import ftplib, io

    def upload_add_room_csv(df_add):
        try:
            ftp_info = st.secrets.get("ftp", {})
            host = ftp_info.get("host")
            user = ftp_info.get("user")
            password = ftp_info.get("password")
            if not host or not user or not password:
                st.error("FTP設定が見つかりません。st.secrets['ftp'] を確認してください。")
                return False
            csv_bytes = df_add.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            with ftplib.FTP(host, timeout=30) as ftp:
                ftp.login(user, password)
                with io.BytesIO(csv_bytes) as bf:
                    bf.seek(0)
                    ftp.storbinary("STOR /mksoul-pro.com/showroom/file/room_list_add.csv", bf)
            return True
        except Exception as e:
            st.error(f"FTPアップロードに失敗しました: {e}")
            return False

    # --- 既存登録済みデータ読込 ---
    try:
        df_add = pd.read_csv(ADD_ROOM_LIST_URL, dtype=str)
        if "ルームID" not in df_add.columns:
            df_add = pd.DataFrame(columns=["ルームID"])
    except Exception:
        df_add = pd.DataFrame(columns=["ルームID"])

    # --- ユーザーID登録フォーム ---
    st.markdown("#### 🔢 新規ユーザーID登録")
    new_room_id = st.text_input("ルームIDを入力してください（数値のみ）", key="new_room_id_input", placeholder="例：123456")

    col_add1, col_add2 = st.columns([1, 3])
    with col_add1:
        if st.button("➕ 登録", key="add_room_button"):
            if new_room_id and new_room_id.strip().isdigit():
                new_room_id = new_room_id.strip()
                if new_room_id not in df_add["ルームID"].astype(str).values:
                    df_add = pd.concat([df_add, pd.DataFrame({"ルームID": [new_room_id]})], ignore_index=True)
                    success = upload_add_room_csv(df_add)
                    if success:
                        st.success(f"✅ ルームID {new_room_id} を登録しました。")
                        time.sleep(0.1)
                        st.rerun()
                    else:
                        st.warning("⚠️ FTPアップロードに失敗しましたが、ローカルデータは更新されました。")
                else:
                    st.warning("⚠️ 既に登録済みのルームIDです。")
            else:
                st.warning("⚠️ 数値のルームIDを入力してください。")

    # --- 登録済みリスト表示 ---
    st.markdown("#### 📋 登録済みユーザー一覧")

    if df_add.empty:
        st.info("現在、登録済みのルームIDはありません。")
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        profiles = []
        room_ids = df_add["ルームID"].dropna().astype(str).tolist()

        # st.info(f"デバッグ: 登録済みルーム情報取得開始 ({len(room_ids)} 件)")

        def fetch_profile(rid):
            """個別ルーム情報を取得"""
            prof = http_get_json(API_ROOM_PROFILE, params={"room_id": rid})
            
            # 「公/フ」のステータスを決定
            if prof and prof.get("is_official") is not None:
                official_status = "公" if prof["is_official"] else "フ"
            else:
                official_status = "-"

            if prof:
                return {
                    "ルーム名": prof.get("room_name", ""),
                    "SHOWランク": prof.get("show_rank_subdivided", "-"),
                    "フォロワー数": prof.get("follower_num", "-"),
                    "まいにち配信": prof.get("live_continuous_days", "-"),
                    "公/フ": official_status, # ★追加
                    "ルームID": rid
                }
            else:
                return {
                    "ルーム名": "(取得失敗)",
                    "SHOWランク": "-",
                    "フォロワー数": "-",
                    "まいにち配信": "-",
                    "公/フ": official_status, # ★追加 (取得失敗時も設定)
                    "ルームID": rid
                }

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_profile, rid): rid for rid in room_ids}
            for future in as_completed(futures):
                profiles.append(future.result())

        elapsed = time.time() - start_time
        # st.info(f"デバッグ: 登録済みルーム情報取得完了 ({len(profiles)} 件, {elapsed:.2f} 秒)")

        df_prof_raw = pd.DataFrame(profiles)
        
        # 順序固定のためのマージ処理
        df_prof = pd.merge(
            df_add.reset_index(), # 元の順序を index 列として保存
            df_prof_raw,
            on="ルームID",
            how="left"
        ).sort_values(by='index').drop(columns=['index']).reset_index(drop=True)

        # --- HTMLテーブルの生成（イベント一覧に合わせた見た目） ---
        html = """
        <style>
        .add-table { width: 100%; border-collapse: collapse; font-size:14px; margin-top:0px; table-layout: auto; }
        .add-table thead th { background:#0b66c2; color:#fff; padding:6px 12px; border:1px solid #e8eef7; text-align:center; position: sticky; top: 0; z-index: 5; }
        .add-table td { padding:6px 12px; border:1px solid #f2f6fb; text-align:center !important; vertical-align: middle; }
        .add-table td.left { text-align:left !important; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:240px; }
        .add-table .link { color:#0b57d0; text-decoration:underline; }
        .add-table-wrapper { max-height: 420px; overflow-y: auto; border:1px solid #ddd; border-radius:6px; padding:0px; }
        </style>
        <div class="add-table-wrapper"><table class="add-table">
        <thead><tr>
          <th>ルーム名</th><th>SHOWランク</th><th>フォロワー数</th><th>まいにち配信</th><th>公/フ</th><th>ルームID</th>
        </tr></thead><tbody>
        """
        #                                                  ^^^^^^^ ★追加

        for _, row in df_prof.iterrows():
            room_name = row.get("ルーム名") or ""
            show_rank = row.get("SHOWランク") or "-"
            official_status_disp = row.get("公/フ") or "-" # ★取得
            follower = row.get("フォロワー数")
            
            try:
                # フォロワー数整形
                follower_fmt = f"{int(follower):,}" if str(follower) not in ("-", "") and pd.notna(follower) else (str(follower) if follower is not None else "-")
            except Exception:
                follower_fmt = str(follower or "-")
                
            live_days = row.get("まいにち配信") or "-"
            rid = row.get("ルームID") or ""
            
            # ルーム名にプロフィールページへのリンクを付与
            if rid:
                room_link = f'<a class="link" href="https://www.showroom-live.com/room/profile?room_id={rid}" target="_blank">{room_name}</a>'
            else:
                room_link = room_name

            html += "<tr>"
            html += f'<td class="left">{room_link}</td>'
            html += f"<td>{show_rank}</td>"
            html += f"<td>{follower_fmt}</td>"
            html += f"<td>{live_days} 日</td>"
            html += f"<td>{official_status_disp}</td>" # ★追加
            html += f"<td>{rid}</td>"
            html += "</tr>"

        html += "</tbody></table></div>"

        st.markdown(html, unsafe_allow_html=True)
        st.caption(f"")

        # CSVダウンロード（既存ボタンと同じ）
        csv_bytes = df_prof.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "登録ユーザー一覧をCSVでダウンロード",
            data=csv_bytes,
            file_name="room_list_add_view.csv",
            key="download_add_csv"
        )
    
    

else:
    # ライバーモードの表示 (既存ロジック)
    
    # ソートボタンの表示
    button_label = (
        "📅 デフォルト表示に戻す (終了日時降順)"
        if st.session_state.sort_by_point
        else "🏆 ポイントの高い順にソート"
    )

    st.button(
        button_label, 
        on_click=toggle_sort_by_point, 
        key="sort_toggle_button"
    )

    # ★★★ 修正箇所: ここに最新化ボタンを追加 ★★★
    st.button(
        "🔄 終了前イベントの最新化", 
        on_click=refresh_data,  # ← 追加
        key="librarian_refresh_button"
    )
    # ★★★ 修正箇所ここまで ★★★
    
    st.markdown(make_html_table_user(df_show, room_id), unsafe_allow_html=True)
    st.caption("2023年9月以降に開始された参加イベントを表示しています。黄色ハイライト行は終了前のイベントです。※ハイライトはイベント終了後、1時間後に消えます。")

    # CSV出力
    cols_to_drop = [c for c in ["is_ongoing", "__highlight_style", "URL", "ルームID"] if c in df_show.columns]
    csv_bytes = df_show.drop(columns=cols_to_drop).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("CSVダウンロード", data=csv_bytes, file_name="event_history.csv", key="user_csv_download")



# =========================================================
# 📊 追加機能: イベント貢献ランキング分析セクション
# =========================================================
st.write("---")
# st.subheader("📊 選択イベントの貢献度集計・分析")
st.markdown("#### 📊 選択イベントの貢献度集計・分析")

# 1. 選択肢の作成
event_options = df_show["イベント名"].tolist()
selected_names = st.multiselect(
    "分析対象のイベントを複数選択してください（10件程度までを推奨）",
    options=event_options,
    help="選択したイベントの貢献100位までのデータを合算・分析します。"
)

@st.cache_data(ttl=3600)
def fetch_contribution_ranking_data(event_id, room_id):
    api_url = f"https://www.showroom-live.com/api/event/contribution_ranking?event_id={event_id}&room_id={room_id}"
    try:
        r = requests.get(api_url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("ranking", [])
    except Exception:
        return []

# 3. 集計実行
if selected_names:
    # ボタンが押されたら計算を実行し session_state に保存する
    if st.button("📊 選択したイベントを集計する"):
        all_data = []
        progress_text = st.empty()
        bar = st.progress(0)
        
        for i, name in enumerate(selected_names):
            target_row = df[df["イベント名"] == name].iloc[0]
            eid = target_row["event_id"]
            
            progress_text.text(f"取得中: {name}...")
            ranking = fetch_contribution_ranking_data(eid, room_id)
            
            if ranking:
                temp_df = pd.DataFrame(ranking)
                temp_df["対象イベント"] = name
                temp_df['name'] = temp_df['name'].apply(
                    lambda x: "！！退会済みユーザー！！" if "Unsubscribed User" in str(x) or "退会済みユーザー" in str(x) else x
                )
                all_data.append(temp_df)
            
            bar.progress((i + 1) / len(selected_names))
            time.sleep(0.1)
        
        bar.empty()
        progress_text.empty()

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 【追加】25位以内判定用フラグ
            combined_df["is_top25"] = combined_df["rank"] <= 25
            
            summary_df = combined_df.groupby("user_id").agg({
                "name": "last",
                "point": ["sum", "mean"],
                "rank": "mean",
                "user_id": "count",
                "is_top25": "sum"  # 【追加】25位以内の合計回数
            })
            
            # 【修正】カラム名に「25位内入賞回数」を追加
            summary_df.columns = ["ユーザー名", "合計ポイント", "入賞時平均ポイント", "入賞時平均順位", "100位入賞回数", "25位入賞回数"]
            summary_df.index.name = "ユーザーID"
            summary_df["ランキング"] = summary_df["合計ポイント"].rank(ascending=False, method='min').astype(int)
            summary_df = summary_df.reset_index()
            
            # 【修正】列の並び順：100位の前に25位を配置
            cols = ["ランキング", "ユーザー名", "合計ポイント", "入賞時平均ポイント", "入賞時平均順位", "25位入賞回数", "100位入賞回数", "ユーザーID"]
            summary_df = summary_df[cols].sort_values("ランキング")

            master_event_list = df["イベント名"].tolist()
            sorted_selected_names = [name for name in master_event_list if name in selected_names]

            # 結果をセッションに保存
            st.session_state["summary_df"] = summary_df
            st.session_state["combined_df"] = combined_df
            st.session_state["last_selected_names"] = sorted_selected_names 
            st.success(f"集計完了: {len(selected_names)} 件のイベントを合算しました。")
        else:
            st.error("ランキングデータを取得できませんでした。")

    # --- 表示セクション ---
    if "summary_df" in st.session_state:
        summary_df = st.session_state["summary_df"]
        combined_df = st.session_state["combined_df"]
        saved_names = st.session_state["last_selected_names"]

        # 【修正】表示用に整形したコピーを作成（計算用の数値を保持したままにする）
        display_df = summary_df.copy()
        
        # ⚠️ map処理を削除またはコメントアウトします。
        # 文字列に変換せず、数値のまま st.dataframe に渡すのがポイントです。

        st.write("##### 🏆 合算貢献ランキング (TOP 100)")
        st.dataframe(
            display_df.head(100),
            use_container_width=True,
            hide_index=True,
            column_config={
                "ランキング": st.column_config.NumberColumn("順位", width="small", format="%d 位"),
                "ユーザー名": st.column_config.TextColumn("ユーザー名", width="large"),
                "合計ポイント": st.column_config.NumberColumn("合計支援ポイント", width="medium", format="%d"),
                "入賞時平均ポイント": st.column_config.NumberColumn("入賞時平均ポイント", width="medium", format="%.1f"),
                "入賞時平均順位": st.column_config.NumberColumn("入賞時平均順位", width="medium", format="%.1f"),
                # 【追加】100位の前に25位を配置。幅は100位と同様 medium
                "25位入賞回数": st.column_config.NumberColumn("25位内数", width="small", format="%d"),
                "100位入賞回数": st.column_config.NumberColumn("100位内数", width="small", format="%d"),
                "ユーザーID": st.column_config.NumberColumn("ユーザーID", width="medium"),
            }
        )
        
        res_csv = summary_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("集計結果をCSVで保存", data=res_csv, file_name="combined_contribution.csv")

        # --- 詳細分析セクション ---
        st.write("---")
        # st.subheader("🔍 特定ユーザーの詳細分析")
        st.write("##### 🔍 特定ユーザーの詳細分析")
        
        
        user_map = {str(row['ユーザーID']): f"{row['ユーザー名']} ({row['ユーザーID']})" for _, row in summary_df.iterrows()}
        target_user_id = st.selectbox(
            "詳細を確認したいユーザーを選択してください",
            options=list(user_map.keys()),
            format_func=lambda x: user_map[x]
        )

        if target_user_id:
            u_df_raw = combined_df[combined_df["user_id"].astype(str) == target_user_id].copy()
            if not u_df_raw.empty:
                u_name = u_df_raw["name"].iloc[-1]
                user_event_data = []
                
                for e_name in saved_names:
                    target_event_row = df[df["イベント名"] == e_name]
                    if not target_event_row.empty:
                        event_total_point = float(str(target_event_row["ポイント"].iloc[0]).replace(',', ''))
                        event_total_rank = target_event_row["順位"].iloc[0]
                        event_level = target_event_row["レベル"].iloc[0]
                    else:
                        event_total_point = 0
                        event_total_rank = "-"
                        event_level = "-"
                    
                    event_match = u_df_raw[u_df_raw["対象イベント"] == e_name]
                    p_val = event_match["point"].iloc[0] if not event_match.empty else 0
                    r_val = int(event_match["rank"].iloc[0]) if not event_match.empty else None
                    share_pct = (p_val / event_total_point * 100) if event_total_point > 0 else 0
                    
                    # 指示通りの表記: 「2位 / 356,257 / L2」
                    total_info = f"{event_total_rank}位 / {event_total_point:,.0f} / L{event_level}"
                    
                    # 配置：イベント名 -> 全体実績（small） -> 個人データ（太字・色）
                    user_event_data.append({
                        "イベント名": e_name,
                        "全体(順位 / pts / Lv)": total_info, 
                        "順位": r_val,
                        "支援ポイント": p_val,
                        "支援割合": share_pct
                    })
                
                u_df = pd.DataFrame(user_event_data)
                u_df['イベント名'] = pd.Categorical(u_df['イベント名'], categories=saved_names, ordered=True)
                u_df = u_df.sort_values('イベント名')

                # 個人実績（右3列）を太字(900)＋背景ハイライト
                styled_df = u_df.style.map(
                    lambda x: 'background-color: #f1f3f6; font-weight: 900; color: #000000;', 
                    subset=['順位', '支援ポイント', '支援割合']
                )

                st.write(f"###### 👤 {u_name} さんの貢献詳細")
                
                st.dataframe(
                    styled_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "イベント名": st.column_config.TextColumn("イベント名", width="large"),
                        "全体(順位 / pts / Lv)": st.column_config.TextColumn("全体(順位 / pt / Lv)", width="small"),
                        "順位": st.column_config.NumberColumn("貢献ランク", format="%d 位", width="small"),
                        "支援ポイント": st.column_config.NumberColumn("支援ポイント", format="%d", width="small"),
                        "支援割合": st.column_config.NumberColumn("支援割合", format="%.2f %%", width="small")
                    }
                )

                st.write("")
                st.write("")


                import altair as alt
                
                u_df['支援割合_label'] = u_df['支援割合'].map('{:.2f} %'.format)
                
                # 共通のツールチップ設定を定義しておくと管理が楽です
                common_tooltip = [
                    alt.Tooltip('イベント名:N'),
                    alt.Tooltip('支援ポイント:Q', format=',', title='支援ポイント'), # タイトルを統一しカンマ区切りに
                    alt.Tooltip('順位:Q'),
                    alt.Tooltip('支援割合_label:N', title='支援割合')
                ]
                
                base = alt.Chart(u_df).encode(
                    x=alt.X(
                        'イベント名:N', 
                        sort=saved_names[::-1], 
                        title='イベント名',
                        axis=alt.Axis(
                            labelAngle=45, 
                            labelLimit=150, 
                            labelFontSize=11
                        )
                    )
                )
                
                # 1. 棒グラフ側にも同じtooltipを設定
                bar = base.mark_bar(color='#5271FF', opacity=0.6).encode(
                    y=alt.Y('支援ポイント:Q', title='支援ポイント（棒）'),
                    tooltip=common_tooltip
                )
                
                # 2. 折れ線側も共通設定を使用
                line = base.mark_line(color='#FF4B4B', point=True).encode(
                    y=alt.Y('順位:Q', title='順位（線：1位が上）', scale=alt.Scale(reverse=True)),
                    tooltip=common_tooltip
                )
                
                st.altair_chart(
                    alt.layer(bar, line).resolve_scale(y='independent').properties(width='container', height=450),
                    use_container_width=True
                )