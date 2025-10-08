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

# ★★★ 新しい定数 (データ収集効率化のため) ★★★
EVENT_ID_SCAN_LIMIT = 50 # フェーズ2でスキャンするイベントIDの範囲
HISTORICAL_EVENT_ID_START = 40000 # 過去2年以上に遡る際のイベントIDの開始点（推定値）

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

# ---------- Utility (既存の関数群) ----------
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
# ★★★ 新しいユーティリティ関数（データ収集効率化） ★★★
# ----------------------------------------------------------------------

def get_max_event_id(df):
    """現在のDBにあるイベントIDの最大値を取得する。"""
    # event_idカラムを数値に変換し、最大値を取得
    df['event_id_num'] = pd.to_numeric(df['event_id'], errors='coerce')
    max_id = df['event_id_num'].max()
    df.drop(columns=['event_id_num'], inplace=True, errors='ignore')
    # NaNの場合は HISTORICAL_EVENT_ID_START を返す
    return int(max_id) if pd.notna(max_id) else HISTORICAL_EVENT_ID_START

def fetch_event_details(event_id):
    """イベントIDからイベント名、URL、期間を取得する。"""
    data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": 1})
    if not data or "event_info" not in data:
        return None
        
    info = data["event_info"]
    return {
        "event_id": str(event_id),
        "URL": info.get("event_url") or "",
        "イベント名": info.get("name") or "",
        # タイムスタンプを 'YYYY/MM/DD HH:MM' 形式の文字列に変換
        "開始日時": fmt_time(info.get("started_at")),
        "終了日時": fmt_time(info.get("ended_at")),
    }

def scan_event_ids_in_range(start_id, end_id, target_room_ids):
    """
    指定されたID範囲をスキャンし、指定ライバーが参加しているイベントのエントリーを収集する。
    """
    new_entries = []
    
    st.info(f"イベントID {start_id} から {end_id} までのスキャンを開始...")
    progress_bar = st.progress(0)
    total_scan = end_id - start_id + 1
    
    for i, event_id in enumerate(range(start_id, end_id + 1)):
        if (i % 50) == 0:
            progress_bar.progress((i + 1) / total_scan)
            st.text(f"スキャン中... ID: {event_id}")

        event_details = fetch_event_details(event_id)
        if not event_details:
            time.sleep(0.01) # API負荷軽減
            continue

        # イベントが存在する場合、指定されたライバーの参加状況を確認
        for room_id in target_room_ids:
            stats = get_event_stats_from_roomlist(event_id, room_id)
            if stats:
                entry = {
                    **event_details,
                    "ルームID": str(room_id),
                    "ライバー名": st.session_state.room_name_cache.get(str(room_id)) or "",
                    "順位": stats.get("rank") or "-",
                    "ポイント": stats.get("point") or 0,
                    "レベル": stats.get("quest_level") or 0,
                }
                new_entries.append(entry)
                # st.text(f"✅ 発見: ID {event_id}, R: {room_id}")
            time.sleep(0.1) # API負荷軽減
        
        time.sleep(0.05) # イベントIDごとのAPI負荷軽減
    
    progress_bar.empty()
    st.info(f"スキャンが完了しました。{len(new_entries)}件の新しいエントリを発見しました。")
    return pd.DataFrame(new_entries)

def get_managed_room_ids(df_all):
    """現在のデータベースに存在するユニークなルームIDのリストを返します。"""
    # 管理者モードでは、全データに含まれるルームIDが「管理対象」と見なされる
    return [str(rid) for rid in df_all["ルームID"].unique() if str(rid) not in ('', 'nan')]

def merge_new_data_into_session(df_new):
    """新しく取得したデータをセッションのdf_allにマージし、重複を削除する"""
    if df_new.empty:
        st.info("マージする新規データはありませんでした。")
        return

    df_old = st.session_state.df_all.copy()
    df_combined = pd.concat([df_old, df_new], ignore_index=True)
    
    # event_idとルームIDの組み合わせで重複を削除（新しいデータが上書きされる）
    df_combined["event_id"] = df_combined["event_id"].astype(str)
    df_combined["ルームID"] = df_combined["ルームID"].astype(str)
    
    # 元のデータ列のみを残す
    keep_cols = [c for c in df_old.columns if c not in ['__start_ts', '__end_ts']]
    
    # 重複削除
    df_combined.drop_duplicates(subset=["event_id", "ルームID"], keep="last", inplace=True)
    
    # マージ後のデータをセッションに保存
    st.session_state.df_all = df_combined[keep_cols].copy()
    st.toast(f"マージ完了。全データ件数: {len(st.session_state.df_all)}件に更新されました。", icon="💾")
    st.session_state.show_data = True # データの再表示をトリガー

# ----------------------------------------------------------------------
# ★★★ 3フェーズ更新ロジックの実装 ★★★
# ----------------------------------------------------------------------

# フェーズ1 (ケース②): 既存の開催中イベント最新化 (既存ロジックを流用し、名前付け)
# このロジックは既存コード内の「if is_admin or st.session_state.get('refresh_trigger', False):」ブロックで行われます。

def run_phase_2_new_event_discovery(df_all, managed_room_ids):
    """
    フェーズ2: 新規イベントの発見と追加 (ケース ①)
    既存DBの最大ID+1からEVENT_ID_SCAN_LIMIT分の狭い範囲をスキャンする。
    """
    if df_all.empty:
        st.warning("DBが空のため、新規イベントスキャンはできません。")
        return pd.DataFrame()

    max_id = get_max_event_id(df_all)
    start_id = max_id + 1
    end_id = max_id + EVENT_ID_SCAN_LIMIT
    
    st.toast(f"フェーズ2: 新規イベントスキャン中 (ID: {start_id}~{end_id})...", icon="🔍")
    
    new_data_df = scan_event_ids_in_range(start_id, end_id, managed_room_ids)

    return new_data_df


def run_phase_3_new_liver_scan(df_all, new_liver_ids):
    """
    フェーズ3: 新規ライバーの履歴取得 (ケース ③)
    新規追加されたライバーIDのみを対象に、過去の全イベントをスキャンする。
    """
    if not new_liver_ids:
        return pd.DataFrame()

    start_id = HISTORICAL_EVENT_ID_START
    max_id = get_max_event_id(df_all) # DBの最大IDまでスキャン
    # 履歴スキャンの終了IDは現在の最大イベントIDとする。DBが空の場合は開始IDに少し加算。
    end_id = max_id if max_id > start_id else start_id + 100 
    
    st.toast(f"フェーズ3: 新規ライバー({len(new_liver_ids)}名)の履歴フルスキャン中 (ID: {start_id}~{end_id})...", icon="⏳")
    
    # scan_event_ids_in_rangeにnew_liver_idsを渡す
    new_data_df = scan_event_ids_in_range(start_id, end_id, new_liver_ids)

    return new_data_df

# ----------------------------------------------------------------------
# ★★★ セッションステートの初期化とコールバック関数 (新規入力欄を追加) ★★★
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
# ★★★ 新規ライバーID入力欄のステート ★★★
if 'new_liver_input' not in st.session_state:
    st.session_state.new_liver_input = ""


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
    """全量表示チェックボックスの値をセッションステートに強制的に同期させるコールバック関数。"""
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


    # ★★★ (フェーズ1) 開催中イベント最新化 (既存ロジックを流用) ★★★
    if st.session_state.get('refresh_trigger', False): # is_adminを削除し、ボタン押下時のみに限定
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
        
        # 1. フェーズ1: 開催中イベント最新化ボタン (既存のリアルタイム更新)
        st.subheader("フェーズ1: 開催中イベントのリアルタイム更新 (ケース②)")
        st.button(
            "🔄 開催中イベントの順位/ポイントを最新化", 
            on_click=refresh_data, 
            key="admin_refresh_button"
        )
        st.caption("既存の開催中イベント（イベントIDがDBに存在する）の順位とポイントのみを更新します。")
        
        st.markdown("---")
        st.subheader("🛠 データ収集機能（フェーズ2/3）")

        # 2. フェーズ2: 新規イベント発見ボタン (ケース①)
        if st.button(f"🔍 フェーズ2: 新規イベント発見 (ID +{EVENT_ID_SCAN_LIMIT} スキャン)", key="run_phase2"):
            with st.spinner("新規イベントの小範囲スキャンを実行中..."):
                managed_rooms = get_managed_room_ids(st.session_state.df_all)
                new_data = run_phase_2_new_event_discovery(st.session_state.df_all, managed_rooms)
                merge_new_data_into_session(new_data)
                
        st.caption(f"現在のDBの最大イベントIDから+{EVENT_ID_SCAN_LIMIT}までの範囲をスキャンします。")
        
        st.markdown("---")

        # 3. フェーズ3: 新規ライバー履歴スキャン (ケース③)
        st.text_input(
            "新規ライバーID (カンマ区切り)",
            value=st.session_state.new_liver_input,
            key="new_liver_input_key",
            help="新規で管理対象に追加したライバーのルームIDをカンマ区切りで入力してください。",
        )
        if st.button("⏳ フェーズ3: 新規ライバーの履歴フルスキャン", key="run_phase3"):
            room_ids_str = st.session_state.new_liver_input_key.strip()
            new_liver_ids = [rid.strip() for rid in room_ids_str.split(',') if rid.strip()]
            if new_liver_ids:
                with st.spinner(f"新規ライバーの全履歴スキャンを実行中（ID: {HISTORICAL_EVENT_ID_START}~最新ID）。これは時間がかかる場合があります..."):
                    # ライバー名を事前にキャッシュ（UI表示をスムーズにするため）
                    for room_id_val in new_liver_ids:
                        room_id_str = str(room_id_val)
                        if room_id_str not in st.session_state.room_name_cache:
                            name = get_room_name(room_id_str)
                            if name:
                                st.session_state.room_name_cache[room_id_str] = name
                            time.sleep(0.05)
                    
                new_data = run_phase_3_new_liver_scan(st.session_state.df_all, new_liver_ids)
                merge_new_data_into_session(new_data)
                st.session_state.new_liver_input = "" # 実行後に入力欄をクリア
            else:
                st.warning("新規ライバーIDを入力してください。")
        st.caption(f"全履歴スキャンは高負荷です。新規ライバーの追加があったときのみ実行してください。 (基準ID: {HISTORICAL_EVENT_ID_START})")

        st.markdown("---") # 区切り線

        # 4. 全量表示トグル (既存)
        st.checkbox(
            "全量表示（期間フィルタ無効）", 
            value=st.session_state.admin_full_data,
            key="admin_full_data_checkbox_internal",
            on_change=toggle_full_data
        )

        # 5. 終了日時フィルタリング (既存)
        selected_end_date = st.selectbox(
            "終了日時で絞り込み",
            options=["全期間"] + unique_end_dates,
            key='admin_end_date_filter',
        )

        # 6. 開始日時フィルタリング (既存)
        selected_start_date = st.selectbox(
            "開始日時で絞り込み",
            options=["全期間"] + unique_start_dates,
            key='admin_start_date_filter',
        )
        
    # 4. プルダウンフィルタの適用
    if selected_end_date != "全期間":
        df_filtered = df_filtered[df_filtered["終了日時"].str.startswith(selected_end_date)].copy()
    if selected_start_date != "全期間":
        df_filtered = df_filtered[df_filtered["開始日時"].str.startswith(selected_start_date)].copy()
        
    # 4.5. ライバー名の最新化 (APIから取得し、キャッシュ) (既存)
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


    # 6. ソート (終了日時が新しいものが上) (既存)
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
    table col:nth-child(5) {{ width: 6%; }}  /* 順位 */
    table col:nth-child(6) {{ width: 12%; }} /* ポイント */
    table col:nth-child(7) {{ width: 6%; }}  /* レベル */
    
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