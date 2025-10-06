# app.py
import streamlit as st
import requests
import pandas as pd
import io
import time
import ftplib
import socket
import traceback
from datetime import datetime
import pytz
import concurrent.futures
from typing import List, Dict, Any

JST = pytz.timezone("Asia/Tokyo")

# ---------- 設定（必要に応じて UI で上書き可能にしています） ----------
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
API_CONTRIBUTION = "https://www.showroom-live.com/api/event/contribution_ranking"
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"

# デフォルト FTP（**推奨：Streamlit Secrets に設定してください**）
# st.secrets の例:
# [ftp]
# host = "ftp11.gmoserver.jp"
# user = "sd0866487@gmoserver.jp"
# password = "v$p7d56C4#QpfCvj"
DEFAULT_FTP_FALLBACK = {
    "host": None,
    "user": None,
    "password": None
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; mksoul-bot/1.0)"
}

# ---------- ヘルパー ----------
def http_get_json(url: str, params: dict = None, retries: int = 2, timeout: float = 10.0, backoff: float = 0.5):
    """GET -> JSON（安全ラップ）。失敗時は None を返す。"""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            else:
                # 404 等は None
                return None
        except (requests.RequestException, socket.timeout):
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            return None
    return None

def ftp_upload_bytes(file_path: str, content_bytes: bytes, retries: int = 3):
    """FTP にバイト列をアップロード。SecretsからFTP情報取得。失敗すると例外を投げる。"""
    ftp_host = st.secrets.get("ftp", {}).get("host") or DEFAULT_FTP_FALLBACK["host"]
    ftp_user = st.secrets.get("ftp", {}).get("user") or DEFAULT_FTP_FALLBACK["user"]
    ftp_pass = st.secrets.get("ftp", {}).get("password") or DEFAULT_FTP_FALLBACK["password"]
    if not ftp_host or not ftp_user:
        raise RuntimeError("FTP information not found in st.secrets['ftp']. Please set ftp.host/ftp.user/ftp.password.")
    last_err = None
    for i in range(retries):
        try:
            with ftplib.FTP(ftp_host, timeout=30) as ftp:
                ftp.login(ftp_user, ftp_pass)
                with io.BytesIO(content_bytes) as bf:
                    bf.seek(0)
                    ftp.storbinary(f"STOR {file_path}", bf)
            return True
        except Exception as e:
            last_err = e
            time.sleep(1 + i)
    raise last_err

def fmt_time(ts):
    """Unix秒 -> 'YYYY/MM/DD HH:MM' (JST) 。 None -> '' """
    try:
        if ts is None:
            return ""
        ts = int(ts)
        # ミリ秒対策
        if ts > 20000000000:
            ts = ts // 1000
        dt = datetime.fromtimestamp(ts, JST)
        return dt.strftime("%Y/%m/%d %H:%M")
    except Exception:
        return ""

# ---------- 管理ルーム（room_list.csv）の取得 ----------
@st.cache_data(ttl=3600)
def load_managed_rooms(url: str) -> pd.DataFrame:
    """
    管理ルームCSVを取得して DataFrame にする。
    柔軟にヘッダ有無を判断する（簡易）。
    期待列（可能）: ルームID, ルーム名, ルームURL, アカウントID, 最終ライブ日時, ...
    最終的に index を 'ルームID' (文字列) にする。
    """
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    txt = r.content.decode("utf-8-sig")
    # まずヘッダありで試す
    try:
        df = pd.read_csv(io.StringIO(txt))
        cols = df.columns.tolist()
        # 判定: 'ルームID' カラムがあればOK。なければヘッダなしとして処理する
        if any("ルーム" in str(c) and ("ID" in str(c) or "id" in str(c).lower()) for c in cols):
            # 正規化 - 列名のよくある英語/日本語を変換して統一
            rename_map = {}
            for c in cols:
                lc = str(c).lower()
                if "room" in lc and "id" in lc:
                    rename_map[c] = "ルームID"
                elif "name" in lc and ("room" in lc or "ルーム" in c):
                    rename_map[c] = "ルーム名"
                elif "account" in lc or "アカウント" in c:
                    rename_map[c] = "アカウントID"
                elif "url" in lc:
                    rename_map[c] = "ルームURL"
            df.rename(columns=rename_map, inplace=True)
        else:
            # ヘッダありだが期待列がない -> fallthrough to headerless
            raise Exception("no expected headers")
    except Exception:
        # headerless: 読み直し
        df = pd.read_csv(io.StringIO(txt), header=None)
        # 最低限1列はあるはず。推定で列を割り当てる
        # A列: ルームID, B列: ルーム名, C列: ルームURL, D列: アカウントID, ...
        col_map = {}
        if df.shape[1] >= 1:
            col_map[0] = "ルームID"
        if df.shape[1] >= 2:
            col_map[1] = "ルーム名"
        if df.shape[1] >= 3:
            col_map[2] = "ルームURL"
        if df.shape[1] >= 4:
            col_map[3] = "アカウントID"
        df.rename(columns=col_map, inplace=True)
    # ルームID を文字列化して index を設定
    if "ルームID" not in df.columns:
        # 最低限ルームID列が取れないと困る。空DF返す
        return pd.DataFrame(columns=["ルームID"])
    df["ルームID"] = df["ルームID"].astype(str).str.strip()
    df = df.set_index("ルームID", drop=False)
    return df

# ---------- スキャンフェーズ（event_id 範囲から「有効」イベントを列挙） ----------
def check_event_has_rooms(event_id: int, timeout: float = 8.0) -> bool:
    """event_id の 1ページ目を叩いて部屋数が存在するかを判定"""
    params = {"event_id": event_id, "p": 1}
    data = http_get_json(API_ROOM_LIST, params=params, timeout=timeout, retries=1)
    if not data:
        return False
    # API の返し方が環境で違うので柔軟に判定
    if isinstance(data, dict):
        # number_of_rooms or total_entries or list
        if data.get("number_of_rooms") is not None:
            return int(data.get("number_of_rooms")) > 0
        if data.get("total_entries") is not None:
            return int(data.get("total_entries")) > 0
        if "list" in data and isinstance(data["list"], list):
            return len(data["list"]) > 0
    return False

def scan_event_ids(start_id: int, end_id: int, max_workers: int, progress_callback=None) -> List[int]:
    """指定範囲の event_id をスキャンして有効な ID のみ返す"""
    event_ids = list(range(start_id, end_id + 1))
    valid = []
    total = len(event_ids)
    checked = 0

    # 並列スキャン（少数スレッドで）。
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check_event_has_rooms, eid): eid for eid in event_ids}
        for fut in concurrent.futures.as_completed(futures):
            eid = futures[fut]
            checked += 1
            try:
                ok = fut.result()
            except Exception:
                ok = False
            if ok:
                valid.append(eid)
            if progress_callback:
                progress_callback(checked, total)
    return sorted(valid)

# ---------- イベント処理フェーズ ----------
def fetch_all_room_entries(event_id: int, max_pages: int = 200, sleep_between_pages: float = 0.05):
    """event_id の room_list を全ページ取得して list を返す"""
    entries = []
    page = 1
    while page <= max_pages:
        params = {"event_id": event_id, "p": page}
        data = http_get_json(API_ROOM_LIST, params=params, timeout=12, retries=1)
        if not data:
            break
        # list キーを期待
        page_entries = data.get("list") or []
        if not page_entries:
            break
        entries.extend(page_entries)
        # last_page / next_page があるなら利用
        last_page = data.get("last_page")
        next_page = data.get("next_page")
        if last_page is not None:
            if page >= int(last_page):
                break
        if next_page is None:
            break
        page += 1
        time.sleep(sleep_between_pages)
    return entries

def fetch_event_detail_via_contrib(event_id: int, any_room_id: str):
    """contribution_ranking API を 1 回叩いてイベント詳細（event_name, started_at, ended_at, event_url, image）を返す"""
    params = {"event_id": event_id, "room_id": any_room_id}
    data = http_get_json(API_CONTRIBUTION, params=params, timeout=12, retries=1)
    if not data:
        return {}
    # data に 'event' キーがあれば利用
    if isinstance(data, dict) and "event" in data and isinstance(data["event"], dict):
        ev = data["event"]
        return {
            "event_name": ev.get("event_name"),
            "started_at": ev.get("started_at"),
            "ended_at": ev.get("ended_at"),
            "event_url": ev.get("event_url"),
            "event_image": ev.get("image")
        }
    # フォールバック: None
    return {}

def process_event(event_id: int, managed_rooms_df: pd.DataFrame, sleep_between_requests: float = 0.02):
    """
    1 event の処理。管理ルームにマッチするレコードのみ返す（list of dict）。
    各 dict は最終的な CSV の 1 行になります。
    """
    results = []
    entries = fetch_all_room_entries(event_id)
    if not entries:
        return results

    # 全 entries の中で管理対象のルームIDだけ抽出
    managed_ids = set(managed_rooms_df.index.astype(str).tolist())
    matched_entries = [e for e in entries if str(e.get("room_id")) in managed_ids]
    if not matched_entries:
        return results  # 管理対象ルームがいなければ無視

    # event-level detail: contribution API を 1 回だけ叩く（第1候補の管理対象roomで）
    any_rid = str(matched_entries[0].get("room_id"))
    detail = fetch_event_detail_via_contrib(event_id, any_rid)
    # fallback: started_at/ended_at が entries 内に無ければ None のまま

    for ent in matched_entries:
        rid = str(ent.get("room_id"))
        # 順位、ポイント、quest_level の取り出し（柔軟に）
        rank = ent.get("rank") or ent.get("position") or ent.get("event_entry", {}).get("rank") or "-"
        point = ent.get("point") or ent.get("event_point") or ent.get("total_point") or 0
        # quest_level は event_entry 内にある場合が多い
        quest_level = ent.get("event_entry", {}).get("quest_level")
        if quest_level is None:
            quest_level = ent.get("quest_level") or 0
        # convert numeric
        try:
            point = int(point)
        except Exception:
            try:
                point = int(float(point))
            except Exception:
                point = 0
        try:
            quest_level = int(quest_level)
        except Exception:
            quest_level = 0

        rec = {
            "PR対象": "",
            "ライバー名": managed_rooms_df.loc[rid, "ルーム名"] if rid in managed_rooms_df.index and "ルーム名" in managed_rooms_df.columns else "",
            "アカウントID": managed_rooms_df.loc[rid, "アカウントID"] if rid in managed_rooms_df.index and "アカウントID" in managed_rooms_df.columns else "",
            "イベント名": detail.get("event_name") or ent.get("event_entry", {}).get("event_name") or None,
            "開始日時": fmt_time(detail.get("started_at") or ent.get("event_entry", {}).get("entried_at")),
            "終了日時": fmt_time(detail.get("ended_at") or ent.get("event_entry", {}).get("updated_at")),
            "順位": rank,
            "ポイント": point,
            "備考": "",
            "紐付け": "○",
            "URL": detail.get("event_url") or "",
            "レベル": quest_level,
            "event_id": str(event_id),
            "ルームID": rid,
            "イベント画像（URL）": detail.get("event_image") or ""
        }
        results.append(rec)
        time.sleep(sleep_between_requests)
    return results

# ---------- メイン処理（並列 + 進捗 + 途中保存） ----------
def fetch_and_build_database(event_start: int, event_end: int, max_workers: int, save_interval: int, save_path_ftp: str, sleep_between_requests: float = 0.02):
    """
    - スキャンフェーズ（有効 event_id の抽出）
    - 並列で event ごとの処理
    - 結果を CSV にして FTP へ都度保存（save_interval）
    - 最終 DataFrame を返す（pandas.DataFrame）
    """
    st.info("📡 イベント一覧（IDレンジ）をスキャンしています...")
    progress_text = st.empty()
    pbar = st.progress(0)

    # load managed rooms
    try:
        managed_rooms = load_managed_rooms(ROOM_LIST_URL)
    except Exception as e:
        st.error(f"管理ルームの取得に失敗しました: {e}")
        return pd.DataFrame()

    # 1) スキャン: 有効 event_id 抽出
    valid_event_ids = []
    def scan_progress(checked, total):
        pbar.progress(int(checked / total * 100))
        progress_text.text(f"スキャン: {checked}/{total}")
    valid_event_ids = scan_event_ids(event_start, event_end, max_workers=max(2, min(max_workers, 20)), progress_callback=scan_progress)
    pbar.progress(0)
    progress_text.text(f"スキャン完了: 有効イベント {len(valid_event_ids)} 件")

    if not valid_event_ids:
        st.warning("指定範囲内に有効なイベントが見つかりませんでした。")
        return pd.DataFrame()

    # 2) 並列処理: 各 event を処理して管理ルーム分のレコードを取得
    st.info("⚙️ 有効イベントを並列で処理しています...")
    total_events = len(valid_event_ids)
    processed_events = 0
    all_records = []

    # helper for saving intermediate CSV
    def save_progress_csv(records: List[dict], desc: str):
        if not records:
            return False
        df_tmp = pd.DataFrame(records)
        # final column order
        col_order = ["PR対象", "ライバー名", "アカウントID", "イベント名", "開始日時", "終了日時",
                     "順位", "ポイント", "備考", "紐付け", "URL", "レベル", "event_id", "ルームID", "イベント画像（URL）"]
        for c in col_order:
            if c not in df_tmp.columns:
                df_tmp[c] = ""
        df_tmp = df_tmp[col_order]
        csv_bytes = df_tmp.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        # attempt FTP upload, fallback to local download via streamlit
        try:
            ftp_upload_bytes(save_path_ftp, csv_bytes, retries=2)
            st.success(f"💾 途中保存完了 ({desc})")
            return True
        except Exception as e:
            st.warning(f"FTPアップロードに失敗しました（{e}）。代わりにダウンロードを提供します。")
            st.download_button("途中保存CSVをダウンロード", data=csv_bytes, file_name=f"event_db_partial_{int(time.time())}.csv")
            return False

    # Thread pool for event processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_event, eid, managed_rooms, sleep_between_requests): eid for eid in valid_event_ids}
        for fut in concurrent.futures.as_completed(futures):
            eid = futures[fut]
            processed_events += 1
            try:
                recs = fut.result()
            except Exception as e:
                # log and continue
                st.error(f"event_id={eid} の処理で例外: {e}")
                recs = []
            if recs:
                all_records.extend(recs)

            # 進捗表示更新
            pct = int(processed_events / total_events * 100)
            pbar.progress(min(100, pct))
            progress_text.text(f"処理中: {processed_events}/{total_events} (event_id={eid})  — 集計中レコード: {len(all_records)}")

            # 途中保存トリガー: SAVE_INTERVAL ごとに保存
            if save_interval > 0 and len(all_records) > 0 and (len(all_records) % save_interval == 0):
                try:
                    save_progress_csv(all_records, f"{len(all_records)}/{len(valid_event_ids)}")
                except Exception as e:
                    st.warning(f"途中保存でエラー: {e}")

    # 最終整形
    if not all_records:
        st.info("処理は完了しましたが、管理対象ルームのマッチはありませんでした。")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # ensure columns/order
    col_order = ["PR対象", "ライバー名", "アカウントID", "イベント名", "開始日時", "終了日時",
                 "順位", "ポイント", "備考", "紐付け", "URL", "レベル", "event_id", "ルームID", "イベント画像（URL）"]
    for c in col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[col_order]
    # sort event_id desc (新しいものが上)
    try:
        df["event_id_sort"] = df["event_id"].astype(int)
        df.sort_values("event_id_sort", ascending=False, inplace=True)
        df.drop(columns=["event_id_sort"], inplace=True)
    except Exception:
        pass

    # 最終保存（FTP へ）
    csv_bytes_final = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    try:
        ftp_upload_bytes(save_path_ftp, csv_bytes_final, retries=3)
        st.success(f"✅ 最終保存（FTP）完了: {save_path_ftp} （{len(df)} 件）")
    except Exception as e:
        st.warning(f"FTP保存に失敗しました: {e}。最終CSV をダウンロードできます。")
        st.download_button("最終結果をダウンロード", data=csv_bytes_final, file_name=f"event_database_{int(time.time())}.csv")

    return df

# ---------- Streamlit UI ----------
st.set_page_config(page_title="SHOWROOM イベント参加データベース更新", layout="wide")
st.title("🎯 SHOWROOM イベント参加データベース更新")

with st.sidebar:
    st.header("実行設定")
    EVENT_ID_START = st.number_input("EVENT_ID_START", value=33000, step=1)
    EVENT_ID_END = st.number_input("EVENT_ID_END", value=41000, step=1)
    MAX_WORKERS = st.number_input("MAX_WORKERS (並列数)", min_value=1, max_value=50, value=10, step=1)
    SAVE_INTERVAL = st.number_input("SAVE_INTERVAL（件数）: 途中保存をこの件数ごとに実施", min_value=0, value=500, step=50)
    SAVE_PATH_FTP = st.text_input("FTP 保存パス(例: /mksoul-pro.com/showroom/file/event_database.csv)", value="/mksoul-pro.com/showroom/file/event_database.csv")
    SLEEP_BETWEEN_REQUESTS = st.number_input("request間スリープ（秒、API負荷対策）", min_value=0.0, value=0.02, step=0.01)
    st.markdown("---")
    st.markdown("**注意**: FTP情報は Streamlit Secrets に `ftp.host` / `ftp.user` / `ftp.password` をセットしてください。")
    if st.button("実行（スキャン→収集）"):
        st.session_state.run_start = time.time()
        try:
            df_res = fetch_and_build_database(
                int(EVENT_ID_START),
                int(EVENT_ID_END),
                int(MAX_WORKERS),
                int(SAVE_INTERVAL),
                str(SAVE_PATH_FTP),
                float(SLEEP_BETWEEN_REQUESTS)
            )
            if isinstance(df_res, pd.DataFrame) and not df_res.empty:
                st.success(f"処理完了: {len(df_res)} 件を収集しました。")
                st.dataframe(df_res.head(200))
                csv_bytes = df_res.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button("CSV をダウンロード", data=csv_bytes, file_name="event_database.csv")
            else:
                st.info("結果が空でした。ログを確認してください。")
        except Exception as e:
            st.error(f"実行中に致命エラーが発生しました: {e}")
            st.exception(traceback.format_exc())
