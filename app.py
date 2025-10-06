# app.py (改良版)
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
from typing import List, Dict, Any, Optional

JST = pytz.timezone("Asia/Tokyo")

API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
API_CONTRIBUTION = "https://www.showroom-live.com/api/event/contribution_ranking"
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
ARCHIVE_URL = "https://mksoul-pro.com/showroom/file/sr-event-archive.csv"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-bot/1.0)"}

# FTP info should be placed in Streamlit Secrets:
# [ftp]
# host = "ftp11.gmoserver.jp"
# user = "sd0866487@gmoserver.jp"
# password = "..."
DEFAULT_FTP_FALLBACK = {"host": None, "user": None, "password": None}

# ---------- ヘルパー ----------
def http_get_json(url: str, params: dict = None, retries: int = 3, timeout: float = 12.0, backoff: float = 0.6):
    """堅牢な GET -> JSON。429や接続エラーに対してリトライする。失敗時は None を返す。"""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            # 429 はレート制限っぽいので少し長めに待つ
            if r.status_code == 429:
                wait = backoff * (attempt + 1) * 2
                time.sleep(wait)
                continue
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            # 404 -> 存在しない
            if r.status_code in (404, 410):
                return None
            # その他は一旦リトライの対象にする
            time.sleep(backoff * (attempt + 1))
        except (requests.RequestException, socket.timeout) as e:
            time.sleep(backoff * (attempt + 1))
            continue
    return None

def fmt_time(ts) -> str:
    """Unix秒 -> 'YYYY/MM/DD HH:MM' (JST) または空文字"""
    try:
        if ts is None:
            return ""
        ts = int(ts)
        if ts > 20000000000:
            ts = ts // 1000
        dt = datetime.fromtimestamp(ts, JST)
        return dt.strftime("%Y/%m/%d %H:%M")
    except Exception:
        return ""

def ftp_upload_bytes(file_path: str, content_bytes: bytes, retries: int = 2):
    ftp_host = st.secrets.get("ftp", {}).get("host") or DEFAULT_FTP_FALLBACK["host"]
    ftp_user = st.secrets.get("ftp", {}).get("user") or DEFAULT_FTP_FALLBACK["user"]
    ftp_pass = st.secrets.get("ftp", {}).get("password") or DEFAULT_FTP_FALLBACK["password"]
    if not ftp_host or not ftp_user:
        raise RuntimeError("FTP情報が st.secrets['ftp'] にありません。")
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

# ---------- 管理ルーム / アーカイブ読み込み ----------
@st.cache_data(ttl=3600)
def load_managed_rooms(url: str) -> pd.DataFrame:
    """room_list.csv を取得して 'ルームID' を index にした DataFrame を返す"""
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    txt = r.content.decode("utf-8-sig")
    # 可能ならヘッダーありで読み、無ければ headerless 想定
    try:
        df = pd.read_csv(io.StringIO(txt))
        # 自動的に列名マッピング
        cols = df.columns.tolist()
        rename = {}
        for c in cols:
            lc = str(c).lower()
            if "room" in lc and "id" in lc:
                rename[c] = "ルームID"
            elif ("name" in lc and ("room" in lc or "ルーム" in str(c))) or "ルーム名" in str(c):
                rename[c] = "ルーム名"
            elif "account" in lc or "アカウント" in str(c):
                rename[c] = "アカウントID"
            elif "url" in lc:
                rename[c] = "ルームURL"
        if rename:
            df.rename(columns=rename, inplace=True)
    except Exception:
        df = pd.read_csv(io.StringIO(txt), header=None)
        col_map = {}
        if df.shape[1] >= 1: col_map[0] = "ルームID"
        if df.shape[1] >= 2: col_map[1] = "ルーム名"
        if df.shape[1] >= 3: col_map[2] = "ルームURL"
        if df.shape[1] >= 4: col_map[3] = "アカウントID"
        df.rename(columns=col_map, inplace=True)
    if "ルームID" not in df.columns:
        return pd.DataFrame(columns=["ルームID"])
    df["ルームID"] = df["ルームID"].astype(str).str.strip()
    # 空のルームID行は除去
    df = df[df["ルームID"].str.strip() != ""].copy()
    df = df.set_index("ルームID", drop=False)
    return df

@st.cache_data(ttl=3600)
def load_event_archive(url: str) -> Dict[str, dict]:
    """sr-event-archive.csv を取得して event_id -> dict のマップを返す（補填用）"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
        df["event_id"] = df["event_id"].apply(lambda v: str(v).strip() if pd.notna(v) else "")
        df = df.dropna(subset=["event_id"])
        out = {}
        for _, row in df.iterrows():
            eid = str(row["event_id"])
            out[eid] = {
                "event_name": row.get("event_name"),
                "started_at": int(float(row["started_at"])) if pd.notna(row.get("started_at")) else None,
                "ended_at": int(float(row["ended_at"])) if pd.notna(row.get("ended_at")) else None,
                "image_m": row.get("image_m"),
                "event_url_key": row.get("event_url_key"),
                "show_ranking": row.get("show_ranking")
            }
        return out
    except Exception:
        return {}

# ---------- room_list ページ取得（堅牢化） ----------
def fetch_all_room_entries(event_id: int, max_pages: int = 500, sleep_between_pages: float = 0.03):
    """event_id の room_list を全ページ取得して entries (list) を返す"""
    entries = []
    page = 1
    while page <= max_pages:
        params = {"event_id": event_id, "p": page}
        data = http_get_json(API_ROOM_LIST, params=params, retries=3, timeout=12)
        if not data:
            break
        # ページ内の list を取得
        page_entries = data.get("list") or []
        if page_entries:
            entries.extend(page_entries)
        # last_page がわかればそこまで回す
        last_page = data.get("last_page")
        current_page = data.get("current_page") or page
        try:
            current_page_i = int(current_page)
        except Exception:
            current_page_i = page
        if last_page is not None:
            try:
                last_page_i = int(last_page)
                if current_page_i >= last_page_i:
                    break
                else:
                    page += 1
                    time.sleep(sleep_between_pages)
                    continue
            except Exception:
                # last_page が数字化できない場合は next_page を使う
                pass
        # next_page が None なら終端
        if data.get("next_page") is None:
            break
        # safety increment
        page += 1
        time.sleep(sleep_between_pages)
    return entries

# ---------- contribution_ranking を複数の room_id で試す ----------
def fetch_event_detail_try_many(event_id: int, candidate_room_ids: List[str], max_attempts_per_room: int = 2):
    """
    matched_entries の room_id を順に試し、event 情報が返るまで続ける。
    戻り値: dict (可能な限り event_name/started_at/ended_at/event_url/image を含む)
    """
    for rid in candidate_room_ids:
        params = {"event_id": event_id, "room_id": rid}
        data = http_get_json(API_CONTRIBUTION, params=params, retries=max_attempts_per_room, timeout=10)
        if not data:
            continue
        # data["event"] があるか
        if isinstance(data, dict) and "event" in data and isinstance(data["event"], dict):
            ev = data["event"]
            return {
                "event_name": ev.get("event_name"),
                "started_at": ev.get("started_at"),
                "ended_at": ev.get("ended_at"),
                "event_url": ev.get("event_url"),
                "event_image": ev.get("image")
            }
    return {}

# ---------- 1イベントを処理する（管理ルームと照合してレコード生成） ----------
def process_event(event_id: int, managed_rooms_df: pd.DataFrame, archive_map: dict, sleep_between_requests: float = 0.02):
    """
    event_id を処理し、managed_rooms に対応するレコード list を返す。
    - room_list を全部取る
    - 管理対象の room_id とマッチする entries を抽出
    - マッチがあれば contribution_ranking を候補 room_id で試し、event info を得る
    - archive_map を fallback に使用
    """
    recs = []
    entries = fetch_all_room_entries(event_id)
    if not entries:
        return recs

    # managed room id set (文字列)
    managed_ids = set(managed_rooms_df.index.astype(str).tolist())
    # 可能なら account_id も比較対象に入れる (補助)
    account_map = {}
    if "アカウントID" in managed_rooms_df.columns:
        account_map = {str(v): idx for idx, v in managed_rooms_df["アカウントID"].items() if pd.notna(v)}

    matched_entries = []
    for e in entries:
        rid = str(e.get("room_id")) if e.get("room_id") is not None else ""
        if rid in managed_ids:
            matched_entries.append(e)
            continue
        # fallback: account_id match
        a = e.get("account_id")
        if a and str(a) in account_map:
            # locate room id from managed_rooms_df by account -> use that row's room id
            idx = account_map[str(a)]
            # idx is index label (ルームID) - ensure it's string
            # note: managed_rooms_df.index contains room_ids, but account_map values are index labels; careful
            # We'll still append if rid exists in the entry (prefers entry's room_id)
            matched_entries.append(e)

    if not matched_entries:
        return recs

    # Try to fetch event-level detail via contribution API by trying multiple room_id candidates
    candidate_rids = [str(e.get("room_id")) for e in matched_entries if e.get("room_id")]
    detail = fetch_event_detail_try_many(event_id, candidate_rids)
    # fallback to archive map
    if not detail:
        detail = archive_map.get(str(event_id), {})
        # if archived, remap keys: archive has started_at, ended_at maybe as ints or strings
        if detail:
            # ensure keys names consistent
            detail = {
                "event_name": detail.get("event_name"),
                "started_at": detail.get("started_at"),
                "ended_at": detail.get("ended_at"),
                "event_url": ( "https://www.showroom-live.com/event/" + detail.get("event_url_key") ) if detail.get("event_url_key") else None,
                "event_image": detail.get("image_m") or detail.get("image")
            }

    # For each matched entry, build record
    for e in matched_entries:
        rid = str(e.get("room_id")) if e.get("room_id") is not None else ""
        # rank/point/quest_level
        rank = e.get("rank") or e.get("position") or "-"
        point = e.get("point") or e.get("event_point") or e.get("total_point") or 0
        quest_level = None
        try:
            quest_level = e.get("event_entry", {}).get("quest_level")
        except Exception:
            quest_level = None
        if quest_level is None:
            quest_level = e.get("quest_level") or 0
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
            "ライバー名": managed_rooms_df.loc[rid, "ルーム名"] if (rid in managed_rooms_df.index and "ルーム名" in managed_rooms_df.columns) else (e.get("room_name") or ""),
            "アカウントID": managed_rooms_df.loc[rid, "アカウントID"] if (rid in managed_rooms_df.index and "アカウントID" in managed_rooms_df.columns) else (e.get("account_id") or ""),
            "イベント名": detail.get("event_name") or None,
            "開始日時": fmt_time(detail.get("started_at")),
            "終了日時": fmt_time(detail.get("ended_at")),
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
        recs.append(rec)
        # 軽い遅延を入れて API 過負荷を避ける
        time.sleep(sleep_between_requests)
    return recs

# ---------- スキャン関数（改善） ----------
def check_event_has_any_page(event_id: int):
    """event_id の 1ページ目を叩いて list や total_entries, number_of_rooms を確認"""
    data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": 1}, retries=2, timeout=8)
    if not data:
        return False, None
    # 判定
    if isinstance(data, dict):
        if data.get("number_of_rooms") is not None:
            return int(data.get("number_of_rooms")) > 0, data
        if data.get("total_entries") is not None:
            try:
                return int(data.get("total_entries")) > 0, data
            except Exception:
                pass
        if "list" in data and isinstance(data["list"], list) and len(data["list"]) > 0:
            return True, data
    # default
    return False, data

def scan_event_ids(start_id: int, end_id: int, max_workers: int, progress_callback=None) -> List[int]:
    """指定範囲を並列スキャンして有効 event_id リストを返す"""
    ids = list(range(start_id, end_id + 1))
    valid = []
    total = len(ids)
    checked = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(2, min(max_workers, 30))) as ex:
        futures = {ex.submit(check_event_has_any_page, eid): eid for eid in ids}
        for fut in concurrent.futures.as_completed(futures):
            eid = futures[fut]
            checked += 1
            ok = False
            try:
                ok, data = fut.result()
            except Exception:
                ok = False
            if ok:
                valid.append(eid)
            if progress_callback:
                progress_callback(checked, total)
    return sorted(valid)

# ---------- メイン収集ロジック（進捗・途中保存つき） ----------
def fetch_and_build_database(event_start: int, event_end: int, max_workers: int, save_interval: int, save_path_ftp: str, sleep_between_requests: float = 0.02):
    st.info("🔎 スキャン開始")
    pbar = st.progress(0)
    st_write = st.empty()
    managed_rooms = load_managed_rooms(ROOM_LIST_URL)
    st.write(f"管理ルーム数: {len(managed_rooms)}")
    archive_map = load_event_archive(ARCHIVE_URL)
    st.write(f"アーカイブイベント数(補填用): {len(archive_map)}")

    # 1) スキャン
    def scan_progress(checked, total):
        pbar.progress(int(checked/total*100))
        st_write.text(f"scan: {checked}/{total}")
    valid_ids = scan_event_ids(event_start, event_end, max_workers=max_workers, progress_callback=scan_progress)
    pbar.progress(0)
    st.write(f"有効イベント候補数: {len(valid_ids)} (範囲 {event_start}〜{event_end})")

    if not valid_ids:
        st.warning("有効イベントが見つかりません。範囲やAPI状況を確認してください。")
        return pd.DataFrame()

    # 2) 並列で各イベントを処理
    total_events = len(valid_ids)
    processed = 0
    all_records = []
    saved_count = 0

    def save_partial(records, desc):
        if not records:
            return False
        df_tmp = pd.DataFrame(records)
        col_order = ["PR対象","ライバー名","アカウントID","イベント名","開始日時","終了日時",
                     "順位","ポイント","備考","紐付け","URL","レベル","event_id","ルームID","イベント画像（URL）"]
        for c in col_order:
            if c not in df_tmp.columns:
                df_tmp[c] = ""
        df_tmp = df_tmp[col_order]
        csv_bytes = df_tmp.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        try:
            ftp_upload_bytes(save_path_ftp, csv_bytes, retries=2)
            st.success(f"途中保存: {desc} (records={len(records)})")
            return True
        except Exception as e:
            st.warning(f"FTP保存失敗: {e}。ローカルダウンロードを用意します。")
            st.download_button("途中保存をダウンロード", data=csv_bytes, file_name=f"event_db_partial_{int(time.time())}.csv")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_event, eid, managed_rooms, archive_map, sleep_between_requests): eid for eid in valid_ids}
        for fut in concurrent.futures.as_completed(futures):
            eid = futures[fut]
            processed += 1
            try:
                recs = fut.result()
            except Exception as e:
                st.error(f"event_id={eid} 処理例外: {e}")
                recs = []
            if recs:
                all_records.extend(recs)
            # 進捗更新
            pbar.progress(int(processed/total_events*100))
            st_write.text(f"処理: {processed}/{total_events} (collected={len(all_records)})")

            # 途中保存: 件数ベース
            if save_interval > 0 and len(all_records) > 0 and (len(all_records) - saved_count) >= save_interval:
                ok = save_partial(all_records, f"{len(all_records)}件 / processed_events={processed}")
                if ok:
                    saved_count = len(all_records)

    # 最終整形
    if not all_records:
        st.info("完了しましたが、管理対象ルームの一致がありませんでした。")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # ensure columns
    col_order = ["PR対象","ライバー名","アカウントID","イベント名","開始日時","終了日時",
                 "順位","ポイント","備考","紐付け","URL","レベル","event_id","ルームID","イベント画像（URL）"]
    for c in col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[col_order]
    # 新しい event_id が上に来るようにソート（数値として）
    try:
        df["event_id_num"] = df["event_id"].astype(int)
        df.sort_values("event_id_num", ascending=False, inplace=True)
        df.drop(columns=["event_id_num"], inplace=True)
    except Exception:
        pass

    # 最終保存
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    try:
        ftp_upload_bytes(save_path_ftp, csv_bytes, retries=3)
        st.success(f"最終保存: FTP に保存しました ({save_path_ftp}) 件数={len(df)}")
    except Exception as e:
        st.warning(f"最終FTP保存に失敗しました: {e}。ダウンロードを用意します。")
        st.download_button("最終CSVをダウンロード", data=csv_bytes, file_name=f"event_database_{int(time.time())}.csv")

    return df

# ---------- UI ----------
st.set_page_config(page_title="SHOWROOM イベントデータ収集（改善版）", layout="wide")
st.title("SHOWROOM イベントデータ収集（改善版）")

with st.sidebar:
    st.header("設定")
    EVENT_ID_START = st.number_input("EVENT_ID_START", value=40290, step=1)
    EVENT_ID_END = st.number_input("EVENT_ID_END", value=40310, step=1)
    MAX_WORKERS = st.number_input("MAX_WORKERS", min_value=1, max_value=50, value=2, step=1)
    SAVE_INTERVAL = st.number_input("SAVE_INTERVAL(件数)", min_value=0, value=200, step=50)
    SAVE_PATH_FTP = st.text_input("FTP 保存パス", value="/mksoul-pro.com/showroom/file/event_database.csv")
    SLEEP_BETWEEN_REQUESTS = st.number_input("sleep_between_requests (秒)", min_value=0.0, value=0.02, step=0.01)
    st.markdown("FTP は st.secrets['ftp'] に設定してください。")

if st.button("実行: データ収集（スキャン→取得）"):
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
            st.success(f"完了: {len(df_res)} 件")
            st.dataframe(df_res.head(200))
            st.download_button("CSVダウンロード", data=df_res.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"), file_name="event_database.csv")
        else:
            st.info("結果は空です。ログを確認してください。")
    except Exception as e:
        st.error(f"致命エラー: {e}")
        st.exception(traceback.format_exc())
