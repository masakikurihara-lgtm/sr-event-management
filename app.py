# app.py
import streamlit as st
import requests
import pandas as pd
import io
from datetime import datetime, timezone, timedelta
import pytz
import time

JST = pytz.timezone("Asia/Tokyo")
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-tool/1.0)"}

st.set_page_config(page_title="SHOWROOM — ライバー別イベント可視化", layout="wide")

# ---------------- helpers ----------------
def http_get_json(url, params=None, timeout=10, retries=2, backoff=0.5):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 410):
                return None
            # 429 or other: wait and retry
            time.sleep(backoff * (i + 1))
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None

def parse_maybe_timestamp_or_fmt(v):
    """v は Unix秒（文字列/数値）か 'YYYY/MM/DD HH:MM' の文字列、もしくは空 -> return datetime or None"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            ts = int(v)
            if ts > 20000000000:
                ts = ts // 1000
            return datetime.fromtimestamp(ts, JST)
        except Exception:
            return None
    s = str(v).strip()
    if s == "" or s.lower() in ("none", "nan"):
        return None
    # try numeric string
    if s.isdigit():
        try:
            ts = int(s)
            if ts > 20000000000:
                ts = ts // 1000
            return datetime.fromtimestamp(ts, JST)
        except Exception:
            pass
    # try 'YYYY/MM/DD HH:MM' or similar
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            dt_naive = datetime.strptime(s, fmt)
            return JST.localize(dt_naive)
        except Exception:
            pass
    # last-resort: pandas parser
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        # ensure tz
        if dt.tzinfo is None:
            dt = dt.tz_localize(JST)
        else:
            dt = dt.tz_convert(JST)
        return dt
    except Exception:
        return None

def fmt_datetime_for_display(dt):
    if dt is None:
        return ""
    try:
        return dt.strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(dt)

def load_event_db(url=EVENT_DB_URL):
    """CSVを取得してDataFrameにし、必要列の存在を保証する"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
    except Exception as e:
        st.error(f"event_database.csv の取得に失敗しました: {e}")
        return pd.DataFrame()

    # Normalize columns (allow different column names)
    colmap = {}
    # target names we want: ライバー名, ルームID, event_id, イベント名, 開始日時, 終了日時, 順位, ポイント, レベル
    for c in df.columns:
        cs = c.strip()
        lower = cs.lower()
        if lower in ("ルームid", "roomid", "room_id", "ルームID".lower()):
            colmap[c] = "ルームID"
        if "ライバー" in cs or ("room" in lower and "name" in lower) or cs in ("ルーム名","room_name"):
            colmap[c] = "ライバー名"
        if lower in ("event_id", "eventid"):
            colmap[c] = "event_id"
        if "イベント名" in cs or "event_name" in lower:
            colmap[c] = "イベント名"
        if "開始" in cs and ("時" in cs or "日" in cs or "start" in lower):
            colmap[c] = "開始日時"
        if "終了" in cs and ("時" in cs or "日" in cs or "end" in lower):
            colmap[c] = "終了日時"
        if cs in ("順位", "rank", "Rank"):
            colmap[c] = "順位"
        if cs in ("ポイント", "point", "ポイント数"):
            colmap[c] = "ポイント"
        if cs in ("レベル", "quest_level", "レベル(クエスト)"):
            colmap[c] = "レベル"
    if colmap:
        df = df.rename(columns=colmap)
    # ensure columns exist
    want = ["ライバー名", "ルームID", "event_id", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "イベント画像（URL）"]
    for w in want:
        if w not in df.columns:
            df[w] = ""
    # trim and normalize types
    df["ルームID"] = df["ルームID"].astype(str).str.strip()
    df["event_id"] = df["event_id"].astype(str).str.strip()
    return df

def update_live_fields_for_row(event_id, room_id):
    """event_id の room_list を巡回して指定 room_id の rank/point/quest_level/room_name を探す。見つかれば dict を返す（ない場合は None）"""
    if (not event_id) or (not room_id):
        return None
    # try pages until found (safe limit)
    max_pages = 200
    p = 1
    while p <= max_pages:
        data = http_get_json(API_ROOM_LIST, params={"event_id": event_id, "p": p}, timeout=8, retries=2)
        if not data:
            return None
        page_list = data.get("list") or []
        for ent in page_list:
            rid = str(ent.get("room_id")) if ent.get("room_id") is not None else ""
            if rid == str(room_id):
                # extract
                rank = ent.get("rank") or ent.get("position") or ent.get("room_rank") or ent.get("順位") or ""
                point = ent.get("point") or ent.get("event_point") or ent.get("total_point") or 0
                # quest level may be under event_entry.quest_level
                quest_level = None
                try:
                    quest_level = ent.get("event_entry", {}).get("quest_level")
                except Exception:
                    quest_level = None
                if quest_level is None:
                    quest_level = ent.get("quest_level") or ent.get("level") or ""
                room_name = ent.get("room_name") or ent.get("room_name_text") or ""
                return {
                    "順位": rank,
                    "ポイント": int(point) if str(point).isdigit() else point,
                    "レベル": int(quest_level) if (isinstance(quest_level,(int,float)) or (str(quest_level).isdigit())) else quest_level,
                    "ライバー名": room_name or None
                }
        # next page?
        if data.get("next_page") is None:
            break
        p += 1
        time.sleep(0.02)
    return None

# ---------------- UI ----------------
st.title("SHOWROOM — ライバー別イベント履歴 / 可視化")

# 左サイド：設定
with st.sidebar:
    st.header("操作")
    room_input = st.text_input("表示するルームID（または管理者 'mksp154851'）", value="")
    st.markdown("**注意**: 管理者は `mksp154851` を入力してください。")
    st.markdown("---")
    st.markdown("データソース")
    st.write(EVENT_DB_URL)
    if st.button("DBを再取得（CSVを再読み込み）"):
        st.experimental_rerun()

# main load DB
df_db = load_event_db(EVENT_DB_URL)
if df_db.empty:
    st.warning("event_database.csv を取得できません。URL・ネットワークを確認してください。")
    st.stop()

# parse date columns to datetime for sorting & filtering
df_db["_開始_dt"] = df_db["開始日時"].apply(parse_maybe_timestamp_or_fmt)
df_db["_終了_dt"] = df_db["終了日時"].apply(parse_maybe_timestamp_or_fmt)

# filter by room_input
is_admin = (str(room_input).strip() == "mksp154851")
if (not room_input) and (not is_admin):
    st.info("表示したいルームIDを左の入力欄に入れてください（管理者は mksp154851 を入力）。")
    st.stop()

if is_admin:
    df_view = df_db.copy()
else:
    # filter rows where ルームID matches input OR account id? here just ルームID
    df_view = df_db[df_db["ルームID"].astype(str) == str(room_input).strip()].copy()

# Optional date filters (sidebar)
with st.sidebar:
    st.header("表示フィルタ")
    min_start = st.date_input("開始日時 >= (省略可)", value=None)
    max_end = st.date_input("終了日時 <= (省略可)", value=None)
    # convert date_input to datetimes if given
    min_start_dt = None
    max_end_dt = None
    try:
        if min_start is not None:
            min_start_dt = JST.localize(datetime.combine(min_start, datetime.min.time()))
    except Exception:
        min_start_dt = None
    try:
        if max_end is not None:
            max_end_dt = JST.localize(datetime.combine(max_end, datetime.max.time()))
    except Exception:
        max_end_dt = None

# apply date filters
if min_start_dt is not None:
    df_view = df_view[df_view["_開始_dt"].notna() & (df_view["_開始_dt"] >= min_start_dt)]
if max_end_dt is not None:
    df_view = df_view[df_view["_終了_dt"].notna() & (df_view["_終了_dt"] <= max_end_dt)]

# sort by 開始日時 desc (newest first). If missing 開始日時, push to bottom.
df_view["_start_sort_key"] = df_view["_開始_dt"].apply(lambda d: d.timestamp() if d is not None else -1)
df_view = df_view.sort_values("_start_sort_key", ascending=False).drop(columns=["_start_sort_key"])

st.write(f"表示件数: {len(df_view)}")

# Provide a button to refresh live data for ongoing events only (or for all shown)
col1, col2 = st.columns([2, 1])
with col1:
    if st.button("表示中のイベントを API で最新化（開催中のみ）"):
        # We'll iterate rows and for those where now < ended_at, call API to update rank/point/level
        now_ts = datetime.now(JST)
        updated = 0
        failed = 0
        progress_bar = st.progress(0)
        status_text = st.empty()
        total_rows = len(df_view)
        i = 0
        # we will build a copy to show updated values
        df_live = df_view.copy()
        for idx, row in df_view.iterrows():
            i += 1
            status_text.text(f"最新化中: {i}/{total_rows} (更新済み: {updated})")
            progress_bar.progress(int(i/total_rows*100))
            ended = row["_終了_dt"]
            # treat no end as not update
            if ended is None:
                time.sleep(0.01)
                continue
            if now_ts >= ended:
                # not ongoing
                time.sleep(0.01)
                continue
            # try update
            event_id = row.get("event_id") or ""
            room_id = row.get("ルームID") or ""
            try:
                res = update_live_fields_for_row(event_id, room_id)
                if res:
                    if res.get("順位") is not None:
                        df_live.at[idx, "順位"] = res.get("順位")
                    if res.get("ポイント") is not None:
                        df_live.at[idx, "ポイント"] = res.get("ポイント")
                    if res.get("レベル") is not None:
                        df_live.at[idx, "レベル"] = res.get("レベル")
                    if res.get("ライバー名"):
                        df_live.at[idx, "ライバー名"] = res.get("ライバー名")
                    updated += 1
                else:
                    failed += 1
                # be polite
                time.sleep(0.05)
            except Exception as e:
                failed += 1
                time.sleep(0.05)
        progress_bar.empty()
        status_text.text(f"完了: 更新 {updated} 件 / 失敗 {failed} 件")
        # replace view with df_live
        df_view = df_live

with col2:
    st.markdown("### 管理")
    if is_admin:
        if st.button("全件を API で最新化（注意: 処理重い）"):
            # For admin: try to update all rows that are ongoing
            now_ts = datetime.now(JST)
            updated = 0
            failed = 0
            total_rows = len(df_view)
            pb = st.progress(0)
            text = st.empty()
            i = 0
            df_live = df_view.copy()
            for idx, row in df_view.iterrows():
                i += 1
                text.text(f"処理中: {i}/{total_rows} (更新済: {updated})")
                pb.progress(int(i/total_rows*100))
                ended = row["_終了_dt"]
                if ended is None:
                    time.sleep(0.01)
                    continue
                if now_ts >= ended:
                    time.sleep(0.01)
                    continue
                event_id = row.get("event_id") or ""
                room_id = row.get("ルームID") or ""
                try:
                    res = update_live_fields_for_row(event_id, room_id)
                    if res:
                        if res.get("順位") is not None:
                            df_live.at[idx, "順位"] = res.get("順位")
                        if res.get("ポイント") is not None:
                            df_live.at[idx, "ポイント"] = res.get("ポイント")
                        if res.get("レベル") is not None:
                            df_live.at[idx, "レベル"] = res.get("レベル")
                        if res.get("ライバー名"):
                            df_live.at[idx, "ライバー名"] = res.get("ライバー名")
                        updated += 1
                    else:
                        failed += 1
                    time.sleep(0.05)
                except Exception:
                    failed += 1
                    time.sleep(0.05)
            pb.empty()
            text.text(f"完了: 更新 {updated} 件 / 失敗 {failed} 件")
            df_view = df_live

# Build HTML table with highlighting for ongoing events
def make_html_table(df_show):
    now_ts = datetime.now(JST)
    cols = ["ライバー名","イベント名","開始日時","終了日時","順位","ポイント","レベル"]
    html = "<table style='width:100%; border-collapse:collapse;'>"
    # header
    html += "<thead><tr>"
    for c in cols:
        html += f"<th style='padding:8px; border-bottom:1px solid #ddd; text-align:center;'>{c}</th>"
    html += "</tr></thead><tbody>"
    for idx, row in df_show.iterrows():
        started = row["_開始_dt"]
        ended = row["_終了_dt"]
        ongoing = False
        if ended is not None and now_ts < ended:
            ongoing = True
        tr_style = "background:#fff;" 
        if ongoing:
            tr_style = "background:#fff7cc;"  # pale highlight
        html += f"<tr style='{tr_style}'>"
        # ライバー名 -> link to room profile if ルームID exists
        room_id = row.get("ルームID") or ""
        name = row.get("ライバー名") or ""
        if room_id:
            name_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{name}</a>'
        else:
            name_html = name
        # イベント名 -> link if event_id or URL exists
        ev_name = row.get("イベント名") or ""
        ev_url = ""
        if row.get("event_id"):
            ev_url = f"https://www.showroom-live.com/event/{row.get('event_id')}"
        elif row.get("URL"):
            ev_url = row.get("URL")
        if ev_name and ev_url:
            ev_html = f'<a href="{ev_url}" target="_blank">{ev_name}</a>'
        else:
            ev_html = ev_name
        # prepare others
        start_txt = row.get("開始日時") or fmt_datetime_for_display(started)
        end_txt = row.get("終了日時") or fmt_datetime_for_display(ended)
        rank_txt = row.get("順位") or ""
        point_txt = row.get("ポイント") if pd.notna(row.get("ポイント")) else ""
        level_txt = row.get("レベル") if pd.notna(row.get("レベル")) else ""
        # build cells
        cells = [name_html, ev_html, start_txt, end_txt, str(rank_txt), str(point_txt), str(level_txt)]
        for cell in cells:
            html += f"<td style='padding:8px; border-bottom:1px solid #eee; text-align:center;'>{cell}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    return html

# Show table
if len(df_view) == 0:
    st.info("該当データはありません。")
else:
    st.markdown("### 結果一覧")
    st.markdown(make_html_table(df_view), unsafe_allow_html=True)
    st.markdown("---")
    st.write("※開催中の行はハイライトされています（黄色）")

# allow CSV download of the current view
csv_bytes = df_view.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("表示中データをCSVでダウンロード", data=csv_bytes, file_name="event_view.csv", mime="text/csv")
