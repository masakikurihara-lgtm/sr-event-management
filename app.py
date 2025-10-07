# app.py — SHOWROOM イベント履歴ビューア（置換用）
import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

# ---------- 設定 ----------
JST = pytz.timezone("Asia/Tokyo")
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"  # 必要に応じて差し替え
API_ROOM_LIST = "https://www.showroom-live.com/api/event/room_list"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.1)"}

st.set_page_config(page_title="SHOWROOM イベント履歴ビューア", layout="wide")


# ---------- ヘルパー ----------
def http_get_json(url, params=None, retries=3, timeout=10, backoff=0.7):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            if r.status_code in (404, 410):
                return None
            time.sleep(backoff * (i + 1))
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None


def fmt_time_from_any(ts):
    """数値（Unix秒またはミリ秒）かフォーマット済文字列を受け、'YYYY/MM/DD HH:MM' を返す。無効なら空文字"""
    if ts is None:
        return ""
    # 既にフォーマット済ならそのまま（YYYY/... など）
    if isinstance(ts, str):
        s = ts.strip()
        if s == "":
            return ""
        # すでに見やすい形式なら返す（判定：スラッシュがあればそのまま）
        if "/" in s:
            return s
        # 数字文字列の可能性
        try:
            n = int(float(s))
            if n > 20000000000:
                n = n // 1000
            return datetime.fromtimestamp(n, JST).strftime("%Y/%m/%d %H:%M")
        except Exception:
            return s
    try:
        n = int(float(ts))
        if n > 20000000000:
            n = n // 1000
        return datetime.fromtimestamp(n, JST).strftime("%Y/%m/%d %H:%M")
    except Exception:
        return ""


def parse_to_ts(val):
    """'YYYY/MM/DD HH:MM' 或いは Unix 秒 -> unix秒(int) を返す。失敗時は None"""
    if val is None or val == "":
        return None
    # 数字（Unix秒）なら直接
    try:
        n = int(float(val))
        if n > 20000000000:
            n = n // 1000
        return n
    except Exception:
        pass
    # 文字列 YYYY/MM/DD
    try:
        return int(datetime.strptime(val, "%Y/%m/%d %H:%M").timestamp())
    except Exception:
        return None


def load_event_db(url):
    """event_database.csv を安全に読み込む。列名の後処理も行う"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
    except Exception as e:
        st.error(f"イベントデータベースの取得に失敗しました: {e}")
        return pd.DataFrame()

    # 列名クリーニング: _fmt や余計な空白を削除
    new_cols = {}
    for c in df.columns:
        nc = c.strip()
        if nc.endswith("_fmt"):
            nc = nc[: -4]
        new_cols[c] = nc
    df.rename(columns=new_cols, inplace=True)

    # 必須列の存在チェックとデフォルト付与
    must = ["event_id", "URL", "ルームID", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "ライバー名"]
    for c in must:
        if c not in df.columns:
            df[c] = ""

    # 文字列トリム
    df = df.applymap(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def get_latest_room_name(room_id):
    data = http_get_json(API_ROOM_PROFILE, params={"room_id": room_id}, retries=2, timeout=6)
    if not data:
        return ""
    # APIによっては辞書のまま返るので安全に取り出す
    if isinstance(data, dict):
        # fallback keys: 'room_name' か 'name'
        return data.get("room_name") or data.get("name") or ""
    return ""


def update_live_fields_for_event_room(event_id, room_id, max_pages=10):
    """
    指定 event_id 内で room_id を検索し、見つかったら rank/point/quest_level/room_name を返す。
    pages を 1..max_pages で検索（APIの仕様で p= の形）
    """
    for p in range(1, max_pages + 1):
        params = {"event_id": event_id, "p": p}
        data = http_get_json(API_ROOM_LIST, params=params, retries=2, timeout=8)
        if not data:
            continue
        page_list = data.get("list") or []
        for e in page_list:
            # API は文字列の room_id もあるので両方扱う
            rid = e.get("room_id")
            if rid is None:
                continue
            if str(rid) == str(room_id):
                rank = e.get("rank") or e.get("position") or "-"
                point = e.get("point") or e.get("event_point") or e.get("total_point") or 0
                quest = None
                try:
                    quest = e.get("event_entry", {}).get("quest_level")
                except Exception:
                    quest = e.get("quest_level") or None
                room_name = e.get("room_name") or ""
                # normalize
                try:
                    point = int(point)
                except Exception:
                    try:
                        point = int(float(point))
                    except Exception:
                        point = 0
                try:
                    quest = int(quest)
                except Exception:
                    quest = 0
                return {"順位": rank, "ポイント": point, "レベル": quest, "ライバー名": room_name}
        # 次ページへ（軽いウェイト）
        time.sleep(0.05)
    return None


# ---------- UI ----------
st.title("🎤 SHOWROOM イベント履歴ビューア")

with st.sidebar:
    st.write("### 操作")
    room_input = st.text_input("表示したいルームIDを入力", value="")
    max_pages_search = st.number_input("開催中更新時に検索する最大ページ数 (p=)", min_value=1, max_value=50, value=10, step=1)
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

# 読み込み
with st.spinner("イベントデータベースを取得中..."):
    df_all = load_event_db(EVENT_DB_URL)

if df_all.empty:
    st.stop()

# 管理者キー: 全表示（要望にあった mksp154851）
is_admin_all = (room_id == "mksp154851")

# フィルタ：ルームID（管理者は全件）
if not is_admin_all:
    df = df_all[df_all["ルームID"].astype(str) == str(room_id)]
else:
    df = df_all.copy()

if df.empty:
    st.warning("該当ルームのデータが見つかりません。")
    st.stop()

# ライバー名最新化（上部ラベル用）
latest_name = get_latest_room_name(room_id) if not is_admin_all else ""
display_name = latest_name if latest_name else (df.iloc[0].get("ライバー名") or "")

# ラベル（ご提示の形式）
link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{display_name or room_id}</a>'
st.markdown(f'<div class="tracking-success" style="font-size:20px; font-weight:700; color:#1a66cc; margin-bottom:8px;">{link_html} の参加イベント</div>', unsafe_allow_html=True)

# フォーマット列補正（開始/終了日時を統一）
df["開始日時"] = df["開始日時"].apply(lambda v: fmt_time_from_any(v))
df["終了日時"] = df["終了日時"].apply(lambda v: fmt_time_from_any(v))

# 開始/終了のタイムスタンプ列を作る（ソートや判定用）
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)

# ソート: 開始日時の新しいものが上（欠損は下）
df.sort_values(by="__start_ts", ascending=False, inplace=True, na_position="last")

# 日付フィルタ（プルダウン、降順）
st.sidebar.markdown("---")
st.sidebar.header("日付で絞り込み")
start_choices = [x for x in df["開始日時"].dropna().unique().tolist() if x != ""]
# sort by __start_ts descending
start_map = {r["開始日時"]: r["__start_ts"] for _, r in df[["開始日時", "__start_ts"]].drop_duplicates().to_dict("records")}
start_choices_sorted = sorted(start_choices, key=lambda x: start_map.get(x, 0), reverse=True)
selected_start = st.sidebar.selectbox("開始日を選択", ["すべて"] + start_choices_sorted)

end_choices = [x for x in df["終了日時"].dropna().unique().tolist() if x != ""]
end_map = {r["終了日時"]: r["__end_ts"] for _, r in df[["終了日時", "__end_ts"]].drop_duplicates().to_dict("records")}
end_choices_sorted = sorted(end_choices, key=lambda x: end_map.get(x, 0), reverse=True)
selected_end = st.sidebar.selectbox("終了日を選択", ["すべて"] + end_choices_sorted)

if selected_start != "すべて":
    df = df[df["開始日時"] == selected_start]
if selected_end != "すべて":
    df = df[df["終了日時"] == selected_end]

if df.empty:
    st.info("選択条件に該当するデータがありません。")
    st.stop()

# --- 開催中イベントの最新化（順位/ポイント/レベル） ---
now_ts = int(datetime.now(JST).timestamp())
# 対象行だけ抽出して更新（進捗表示）
ongoing_mask = df["__end_ts"].apply(lambda x: (x is not None and now_ts < int(x)))
ongoing_rows = df[ongoing_mask]
if not ongoing_rows.empty:
    st.info(f"開催中イベントを最新化します（{len(ongoing_rows)} 件）...")
    p = st.progress(0)
    updated = 0
    for i, (idx, row) in enumerate(ongoing_rows.iterrows(), start=1):
        eid = row.get("event_id")
        rid = row.get("ルームID")
        if not eid or not rid:
            continue
        upd = update_live_fields_for_event_room(eid, rid, max_pages=max_pages_search)
        if upd:
            # 更新を反映
            if "順位" in df.columns:
                df.at[idx, "順位"] = upd.get("順位", df.at[idx, "順位"])
            if "ポイント" in df.columns:
                df.at[idx, "ポイント"] = str(upd.get("ポイント", df.at[idx, "ポイント"]))
            if "レベル" in df.columns:
                df.at[idx, "レベル"] = str(upd.get("レベル", df.at[idx, "レベル"]))
            # ライバー名も更新（表示ラベル等）
            if upd.get("ライバー名"):
                df.at[idx, "ライバー名"] = upd.get("ライバー名")
                if not display_name:
                    display_name = upd.get("ライバー名")
        updated += 1
        p.progress(int(i / len(ongoing_rows) * 100))
    st.success(f"開催中イベントの最新化完了（{updated} 更新）")

# テーブル表示用データ整形（表示列順）
display_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL"]
df_show = df.copy()
# 保険：URL列が別名の場合のフォールバック
if "URL" not in df_show.columns and "event_url" in df_show.columns:
    df_show["URL"] = df_show["event_url"]

# HTML テーブル作成（固定高さ + スクロール + ヘッダ色 + ヘッダセンタリング）
def make_html_table(df_in):
    cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]
    html = """
    <style>
    .scroll-table { height:520px; overflow-y:auto; border:1px solid #ddd; border-radius:6px; }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    thead th {
        position: sticky; top:0; background:#1a66cc; color:#fff; padding:8px; text-align:center;
    }
    tbody td { padding:8px; border-bottom:1px solid #f2f2f2; text-align:center; vertical-align:middle; }
    tr.ongoing { background:#fff7cc; }
    a.evlink { color:#0b57d0; text-decoration:none; }
    </style>
    <div class="scroll-table"><table><thead><tr>"""
    for c in cols:
        html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"
    for _, r in df_in.iterrows():
        ongoing = False
        try:
            et = r["__end_ts"]
            ongoing = (et is not None and now_ts < int(et))
        except Exception:
            ongoing = False
        tr_class = "ongoing" if ongoing else ""
        name = r.get("イベント名") or ""
        url = r.get("URL") or ""
        ev_html = f'<a class="evlink" href="{url}" target="_blank">{name}</a>' if url else name
        html += f'<tr class="{tr_class}">'
        html += f"<td>{ev_html}</td>"
        html += f"<td>{r.get('開始日時','')}</td>"
        html += f"<td>{r.get('終了日時','')}</td>"
        html += f"<td>{r.get('順位','')}</td>"
        html += f"<td>{r.get('ポイント','')}</td>"
        html += f"<td>{r.get('レベル','')}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html

st.markdown(make_html_table(df_show), unsafe_allow_html=True)
st.caption("黄色行は開催中のイベント（終了日時が未来）です。")

# CSVダウンロード
csv_bytes = df_show[["イベント名","開始日時","終了日時","順位","ポイント","レベル","URL"]].to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("結果をCSVダウンロード", data=csv_bytes, file_name="event_history_view.csv", mime="text/csv")
