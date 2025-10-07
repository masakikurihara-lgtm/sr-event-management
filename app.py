# app.py — SHOWROOM: 参加イベント履歴ビューア（開催中判定を event_database.csv で行い、
#                参加中は /api/room/event_and_support で最新化する実装）
import streamlit as st
import requests
import pandas as pd
import io
import time
from datetime import datetime
import pytz

JST = pytz.timezone("Asia/Tokyo")

# --- 設定（必要に応じて差し替えてください） ---
EVENT_DB_URL = "https://mksoul-pro.com/showroom/file/event_database.csv"
API_ROOM_PROFILE = "https://www.showroom-live.com/api/room/profile"
API_ROOM_EVENT_AND_SUPPORT = "https://www.showroom-live.com/api/room/event_and_support"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mksoul-view/1.2)"}

st.set_page_config(page_title="SHOWROOM：参加イベント履歴（簡易ビュー）", layout="wide")


# ---------- ヘルパー ----------
def http_get_json(url, params=None, retries=2, timeout=8, backoff=0.5):
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
    """Unix秒（またはミリ秒）か既成フォーマット文字列を受け取り 'YYYY/MM/DD HH:MM' を返す"""
    if ts is None or ts == "" or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    # 文字列で既に '/' を含むならそのままトリムして返す
    if isinstance(ts, str):
        s = ts.strip()
        if s == "":
            return ""
        if "/" in s:
            return s
        # 数値文字列の可能性
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
    """'YYYY/MM/DD HH:MM' または Unix 秒 -> unix秒(int) を返す。失敗時 None"""
    if val is None or val == "":
        return None
    # 数値ならそのまま
    try:
        n = int(float(val))
        if n > 20000000000:
            n = n // 1000
        return n
    except Exception:
        pass
    # 文字列日付パース
    try:
        return int(datetime.strptime(val, "%Y/%m/%d %H:%M").timestamp())
    except Exception:
        return None


def load_event_db(url):
    """event_database.csv を取得して列名クリーンアップして返す"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        txt = r.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(txt), dtype=str)
    except Exception as e:
        st.error(f"イベントDB取得失敗: {e}")
        return pd.DataFrame()

    # 列名から余分な "_fmt" を除去して正規化
    new_cols = {}
    for c in df.columns:
        nc = str(c).strip()
        if nc.endswith("_fmt"):
            nc = nc[:-4]
        new_cols[c] = nc
    df.rename(columns=new_cols, inplace=True)

    # 必須列の保障（不足があれば空で追加）
    must = ["event_id", "URL", "ルームID", "イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "ライバー名"]
    for c in must:
        if c not in df.columns:
            df[c] = ""

    # trim
    df = df.applymap(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def get_latest_room_name(room_id):
    """room/profile で room_name を取ってくる（失敗時は空）"""
    data = http_get_json(API_ROOM_PROFILE, params={"room_id": room_id}, retries=2, timeout=6)
    if not data or not isinstance(data, dict):
        return ""
    return data.get("room_name") or data.get("name") or ""


def get_event_and_support_for_room(room_id):
    """/api/room/event_and_support?room_id= を呼んで rank, point, quest_level を返す（見つからなければ None）"""
    data = http_get_json(API_ROOM_EVENT_AND_SUPPORT, params={"room_id": room_id}, retries=2, timeout=6)
    if not data or not isinstance(data, dict):
        return None
    # 直接キーがある場合
    rank = data.get("rank") or data.get("position")
    point = data.get("point") or data.get("event_point") or data.get("total_point")
    quest = data.get("quest_level") or data.get("questLevel") or data.get("quest")
    # 場合によりネストされているケースを吸収
    if rank is None and "event" in data and isinstance(data["event"], dict):
        ev = data["event"]
        rank = rank or ev.get("rank") or ev.get("position")
        point = point or ev.get("point") or ev.get("event_point")
        quest = quest or ev.get("quest_level")
    # 別のフィールド名を探す（深掘り）
    if rank is None:
        # 再帰的に探索して rank/point/quest_level がまとまってる dict を探す
        def find(d):
            if isinstance(d, dict):
                if any(k in d for k in ("rank", "point", "quest_level", "questLevel")):
                    return d
                for v in d.values():
                    res = find(v)
                    if res:
                        return res
            elif isinstance(d, list):
                for item in d:
                    res = find(item)
                    if res:
                        return res
            return None
        found = find(data)
        if found:
            rank = rank or found.get("rank") or found.get("position")
            point = point or found.get("point") or found.get("event_point")
            quest = quest or found.get("quest_level") or found.get("questLevel")
    # 型変換
    try:
        if point is not None:
            point = int(point)
    except Exception:
        try:
            point = int(float(point))
        except Exception:
            point = 0
    try:
        quest = int(quest) if quest is not None else 0
    except Exception:
        quest = 0
    if rank is None and (point == 0 and quest == 0):
        # 中身薄ければ無効と判断
        return None
    return {"rank": rank if rank is not None else "-", "point": point, "quest_level": quest}


# ---------- UI ----------
st.title("🎤 SHOWROOM：参加イベント履歴ビューア")

with st.sidebar:
    st.write("### 操作")
    room_input = st.text_input("表示するルームIDを入力", value="")
    # 管理者（全件表示）キー（例: mksp154851）
    st.write("（管理者用: ルームID に 'mksp154851' と入力すると全件表示になります）")
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

# load DB
with st.spinner("イベントDBを取得中..."):
    df_all = load_event_db(EVENT_DB_URL)

if df_all.empty:
    st.stop()

is_admin_all = (room_id == "mksp154851")

# filter by room
if not is_admin_all:
    df = df_all[df_all["ルームID"].astype(str) == str(room_id)].copy()
else:
    df = df_all.copy()

if df.empty:
    st.warning("該当するデータが見つかりません。")
    st.stop()

# 最新のライバー名（上部ラベル）
latest_name = get_latest_room_name(room_id) if not is_admin_all else ""
display_name = latest_name if latest_name else (df.iloc[0].get("ライバー名") or room_id)
link_html = f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{display_name}</a>'
st.markdown(f'<div style="font-size:20px; font-weight:700; color:#1a66cc; margin-bottom:8px;">{link_html} の参加イベント</div>', unsafe_allow_html=True)

# 正規化：開始/終了日時表示列を統一
df["開始日時"] = df["開始日時"].apply(fmt_time_from_any)
df["終了日時"] = df["終了日時"].apply(fmt_time_from_any)
df["__start_ts"] = df["開始日時"].apply(parse_to_ts)
df["__end_ts"] = df["終了日時"].apply(parse_to_ts)

# sort by start desc (新しい順)
df.sort_values(by="__start_ts", ascending=False, inplace=True, na_position="last")

# サイドバー：日付フィルタ（プルダウン、降順）
st.sidebar.markdown("---")
st.sidebar.header("日付で絞り込み")
start_choices = [x for x in df["開始日時"].dropna().unique().tolist() if x != ""]
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

# 判定基準: CSV の 終了日時 が現在時刻より未来 -> 「開催中」とみなす
now_ts = int(datetime.now(JST).timestamp())
df["is_ongoing_by_csv"] = df["__end_ts"].apply(lambda x: (x is not None and x > now_ts))

# もし CSV に開催中レコードがあれば（＝参加中の判定）、event_and_support で最新化する
# ※ユーザー指示: 「開催中の判定は CSV の終了日時で行う」→ その場合のみ API 呼び出しを行う
ongoing_rows = df[df["is_ongoing_by_csv"] == True]
if not ongoing_rows.empty and not is_admin_all:
    st.info(f"CSV上で開催中と判断されたイベントが {len(ongoing_rows)} 件あります。参加中として API で最新化を行います...")
    with st.spinner("参加中イベントの最新情報を取得中..."):
        # API はルーム単位で現在参加中イベント情報を返す想定（room_id を指定）
        evs = get_event_and_support_for_room(room_id)
        if evs:
            # evs の rank/point/quest_level を開催中の行に反映（該当する event_id に限定する情報が無ければ、すべての開催中行に適用）
            for idx, row in ongoing_rows.iterrows():
                # 優先：もし API が event_id を返すなどの厳密なマッチ手段があれば使う（今回は event_and_support に event_id が来ない想定）
                df.at[idx, "順位"] = evs.get("rank", df.at[idx, "順位"])
                df.at[idx, "ポイント"] = str(evs.get("point", df.at[idx, "ポイント"]))
                df.at[idx, "レベル"] = str(evs.get("quest_level", df.at[idx, "レベル"]))
            st.success("参加中イベントを API で最新化しました。")
        else:
            st.info("参加中API の応答がありませんでした。CSV の値を表示します。")

# 表示用列の整備とソート（開始日降順は維持）
display_cols = ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル", "URL"]
for c in display_cols:
    if c not in df.columns:
        df[c] = ""

df_show = df[display_cols].copy()

# HTML テーブル（固定高さスクロール、ヘッダ色付け、ヘッダ中央揃え、開催中行ハイライト）
def make_html_table(df_in):
    html = """
    <style>
    .scroll-table { height:520px; overflow-y:auto; border:1px solid #ddd; border-radius:6px; }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    thead th {
        position: sticky; top:0; background:#0b66c2; color:#fff; padding:8px; text-align:center;
    }
    tbody td { padding:8px; border-bottom:1px solid #f2f2f2; text-align:center; vertical-align:middle; }
    tr.ongoing { background:#fff7cc; }
    a.evlink { color:#0b57d0; text-decoration:none; }
    </style>
    <div class="scroll-table"><table><thead><tr>"""
    for c in ["イベント名", "開始日時", "終了日時", "順位", "ポイント", "レベル"]:
        html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"
    for idx, r in df_in.iterrows():
        ongoing = False
        try:
            ongoing = bool(r.get(" __end_ts") or r.get("__end_ts"))  # safe check
        except Exception:
            ongoing = False
        # 正しい判定は元 df にある is_ongoing_by_csv（参照）
        ongoing_flag = False
        try:
            ongoing_flag = bool(df.loc[idx, "is_ongoing_by_csv"])
        except Exception:
            ongoing_flag = False
        tr_class = "ongoing" if ongoing_flag else ""
        ev_name = r.get("イベント名") or ""
        url = r.get("URL") or ""
        ev_link = f'<a class="evlink" href="{url}" target="_blank">{ev_name}</a>' if url else ev_name
        html += f'<tr class="{tr_class}">'
        html += f"<td>{ev_link}</td>"
        html += f"<td>{r.get('開始日時','')}</td>"
        html += f"<td>{r.get('終了日時','')}</td>"
        html += f"<td>{r.get('順位','')}</td>"
        html += f"<td>{r.get('ポイント','')}</td>"
        html += f"<td>{r.get('レベル','')}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html

st.markdown(make_html_table(df_show), unsafe_allow_html=True)
st.caption("黄色行は CSV 上で『開催中』（終了日時が現在より未来）のイベントです。\n（開催中の場合は /api/room/event_and_support で順位/ポイント/レベルを最新取得します）")

# CSV ダウンロード（表示用）
csv_bytes = df_show.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("結果をCSVダウンロード", data=csv_bytes, file_name="event_history_view.csv", mime="text/csv")
