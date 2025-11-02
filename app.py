# app.py
import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

st.set_page_config(page_title="Giải Chim Non Lần 2 — League Manager", layout="wide")

# ========== 1) SECRETS ==========
SECRETS = st.secrets
DATA_SOURCE = SECRETS.get("DATA_SOURCE", "sheets")
SHEET_NAME  = SECRETS.get("SHEET_NAME", "chimnon_backend_with_numbers")
ADMIN_PASSWORD = SECRETS.get("ADMIN_PASSWORD", "")
SA_INFO = dict(SECRETS.get("gspread_service_account", {}))
# Ưu tiên lấy SHEET_KEY ở cấp gốc; nếu ai đó lỡ đặt vào block thì fallback
SHEET_KEY = (SECRETS.get("SHEET_KEY", "") or SA_INFO.get("SHEET_KEY", "")).strip()

# ========== 2) KẾT NỐI GSPREAD ==========
@st.cache_resource(show_spinner=False)
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SA_INFO, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(show_spinner=False, ttl=120)
def list_sa_spreadsheets():
    try:
        client = get_gspread_client()
        return client.list_spreadsheet_files()
    except Exception as e:
        return [{"name": f"(không lấy được danh sách) — {e}", "id": ""}]

@st.cache_data(show_spinner=True, ttl=60)
def load_worksheet_df(sheet_key: str, ws_name: str) -> pd.DataFrame:
    """Đọc 1 worksheet thành DataFrame. Cache theo (sheet_key, ws_name) để tránh UnhashableParamError."""
    try:
        client = get_gspread_client()
        sh = client.open_by_key(sheet_key)
        ws = sh.worksheet(ws_name)
        rows = ws.get_all_records()
        return pd.DataFrame(rows)
    except Exception as e:
        # Log nhẹ để biết trạng thái
        st.info(f"Không đọc được worksheet '{ws_name}': {e}")
        return pd.DataFrame()

# ========== 3) TÍNH BXH ==========
def compute_standings(teams_df: pd.DataFrame, matches_df: pd.DataFrame) -> pd.DataFrame:
    if teams_df.empty or matches_df.empty:
        return pd.DataFrame()

    tdf = teams_df.copy()
    tdf.columns = [c.strip().lower() for c in tdf.columns]
    mdf = matches_df.copy()
    mdf.columns = [c.strip().lower() for c in mdf.columns]

    needed = {"home_team_id", "away_team_id", "home_goals", "away_goals"}
    if not needed.issubset(set(mdf.columns)):
        return pd.DataFrame()

    for c in ["home_goals", "away_goals"]:
        mdf[c] = pd.to_numeric(mdf[c], errors="coerce").fillna(0).astype(int)

    points, stats = {}, {}
    def ensure(tid):
        if tid not in points: points[tid] = 0
        if tid not in stats:  stats[tid] = {"P":0,"W":0,"D":0,"L":0,"GF":0,"GA":0,"GD":0}

    for _, r in mdf.iterrows():
        h, a = str(r["home_team_id"]).strip(), str(r["away_team_id"]).strip()
        hg, ag = int(r["home_goals"]), int(r["away_goals"])
        ensure(h); ensure(a)
        stats[h]["P"] += 1; stats[a]["P"] += 1
        stats[h]["GF"] += hg; stats[h]["GA"] += ag; stats[h]["GD"] = stats[h]["GF"]-stats[h]["GA"]
        stats[a]["GF"] += ag; stats[a]["GA"] += hg; stats[a]["GD"] = stats[a]["GF"]-stats[a]["GA"]
        if hg > ag: points[h]+=3; stats[h]["W"]+=1; stats[a]["L"]+=1
        elif hg < ag: points[a]+=3; stats[a]["W"]+=1; stats[h]["L"]+=1
        else: points[h]+=1; points[a]+=1; stats[h]["D"]+=1; stats[a]["D"]+=1

    name_col = "team_name" if "team_name" in tdf.columns else ("short_name" if "short_name" in tdf.columns else "team_id")
    rows = []
    for _, tr in tdf.iterrows():
        tid = str(tr.get("team_id", "")).strip()
        if not tid: continue
        s = stats.get(tid, {"P":0,"W":0,"D":0,"L":0,"GF":0,"GA":0,"GD":0})
        rows.append({
            "Team ID": tid,
            "Đội": tr.get(name_col, tid),
            "Trận": s["P"], "Thắng": s["W"], "Hòa": s["D"], "Thua": s["L"],
            "BT": s["GF"], "BB": s["GA"], "HS": s["GD"], "Điểm": points.get(tid,0)
        })
    df = pd.DataFrame(rows)
    if df.empty: return df
    df = df.sort_values(by=["Điểm","HS","BT"], ascending=[False,False,False]).reset_index(drop=True)
    df.insert(0, "Hạng", range(1, len(df)+1))
    return df

# ========== 4) UI ==========
st.title("Giải Chim Non Lần 2 — League Manager")

with st.expander("🔐 Kết nối & Debug", expanded=True):
    if DATA_SOURCE.lower() != "sheets":
        st.error('DATA_SOURCE không phải "sheets". Kiểm tra Secrets.')
    else:
        files = list_sa_spreadsheets()
        st.write("🔎 **SA nhìn thấy các file (tên / id)**")
        if files:
            try:
                st.dataframe(pd.DataFrame(files)[["name","id"]], use_container_width=True, height=180)
            except Exception:
                st.dataframe(pd.DataFrame(files), use_container_width=True, height=180)
        else:
            st.info("Service Account chưa thấy file nào. Hãy SHARE file Google Sheet cho email SA với quyền Editor.")

        if not SHEET_KEY:
            st.error("Chưa có SHEET_KEY trong Secrets (đặt ở cấp gốc, không nằm trong [gspread_service_account]).")
            st.stop()
        # Thử mở bằng KEY chỉ để xác nhận; không dùng đối tượng sh cho cache
        try:
            _client = get_gspread_client()
            _client.open_by_key(SHEET_KEY)
            st.success(f"✅ Mở bằng KEY: {SHEET_KEY}")
        except Exception as e:
            st.error(f"❌ Không mở được bằng KEY. Kiểm tra đã share đúng email SA.\n\n{e}")
            st.stop()

# ========== 5) ĐỌC DỮ LIỆU ==========
teams_df   = load_worksheet_df(SHEET_KEY, "teams")
players_df = load_worksheet_df(SHEET_KEY, "players")
matches_df = load_worksheet_df(SHEET_KEY, "matches")
events_df  = load_worksheet_df(SHEET_KEY, "events")

# ========== 6) TABS ==========
tab1, tab2, tab3 = st.tabs(["🏆 Bảng xếp hạng", "📅 Lịch thi đấu", "👤 Cầu thủ & Ghi bàn"])

with tab1:
    st.subheader("Bảng xếp hạng")
    if teams_df.empty or matches_df.empty:
        st.warning("Thiếu sheet 'teams' hoặc 'matches' → chưa thể tính BXH.")
    else:
        standings = compute_standings(teams_df, matches_df)
        st.dataframe(standings, use_container_width=True)

with tab2:
    st.subheader("Lịch thi đấu")
    if matches_df.empty:
        st.info("Chưa có dữ liệu 'matches'.")
    else:
        st.dataframe(matches_df, use_container_width=True)

with tab3:
    left, right = st.columns([2,1])
    with left:
        st.subheader("Danh sách cầu thủ")
        if players_df.empty:
            st.info("Chưa có dữ liệu 'players'.")
        else:
            st.dataframe(players_df, use_container_width=True)
    with right:
        st.subheader("Thống kê ghi bàn / thẻ")
        if events_df.empty:
            st.info("Chưa có dữ liệu 'events'.")
        else:
            ev = events_df.copy()
            ev.columns = [c.strip().lower() for c in ev.columns]
            if "event_type" in ev.columns and "player_id" in ev.columns:
                goals = (ev[ev["event_type"].str.lower() == "goal"]
                         .groupby("player_id").size().reset_index(name="Goals"))
                out = players_df.merge(goals, how="left", on="player_id")
                out["Goals"] = out["Goals"].fillna(0).astype(int)
                out = out.sort_values("Goals", ascending=False)
                keep_cols = [c for c in ["player_id","player_name","team_id","number","Goals"] if c in out.columns]
                st.dataframe(out[keep_cols], use_container_width=True)
            else:
                st.info("Sheet 'events' thiếu cột 'event_type' hoặc 'player_id'.")

st.caption(f"Cập nhật: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
