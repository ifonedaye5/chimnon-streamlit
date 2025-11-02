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

def compute_fairplay(events_df: pd.DataFrame) -> dict:
    """
    Tính điểm Fair-Play theo điều lệ:
      yellow = 1, second_yellow = 3, red = 3, yellow_plus_direct_red = 4
    (điểm càng thấp càng tốt)
    """
    if events_df is None or events_df.empty:
        return {}
    pts = {}
    for _, e in events_df.iterrows():
        team = str(e.get("team_id", "")).strip()
        et  = str(e.get("event_type", "")).strip().lower()
        if not team:
            continue
        add = 0
        if et == "yellow":
            add = 1
        elif et == "second_yellow":
            add = 3
        elif et == "red":
            add = 3
        elif et == "yellow_plus_direct_red":
            add = 4
        pts[team] = pts.get(team, 0) + add
    return pts

def compute_standings(teams_df: pd.DataFrame, matches_df: pd.DataFrame, events_df: pd.DataFrame=None) -> pd.DataFrame:
    """
    BXH theo đúng ưu tiên Điều lệ Thành Dũng:
      1) Đối đầu trực tiếp
      2) Hiệu số (GD)
      3) Bàn thắng (GF)
      4) Fair-Play (ít hơn xếp trên)
    Giữ nguyên format cột như bản cũ, có thêm cột FairPlay để minh bạch.
    """
    if teams_df.empty or matches_df.empty:
        return pd.DataFrame()

    # Chuẩn cột
    tdf = teams_df.copy()
    tdf.columns = [c.strip().lower() for c in tdf.columns]
    mdf = matches_df.copy()
    mdf.columns = [c.strip().lower() for c in mdf.columns]

    needed = {"home_team_id", "away_team_id", "home_goals", "away_goals"}
    if not needed.issubset(set(mdf.columns)):
        return pd.DataFrame()

    # Ép số
    for c in ["home_goals", "away_goals"]:
        mdf[c] = pd.to_numeric(mdf[c], errors="coerce").fillna(0).astype(int)

    # Bảng điểm thô
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
        if hg > ag:
            points[h]+=3; stats[h]["W"]+=1; stats[a]["L"]+=1
        elif hg < ag:
            points[a]+=3; stats[a]["W"]+=1; stats[h]["L"]+=1
        else:
            points[h]+=1; points[a]+=1; stats[h]["D"]+=1; stats[a]["D"]+=1

    # Fair-Play
    fair = compute_fairplay(events_df)
    # Tạo bảng hiển thị
    name_col = "team_name" if "team_name" in tdf.columns else ("short_name" if "short_name" in tdf.columns else "team_id")
    rows = []
    for _, tr in tdf.iterrows():
        tid = str(tr.get("team_id", "")).strip()
        if not tid: 
            continue
        s = stats.get(tid, {"P":0,"W":0,"D":0,"L":0,"GF":0,"GA":0,"GD":0})
        rows.append({
            "Team ID": tid,
            "Đội": tr.get(name_col, tid),
            "Trận": s["P"], "Thắng": s["W"], "Hòa": s["D"], "Thua": s["L"],
            "BT": s["GF"], "BB": s["GA"], "HS": s["GD"], "Điểm": points.get(tid,0),
            "FairPlay": fair.get(tid, 0)
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # ---- HEAD-TO-HEAD comparator ----
    from functools import cmp_to_key
    def head_to_head(t1: str, t2: str) -> int:
        """Trả về 1 nếu t1 > t2 (t1 xếp trên), -1 nếu t1 < t2, 0 nếu bằng theo đối đầu."""
        sub = mdf[((mdf["home_team_id"].astype(str)==t1) & (mdf["away_team_id"].astype(str)==t2)) |
                  ((mdf["home_team_id"].astype(str)==t2) & (mdf["away_team_id"].astype(str)==t1))]
        if sub.empty:
            return 0
        pts1 = pts2 = gd1 = gd2 = gf1 = gf2 = 0
        for _, m in sub.iterrows():
            h, a = str(m["home_team_id"]), str(m["away_team_id"])
            hg, ag = int(m["home_goals"]), int(m["away_goals"])
            if h == t1:
                gf1 += hg; gf2 += ag; gd1 += (hg-ag); gd2 += (ag-hg)
                if hg > ag: pts1 += 3
                elif hg < ag: pts2 += 3
                else: pts1 += 1; pts2 += 1
            elif a == t1:
                gf1 += ag; gf2 += hg; gd1 += (ag-hg); gd2 += (hg-ag)
                if ag > hg: pts1 += 3
                elif ag < hg: pts2 += 3
                else: pts1 += 1; pts2 += 1
        if pts1 != pts2: return 1 if pts1 > pts2 else -1
        if gd1  != gd2:  return 1 if gd1  > gd2  else -1
        if gf1  != gf2:  return 1 if gf1  > gf2  else -1
        return 0

    # Map nhanh chỉ số theo Team ID
    by_id = df.set_index("Team ID")

    def cmp(a: str, b: str) -> int:
        # 1) Đối đầu trực tiếp
        hh = head_to_head(a, b)
        if hh != 0:
            return -hh  # head_to_head trả 1 nghĩa là a tốt hơn -> sort tăng cần đảo dấu

        # 2) Hiệu số GD
        gd_a, gd_b = by_id.at[a, "HS"], by_id.at[b, "HS"]
        if gd_a != gd_b:
            return -1 if gd_a > gd_b else 1

        # 3) Bàn thắng GF
        gf_a, gf_b = by_id.at[a, "BT"], by_id.at[b, "BT"]
        if gf_a != gf_b:
            return -1 if gf_a > gf_b else 1

        # 4) Fair-Play (ít hơn xếp trên)
        fp_a, fp_b = by_id.at[a, "FairPlay"], by_id.at[b, "FairPlay"]
        if fp_a != fp_b:
            return -1 if fp_a < fp_b else 1

        return 0

    # Sắp xếp theo: Điểm (desc) trước rồi mới áp comparator để xử lý tie-break
    df = df.sort_values(by=["Điểm"], ascending=False).reset_index(drop=True)
    order = sorted(df["Team ID"].tolist(), key=cmp_to_key(cmp))
    df = by_id.loc[order].reset_index()

    # Cột Hạng
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
        # Chuẩn hoá tên cột để lọc nhóm
        tdf = teams_df.copy()
        tdf.columns = [c.strip().lower() for c in tdf.columns]
        mdf = matches_df.copy()
        mdf.columns = [c.strip().lower() for c in mdf.columns]

        view_mode = st.radio("Chế độ xem", ["Theo bảng (A/B)", "Tất cả"], horizontal=True)

        def standings_group(grp: str):
            # lọc theo cột 'group' trong cả teams và matches
            t_sub = tdf[tdf.get("group", "").astype(str).str.upper() == grp]
            m_sub = mdf[mdf.get("group", "").astype(str).str.upper() == grp]
            return compute_standings(t_sub, m_sub, events_df)

        if view_mode == "Theo bảng (A/B)":
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### Bảng A")
                st.dataframe(standings_group("A"), use_container_width=True)
            with c2:
                st.markdown("#### Bảng B")
                st.dataframe(standings_group("B"), use_container_width=True)
        else:
            # Gộp lại nhưng có cột 'Bảng' để dễ phân biệt
            sA = standings_group("A"); sA.insert(1, "Bảng", "A")
            sB = standings_group("B"); sB.insert(1, "Bảng", "B")
            merged = pd.concat([sA, sB], ignore_index=True)
            st.dataframe(merged, use_container_width=True)


with tab2:
    st.subheader("Lịch thi đấu")
    if matches_df.empty:
        st.info("Chưa có dữ liệu 'matches'.")
    else:
        # Chuẩn hoá cột
        tdf = teams_df.copy();  tdf.columns = [c.strip().lower() for c in tdf.columns]
        mdf = matches_df.copy(); mdf.columns = [c.strip().lower() for c in mdf.columns]

        # Map team_id -> team_name
        name_map = dict(zip(tdf.get("team_id", pd.Series(dtype=str)),
                            tdf.get("team_name", pd.Series(dtype=str))))
        mdf["Đội chủ nhà"] = mdf["home_team_id"].map(name_map).fillna(mdf["home_team_id"])
        mdf["Đội khách"]   = mdf["away_team_id"].map(name_map).fillna(mdf["away_team_id"])

        # Bộ lọc
        col1, col2, col3 = st.columns([1,1,1.2])
        with col1:
            grp = st.selectbox("Chọn bảng", ["Tất cả", "A", "B"])
        with col2:
            view_mode = st.selectbox("Chế độ hiển thị", ["Tách theo vòng", "Gộp tất cả"])
        with col3:
            # Khi ở chế độ "Gộp tất cả" mới cho lọc một vòng riêng
            rounds_all = sorted(pd.Series(mdf.get("round", [])).dropna().unique().tolist())
            rnd = st.selectbox("Chọn vòng", ["Tất cả"] + rounds_all)

        # Áp bộ lọc dữ liệu nền
        show = mdf.copy()
        if grp != "Tất cả":
            show = show[show.get("group", "").astype(str).str.upper() == grp]
        if view_mode == "Gộp tất cả" and rnd != "Tất cả":
            show = show[show.get("round", "") == rnd]

        # Chuẩn các cột hiển thị + header tiếng Việt
        def beautify(df: pd.DataFrame) -> pd.DataFrame:
            cols = [
                "match_id","stage","group","round","date","time","venue",
                "Đội chủ nhà","Đội khách","home_goals","away_goals","status","notes"
            ]
            cols = [c for c in cols if c in df.columns]
            return df[cols].rename(columns={
                "match_id": "Mã trận",
                "stage": "Giai đoạn",
                "group": "Bảng",
                "round": "Vòng",
                "date": "Ngày",
                "time": "Giờ",
                "venue": "Sân đấu",
                "home_goals": "BT Chủ nhà",
                "away_goals": "BT Khách",
                "status": "Trạng thái",
                "notes": "Ghi chú"
            })

        # Hiển thị
        if view_mode == "Tách theo vòng":
            if show.empty:
                st.info("Không có trận nào khớp bộ lọc.")
            else:
                # Danh sách vòng còn lại sau khi lọc theo bảng
                rounds = sorted(pd.Series(show.get("round", [])).dropna().unique().tolist())
                for r in rounds:
                    sub = show[show.get("round", "") == r].copy()
                    st.markdown(f"### Vòng {r}")
                    # Sắp xếp đẹp theo Ngày → Giờ → Sân
                    if {"date","time","venue"}.issubset(sub.columns):
                        sub = sub.sort_values(by=["date","time","venue","match_id"])
                    st.dataframe(beautify(sub), use_container_width=True)
                    st.divider()
        else:
            # Gộp tất cả vào một bảng
            if {"date","time","venue"}.issubset(show.columns):
                show = show.sort_values(by=["date","time","venue","match_id"])
            st.dataframe(beautify(show), use_container_width=True)



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
