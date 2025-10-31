# app.py — Chim Non League Manager (Streamlit + Excel/Google Sheets)
# ------------------------------------------------------------------
import os, json
from dataclasses import dataclass
from typing import Dict
import pandas as pd
import streamlit as st

# ================= Config & constants =================
APP_TITLE = "Giải Chim Non Lần 2 — League Manager"
DATA_FILE = os.getenv("DATA_FILE", "chimnon_template.xlsx")   # fallback Excel
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "chimnon2025")

FAIRPLAY_POINTS = {
    "yellow": int(os.getenv("FAIRPLAY_YELLOW", 1)),
    "second_yellow": int(os.getenv("FAIRPLAY_SECOND_YELLOW", 3)),
    "red": int(os.getenv("FAIRPLAY_RED", 3)),
    "yellow_plus_direct_red": int(os.getenv("FAIRPLAY_YELLOW_PLUS_DIRECT_RED", 4)),
}

# ================= Data loaders =================
@st.cache_data(show_spinner=False)
def load_excel(path: str) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path)
    dfs = {name: xls.parse(name) for name in xls.sheet_names}
    for k in list(dfs.keys()):
        dfs[k] = dfs[k].fillna("")
    return dfs

@st.cache_data(show_spinner=False)
def load_sheets(sheet_name: str) -> Dict[str, pd.DataFrame]:
    """Read all worksheets from a Google Sheet shared to Service Account."""
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        dict(st.secrets["gspread_service_account"]), scope
    )
    gc = gspread.authorize(creds)
    sh = gc.open(sheet_name)
    dfs = {}
    for ws in sh.worksheets():
        try:
            dfs[ws.title] = pd.DataFrame(ws.get_all_records()).fillna("")
        except Exception:
            dfs[ws.title] = pd.DataFrame()
    return dfs

@st.cache_data(show_spinner=False)
def load_settings(dfs: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    if "settings" not in dfs or dfs["settings"].empty:
        return {}
    return {str(k): str(v) for k, v in dfs["settings"].iloc[0].to_dict().items()}

@st.cache_data(show_spinner=False)
def get_ids_map(df: pd.DataFrame, id_col: str, label_col: str) -> Dict[str, str]:
    if df.empty: return {}
    return {str(r.get(id_col, "")): str(r.get(label_col, "")) for _, r in df.iterrows()}

def save_excel(path: str, dfs: Dict[str, pd.DataFrame]):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, index=False, sheet_name=name)

# ================= Fair-play & standings =================
def compute_fairplay(events: pd.DataFrame) -> Dict[str, int]:
    points = {}
    if events.empty: return points
    for _, e in events.iterrows():
        team = str(e.get("team_id", "")); etype = str(e.get("event_type", "")).lower()
        if not team: continue
        add = 0
        if etype == "yellow": add = FAIRPLAY_POINTS["yellow"]
        elif etype == "second_yellow": add = FAIRPLAY_POINTS["second_yellow"]
        elif etype == "red": add = FAIRPLAY_POINTS["red"]
        elif etype == "yellow_plus_direct_red": add = FAIRPLAY_POINTS["yellow_plus_direct_red"]
        points[team] = points.get(team, 0) + add
    return points

def compute_group_table(group_name: str, teams_df: pd.DataFrame, matches_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    teams = teams_df[teams_df["group"].astype(str).str.upper()==str(group_name).upper()].copy()
    team_ids = teams["team_id"].astype(str).tolist()
    table = pd.DataFrame({
        "team_id": team_ids,
        "team_name": [teams.set_index("team_id").loc[t, "team_name"] if t in teams.set_index("team_id").index else t for t in team_ids],
        "P": 0, "W": 0, "D": 0, "L": 0, "GF": 0, "GA": 0, "GD": 0, "Pts": 0,
    }).set_index("team_id")

    gms = matches_df[
        (matches_df["group"].astype(str).str.upper()==str(group_name).upper()) &
        (matches_df["status"].isin(["finished","walkover_home","walkover_away"]))
    ]

    def as_int(x): 
        try: return int(str(x) or 0)
        except: return 0

    for _, m in gms.iterrows():
        h = str(m.get("home_team_id","")); a = str(m.get("away_team_id",""))
        if h not in table.index or a not in table.index: continue
        hg, ag = as_int(m.get("home_goals",0)), as_int(m.get("away_goals",0))
        status = str(m.get("status",""))
        if status=="walkover_home": hg, ag = 3, 0
        elif status=="walkover_away": hg, ag = 0, 3
        table.at[h,"P"] += 1; table.at[a,"P"] += 1
        table.at[h,"GF"] += hg; table.at[h,"GA"] += ag
        table.at[a,"GF"] += ag; table.at[a,"GA"] += hg
        if hg>ag: table.at[h,"W"]+=1; table.at[h,"Pts"]+=3; table.at[a,"L"]+=1
        elif hg<ag: table.at[a,"W"]+=1; table.at[a,"Pts"]+=3; table.at[h,"L"]+=1
        else: table.at[h,"D"]+=1; table.at[a,"D"]+=1; table.at[h,"Pts"]+=1; table.at[a,"Pts"]+=1

    table["GD"] = table["GF"] - table["GA"]
    fp = compute_fairplay(events_df)
    table["FairPlay"] = [fp.get(t, 0) for t in table.index]

    base_sorted = table.sort_values(["Pts","GD","GF","FairPlay"], ascending=[False,False,False,True]).copy()
    base_sorted.reset_index(inplace=True)
    return base_sorted[["team_id","team_name","P","W","D","L","GF","GA","GD","Pts","FairPlay"]]

# ================= Admin forms (giữ nguyên logic cũ) =================
def admin_guard() -> bool:
    st.info("Chế độ quản trị — chỉ bạn được nhập dữ liệu. Người xem sẽ chỉ có quyền xem.")
    pwd = st.text_input("Nhập mật khẩu quản trị", type="password")
    if st.button("Đăng nhập quản trị"):
        if pwd == ADMIN_PASSWORD: st.session_state["is_admin"] = True
        else: st.error("Sai mật khẩu")
    return st.session_state.get("is_admin", False)

def admin_team_player_manager(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Quản lý đội & cầu thủ")
    teams = dfs.get("teams", pd.DataFrame()); players = dfs.get("players", pd.DataFrame())
    with st.expander("➕ Thêm đội bóng"):
        t_id = st.text_input("team_id (duy nhất)"); t_name = st.text_input("Tên đội")
        t_short = st.text_input("Tên ngắn"); t_group = st.selectbox("Bảng", ["A","B"])
        t_manager = st.text_input("HLV/Đội trưởng"); t_phone = st.text_input("SĐT")
        t_primary = st.text_input("Màu áo chính"); t_secondary = st.text_input("Màu áo phụ")
        t_logo = st.text_input("Logo URL"); t_notes = st.text_area("Ghi chú")
        if st.button("Lưu đội mới"):
            if t_id and t_name:
                if teams.empty or not (teams["team_id"].astype(str)==t_id).any():
                    new = pd.DataFrame([[t_id,t_name,t_short,t_group,t_manager,t_phone,t_primary,t_secondary,t_logo,t_notes]], columns=teams.columns)
                    dfs["teams"] = pd.concat([teams, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs); st.success("Đã thêm đội"); st.cache_data.clear()
                else: st.error("team_id đã tồn tại")
            else: st.error("Thiếu team_id hoặc tên đội")
    with st.expander("➕ Đăng ký cầu thủ (tối đa 30/đội)"):
        team_ids = dfs["teams"]["team_id"].astype(str).tolist() if "teams" in dfs and not dfs["teams"].empty else []
        p_team = st.selectbox("Đội", options=team_ids)
        team_count = players[players["team_id"].astype(str)==str(p_team)].shape[0] if not players.empty else 0
        st.caption(f"Hiện đội {p_team} có {team_count}/30 cầu thủ")
        if team_count >= 30: st.error("Đội đã đủ 30 cầu thủ — không thể thêm.")
        else:
            p_id = st.text_input("player_id (duy nhất)"); p_name = st.text_input("Tên cầu thủ")
            p_num = st.number_input("Số áo", min_value=0, max_value=99, step=1)
            p_pos = st.selectbox("Vị trí", ["GK","DF","MF","FW","Other"])
            p_dob = st.text_input("Ngày sinh (YYYY-MM-DD)"); p_nat = st.text_input("Quốc tịch", value="VN")
            p_reg = st.checkbox("Đã đăng ký", value=True)
            if st.button("Lưu cầu thủ"):
                if p_id and p_name:
                    if players.empty or not (players["player_id"].astype(str)==p_id).any():
                        new = pd.DataFrame([[p_id,p_team,p_name,p_num,p_pos,p_dob,p_nat,p_reg]], columns=players.columns)
                        dfs["players"] = pd.concat([players, new], ignore_index=True)
                        save_excel(DATA_FILE, dfs); st.success("Đã thêm cầu thủ"); st.cache_data.clear()
                    else: st.error("player_id đã tồn tại")
                else: st.error("Thiếu player_id hoặc tên cầu thủ")

def admin_schedule_results(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Lịch thi đấu & nhập kết quả")
    teams_map = get_ids_map(dfs.get("teams", pd.DataFrame()), "team_id", "team_name")
    matches = dfs.get("matches", pd.DataFrame())
    with st.expander("➕ Tạo trận đấu mới"):
        stage = st.selectbox("Giai đoạn", ["group","KO"]); group = st.selectbox("Bảng", ["A","B","-"])
        round_ = st.text_input("Vòng (VD: Vòng 1 / Tứ kết / BK / CK)")
        date = st.date_input("Ngày"); time = st.time_input("Giờ"); venue = st.text_input("Sân")
        home = st.selectbox("Chủ nhà", list(teams_map.keys()), format_func=lambda x: teams_map.get(x,x))
        away = st.selectbox("Đội khách", [t for t in teams_map.keys() if t != home], format_func=lambda x: teams_map.get(x,x))
        match_id = st.text_input("match_id (duy nhất)")
        if st.button("Lưu lịch"):
            if match_id and home and away:
                if matches.empty or not (matches["match_id"].astype(str)==match_id).any():
                    new = pd.DataFrame([[match_id,stage,group,round_,str(date),str(time),venue,home,away,"","","scheduled",""]], columns=matches.columns)
                    dfs["matches"] = pd.concat([matches, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs); st.success("Đã tạo trận đấu"); st.cache_data.clear()
                else: st.error("match_id đã tồn tại")
            else: st.error("Thiếu match_id hoặc đội thi đấu")
    with st.expander("✏️ Nhập kết quả / thẻ phạt / ghi bàn"):
        if matches.empty: st.info("Chưa có trận nào"); return
        sel = st.selectbox("Chọn trận", matches["match_id"].astype(str).tolist())
        m = matches[matches["match_id"].astype(str)==str(sel)].iloc[0]
        st.caption(f"{teams_map.get(str(m['home_team_id']), m['home_team_id'])} vs {teams_map.get(str(m['away_team_id']), m['away_team_id'])} — {m['date']} {m['time']}")
        def as_int(x): 
            try: return int(str(x) or 0)
            except: return 0
        hg = st.number_input("Bàn thắng đội chủ nhà", min_value=0, step=1, value=as_int(m.get("home_goals")))
        ag = st.number_input("Bàn thắng đội khách", min_value=0, step=1, value=as_int(m.get("away_goals")))
        status = st.selectbox("Trạng thái", ["finished","scheduled","walkover_home","walkover_away","cancelled"], index=0)
        note = st.text_area("Ghi chú", value=str(m.get("notes","")))
        if st.button("✅ Lưu kết quả trận"):
            idx = dfs["matches"][dfs["matches"]["match_id"].astype(str)==str(sel)].index[0]
            dfs["matches"].at[idx,"home_goals"]=int(hg); dfs["matches"].at[idx,"away_goals"]=int(ag)
            dfs["matches"].at[idx,"status"]=status; dfs["matches"].at[idx,"notes"]=note
            save_excel(DATA_FILE, dfs); st.success("Đã lưu kết quả"); st.cache_data.clear()
        st.markdown("**Sự kiện trận đấu**")
        events = dfs.get("events", pd.DataFrame())
        event_id = st.text_input("event_id (duy nhất)"); minute = st.number_input("Phút", 0, 120, 0)
        etype = st.selectbox("Loại sự kiện", ["goal","own_goal","yellow","red","second_yellow"])
        team_pick = st.selectbox("Đội", [str(m["home_team_id"]), str(m["away_team_id"])], format_func=lambda x: teams_map.get(x,x))
        team_players = dfs["players"][dfs["players"]["team_id"].astype(str)==str(team_pick)] if "players" in dfs else pd.DataFrame()
        pid_name = team_players.set_index("player_id")["player_name"].to_dict() if not team_players.empty else {}
        player_id = st.selectbox("Cầu thủ", list(pid_name.keys()), format_func=lambda pid: pid_name.get(pid,pid))
        assist_id = st.selectbox("Kiến tạo (nếu có)", [""]+list(pid_name.keys()), format_func=lambda pid: pid_name.get(pid,"") if pid else "")
        if st.button("➕ Lưu sự kiện"):
            if event_id and player_id:
                if events.empty or not (events["event_id"].astype(str)==event_id).any():
                    new = pd.DataFrame([[event_id, sel, minute, team_pick, player_id, etype, assist_id, ""]], columns=events.columns)
                    dfs["events"] = pd.concat([events, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs); st.success("Đã lưu sự kiện"); st.cache_data.clear()
                else: st.error("event_id đã tồn tại")
            else: st.error("Thiếu event_id hoặc player_id")

# ================= Public views =================
def view_standings(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Bảng xếp hạng")
    teams = dfs.get("teams", pd.DataFrame()); matches = dfs.get("matches", pd.DataFrame()); events = dfs.get("events", pd.DataFrame())
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Bảng A"); ta = compute_group_table("A", teams, matches, events)
        st.dataframe(ta, use_container_width=True)
    with c2:
        st.markdown("### Bảng B"); tb = compute_group_table("B", teams, matches, events)
        st.dataframe(tb, use_container_width=True)
    st.markdown("#### Đội vào vòng loại trực tiếp (Top 4 mỗi bảng)")
    qa = ta.head(4) if not ta.empty else pd.DataFrame(); qb = tb.head(4) if not tb.empty else pd.DataFrame()
    a,b = st.columns(2)
    with a: st.write("**Bảng A**"); 
    with a: st.table(qa[["team_name","Pts","GD","GF","FairPlay"]]) if not qa.empty else st.info("Chưa có")
    with b: st.write("**Bảng B**"); 
    with b: st.table(qb[["team_name","Pts","GD","GF","FairPlay"]]) if not qb.empty else st.info("Chưa có")

def view_fixtures_results(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Lịch thi đấu & Kết quả")
    matches = dfs.get("matches", pd.DataFrame())
    teams_map = get_ids_map(dfs.get("teams", pd.DataFrame()), "team_id", "team_name")
    if matches.empty: st.info("Chưa có lịch"); return
    matches = matches.sort_values(by=["date","time"]).reset_index(drop=True)
    for _, m in matches.iterrows():
        home = teams_map.get(str(m["home_team_id"]), m["home_team_id"])
        away = teams_map.get(str(m["away_team_id"]), m["away_team_id"])
        finished = str(m.get("status")) in ["finished","walkover_home","walkover_away"]
        score = f"{int(m.get('home_goals') or 0)} - {int(m.get('away_goals') or 0)}" if finished else "vs"
        st.markdown(f"**{home}** {score} **{away}**")
        st.caption(f"{m['date']} {m['time']} • {m['venue']} • {m['round']} • {m['stage'].upper()} • Trạng thái: {m['status']}")
        st.divider()

def view_top_scorers_and_fairplay(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Thống kê cá nhân & kỷ luật")
    events = dfs.get("events", pd.DataFrame()); players = dfs.get("players", pd.DataFrame())
    teams_map = get_ids_map(dfs.get("teams", pd.DataFrame()), "team_id", "team_name")
    if events.empty: st.info("Chưa có dữ liệu sự kiện"); return
    goals = events[events["event_type"].isin(["goal"])].groupby("player_id").size().reset_index(name="Goals")
    if not goals.empty:
        pnames = players.set_index("player_id")["player_name"].to_dict()
        ptm = players.set_index("player_id")["team_id"].to_dict()
        goals["Player"] = goals["player_id"].map(lambda x: pnames.get(x, x))
        goals["Team"] = goals["player_id"].map(lambda x: teams_map.get(ptm.get(x, ""), ptm.get(x, "")))
        st.markdown("### Top ghi bàn"); st.dataframe(goals[["Player","Team","Goals"]], use_container_width=True)
    fp_map = compute_fairplay(events)
    if fp_map:
        fp_df = pd.DataFrame({"team_id": list(fp_map.keys()), "FairPlay": list(fp_map.values())})
        fp_df["Team"] = fp_df["team_id"].map(lambda x: teams_map.get(x, x))
        st.markdown("### Điểm Fair-play (càng thấp càng tốt)")
        st.dataframe(fp_df[["Team","FairPlay"]].sort_values("FairPlay"), use_container_width=True)

# ================= Main =================
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    source = st.secrets.get("DATA_SOURCE", "excel").strip().lower()
    try:
        if source == "sheets":
            dfs = load_sheets(st.secrets["SHEET_NAME"])
            st.caption("Nguồn dữ liệu: **Google Sheets**")
        else:
            if not os.path.exists(DATA_FILE):
                st.error(f"Không tìm thấy file dữ liệu: {DATA_FILE}")
                st.stop()
            dfs = load_excel(DATA_FILE)
            st.caption("Nguồn dữ liệu: **Excel (local)**")
    except Exception as e:
        st.error(f"Lỗi tải dữ liệu: {e}")
        st.stop()

    settings = load_settings(dfs)
    tab_public, tab_admin = st.tabs(["Công khai", "Quản trị"])
    with tab_public:
        view_standings(dfs); st.divider()
        view_fixtures_results(dfs); st.divider()
        view_top_scorers_and_fairplay(dfs)
    with tab_admin:
        if ADMIN_PASSWORD and admin_guard():
            admin_team_player_manager(dfs); st.divider(); admin_schedule_results(dfs)
        else:
            st.info("Nhập mật khẩu để vào chế độ quản trị")

if __name__ == "__main__":
    main()
