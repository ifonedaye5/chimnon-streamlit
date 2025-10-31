# app.py — Chim Non League Manager (Streamlit + Excel/Google Sheets)
# ---------------------------------------------------------------
# Quick start (Excel backend first):
# 1) pip install streamlit pandas openpyxl python-dotenv
# 2) Put chimnon_template.xlsx next to this file (or set DATA_FILE in .env)
# 3) streamlit run app.py
#
# Optional: Google Sheets backend (later):
# - pip install gspread oauth2client
# - Set GOOGLE_SHEETS_JSON (service-account json) & SHEET_NAME in .env
#
# Admin mode: set ADMIN_PASSWORD in .env; viewers can only read.

import os
import io
from dataclasses import dataclass
from typing import List, Dict, Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ------------------ Config & Constants ------------------
load_dotenv()
APP_TITLE = "Giải Chim Non Lần 2 — League Manager"
DATA_FILE = os.getenv("DATA_FILE", "chimnon_template.xlsx")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "chimnon2025")  # change in .env

FAIRPLAY_POINTS = {
    "yellow": int(os.getenv("FAIRPLAY_YELLOW", 1)),
    "second_yellow": int(os.getenv("FAIRPLAY_SECOND_YELLOW", 3)),
    "red": int(os.getenv("FAIRPLAY_RED", 3)),
    "yellow_plus_direct_red": int(os.getenv("FAIRPLAY_YELLOW_PLUS_DIRECT_RED", 4)),
}

# ------------------ Utilities ------------------
@st.cache_data(show_spinner=False)
def load_excel(path: str) -> Dict[str, pd.DataFrame]:
    if not os.path.exists(path):
        st.error(f"Không tìm thấy file dữ liệu: {path}")
        st.stop()
    xls = pd.ExcelFile(path)
    dfs = {name: xls.parse(name) for name in xls.sheet_names}
    # Normalize dtypes
    for k, v in dfs.items():
        dfs[k] = v.fillna("")
    return dfs

@st.cache_data(show_spinner=False)
def load_settings(dfs: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    if "settings" not in dfs:
        return {}
    row = dfs["settings"].iloc[0].to_dict() if len(dfs["settings"]) else {}
    return {str(k): str(v) for k, v in row.items()}

@st.cache_data(show_spinner=False)
def get_ids_map(df: pd.DataFrame, id_col: str, label_col: str) -> Dict[str, str]:
    m = {}
    if df.empty:
        return m
    for _, r in df.iterrows():
        m[str(r.get(id_col, ""))] = str(r.get(label_col, ""))
    return m

# Persist saves (Excel overwrite)
def save_excel(path: str, dfs: Dict[str, pd.DataFrame]):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, index=False, sheet_name=name)

# ------------------ Fair-play computation ------------------
def compute_fairplay(events: pd.DataFrame) -> Dict[str, int]:
    # Return total fairplay points per team_id (lower is better)
    points = {}
    if events.empty:
        return points
    for _, e in events.iterrows():
        team = str(e.get("team_id", ""))
        etype = str(e.get("event_type", "")).lower()
        if not team:
            continue
        add = 0
        if etype == "yellow":
            add = FAIRPLAY_POINTS["yellow"]
        elif etype == "second_yellow":
            add = FAIRPLAY_POINTS["second_yellow"]
        elif etype == "red":
            add = FAIRPLAY_POINTS["red"]
        elif etype == "yellow_plus_direct_red":
            add = FAIRPLAY_POINTS["yellow_plus_direct_red"]
        points[team] = points.get(team, 0) + add
    return points

# ------------------ Standings computation ------------------
def compute_group_table(group_name: str, teams_df: pd.DataFrame, matches_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    teams = teams_df[teams_df["group"].astype(str).str.upper() == str(group_name).upper()].copy()
    team_ids = teams["team_id"].astype(str).tolist()

    # Prepare base table
    table = pd.DataFrame({
        "team_id": team_ids,
        "team_name": [teams.set_index("team_id").loc[t, "team_name"] if t in teams.set_index("team_id").index else t for t in team_ids],
        "P": 0, "W": 0, "D": 0, "L": 0,
        "GF": 0, "GA": 0, "GD": 0, "Pts": 0,
    })
    table.set_index("team_id", inplace=True)

    # Consider only finished group matches in this group
    gms = matches_df[(matches_df["group"].astype(str).str.upper()==str(group_name).upper()) & (matches_df["status"].isin(["finished", "walkover_home", "walkover_away"]))]

    for _, m in gms.iterrows():
        h = str(m.get("home_team_id", ""))
        a = str(m.get("away_team_id", ""))
        if h not in table.index or a not in table.index:  # ignore if unknown
            continue
        hg = int(str(m.get("home_goals", 0)) or 0)
        ag = int(str(m.get("away_goals", 0)) or 0)

        # Walkover handling (0-3 or 3-0)
        status = str(m.get("status", ""))
        if status == "walkover_home":
            hg, ag = 3, 0
        elif status == "walkover_away":
            hg, ag = 0, 3

        # Update P, GF, GA
        table.at[h, "P"] += 1
        table.at[a, "P"] += 1
        table.at[h, "GF"] += hg
        table.at[h, "GA"] += ag
        table.at[a, "GF"] += ag
        table.at[a, "GA"] += hg

        if hg > ag:
            table.at[h, "W"] += 1; table.at[a, "L"] += 1
            table.at[h, "Pts"] += 3
        elif hg < ag:
            table.at[a, "W"] += 1; table.at[h, "L"] += 1
            table.at[a, "Pts"] += 3
        else:
            table.at[h, "D"] += 1; table.at[a, "D"] += 1
            table.at[h, "Pts"] += 1; table.at[a, "Pts"] += 1

    # Compute GD
    table["GD"] = table["GF"] - table["GA"]

    # Fair-play
    fp = compute_fairplay(events_df)
    table["FairPlay"] = [fp.get(t, 0) for t in table.index]

    # Head-to-head tie-breaker: for ties of 2+ teams, compute mini-league
    def sort_with_tiebreak(df_block: pd.DataFrame) -> pd.DataFrame:
        if len(df_block) <= 1:
            return df_block
        tied_ids = df_block.index.tolist()
        # Mini-league matches between tied teams
        mini = matches_df[
            (matches_df["group"].astype(str).str.upper()==str(group_name).upper()) &
            (matches_df["status"].isin(["finished", "walkover_home", "walkover_away"])) &
            (matches_df["home_team_id"].astype(str).isin(tied_ids)) &
            (matches_df["away_team_id"].astype(str).isin(tied_ids))
        ]
        # Build temporary mini table
        mt = pd.DataFrame({"team_id": tied_ids, "Pts": 0, "GD": 0, "GF": 0})
        mt.set_index("team_id", inplace=True)
        for _, m in mini.iterrows():
            h = str(m.get("home_team_id", "")); a = str(m.get("away_team_id", ""))
            hg = int(str(m.get("home_goals", 0)) or 0); ag = int(str(m.get("away_goals", 0)) or 0)
            status = str(m.get("status", ""))
            if status == "walkover_home":
                hg, ag = 3, 0
            elif status == "walkover_away":
                hg, ag = 0, 3
            # Update
            mt.at[h, "GF"] += hg; mt.at[h, "GD"] += (hg-ag)
            mt.at[a, "GF"] += ag; mt.at[a, "GD"] += (ag-hg)
            if hg>ag: mt.at[h, "Pts"] += 3
            elif hg<ag: mt.at[a, "Pts"] += 3
            else: mt.at[h, "Pts"] += 1; mt.at[a, "Pts"] += 1
        # Now order following: head-to-head Pts, head-to-head GD, head-to-head GF, then overall GD, overall GF, then FairPlay
        ordered_ids = mt.sort_values(["Pts","GD","GF"], ascending=[False, False, False]).index.tolist()
        # Apply next tie-breakers if still exact; we’ll extend using overall metrics and FairPlay
        # Convert to dataframe in that order
        sorted_block = df_block.loc[ordered_ids]
        # If still ties remain (rare), add overall fallback
        sorted_block = sorted_block.sort_values(
            by=["Pts","GD","GF","FairPlay"], ascending=[False, False, False, True]
        )
        return sorted_block

    # First sort by Pts/GD/GF/FairPlay to identify tie groups, then re-sort ties by H2H
    base_sorted = table.sort_values(by=["Pts","GD","GF","FairPlay"], ascending=[False, False, False, True]).copy()

    # Walk through tied groups of equal Pts
    final_rows = []
    i = 0
    while i < len(base_sorted):
        pts = base_sorted.iloc[i]["Pts"]
        # find block with same points
        j = i + 1
        while j < len(base_sorted) and base_sorted.iloc[j]["Pts"] == pts:
            j += 1
        block = base_sorted.iloc[i:j]
        if len(block) > 1:
            block = sort_with_tiebreak(block)
        final_rows.append(block)
        i = j

    final = pd.concat(final_rows)
    final.reset_index(inplace=True)
    return final[["team_id","team_name","P","W","D","L","GF","GA","GD","Pts","FairPlay"]]

# ------------------ Admin Forms ------------------
def admin_guard() -> bool:
    st.info("Chế độ quản trị — chỉ bạn được nhập dữ liệu. Người xem sẽ chỉ có quyền xem.")
    pwd = st.text_input("Nhập mật khẩu quản trị", type="password")
    if st.button("Đăng nhập quản trị"):
        if pwd == ADMIN_PASSWORD:
            st.session_state["is_admin"] = True
        else:
            st.error("Sai mật khẩu")
    return st.session_state.get("is_admin", False)


def admin_team_player_manager(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Quản lý đội & cầu thủ")
    teams = dfs.get("teams", pd.DataFrame())
    players = dfs.get("players", pd.DataFrame())

    # Add team
    with st.expander("➕ Thêm đội bóng"):
        t_id = st.text_input("team_id (duy nhất)")
        t_name = st.text_input("Tên đội")
        t_short = st.text_input("Tên ngắn (3-6 ký tự)")
        t_group = st.selectbox("Bảng", options=["A","B"])  # fixed two groups
        t_manager = st.text_input("HLV/Đội trưởng")
        t_phone = st.text_input("SĐT")
        t_primary = st.text_input("Màu áo chính")
        t_secondary = st.text_input("Màu áo phụ")
        t_logo = st.text_input("Logo URL")
        t_notes = st.text_area("Ghi chú")
        if st.button("Lưu đội mới"):
            if t_id and t_name:
                if not (teams["team_id"].astype(str) == t_id).any():
                    new = pd.DataFrame([[t_id,t_name,t_short,t_group,t_manager,t_phone,t_primary,t_secondary,t_logo,t_notes]], columns=teams.columns)
                    dfs["teams"] = pd.concat([teams, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs)
                    st.success("Đã thêm đội")
                    st.cache_data.clear()
                else:
                    st.error("team_id đã tồn tại")
            else:
                st.error("Thiếu team_id hoặc tên đội")

    # Add player
    with st.expander("➕ Đăng ký cầu thủ (tối đa 30/đội)"):
        team_ids = dfs["teams"]["team_id"].astype(str).tolist() if not dfs.get("teams", pd.DataFrame()).empty else []
        p_team = st.selectbox("Đội", options=team_ids)
        # enforce max 30
        team_count = players[players["team_id"].astype(str)==str(p_team)].shape[0]
        st.caption(f"Hiện đội {p_team} có {team_count}/30 cầu thủ")
        if team_count >= 30:
            st.error("Đội đã đủ 30 cầu thủ — không thể thêm.")
        else:
            p_id = st.text_input("player_id (duy nhất)")
            p_name = st.text_input("Tên cầu thủ")
            p_num = st.number_input("Số áo", min_value=0, max_value=99, step=1)
            p_pos = st.selectbox("Vị trí", ["GK","DF","MF","FW","Other"])  # simple
            p_dob = st.text_input("Ngày sinh (YYYY-MM-DD)")
            p_nat = st.text_input("Quốc tịch", value="VN")
            p_reg = st.checkbox("Đã đăng ký", value=True)
            if st.button("Lưu cầu thủ"):
                if p_id and p_name:
                    if not (players["player_id"].astype(str)==p_id).any():
                        new = pd.DataFrame([[p_id,p_team,p_name,p_num,p_pos,p_dob,p_nat, p_reg]], columns=players.columns)
                        dfs["players"] = pd.concat([players, new], ignore_index=True)
                        save_excel(DATA_FILE, dfs)
                        st.success("Đã thêm cầu thủ")
                        st.cache_data.clear()
                    else:
                        st.error("player_id đã tồn tại")
                else:
                    st.error("Thiếu player_id hoặc tên cầu thủ")


def admin_schedule_results(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Lịch thi đấu & nhập kết quả")
    teams_map = get_ids_map(dfs["teams"], "team_id", "team_name") if not dfs.get("teams", pd.DataFrame()).empty else {}
    matches = dfs.get("matches", pd.DataFrame())

    with st.expander("➕ Tạo trận đấu mới"):
        stage = st.selectbox("Giai đoạn", ["group","KO"])  # group stage or knockout
        group = st.selectbox("Bảng", ["A","B","-"])
        round_ = st.text_input("Vòng (VD: Vòng 1 / Tứ kết / BK / CK)")
        date = st.date_input("Ngày")
        time = st.time_input("Giờ")
        venue = st.text_input("Sân")
        home = st.selectbox("Chủ nhà", list(teams_map.keys()), format_func=lambda x: teams_map.get(x, x))
        away = st.selectbox("Đội khách", [t for t in teams_map.keys() if t != home], format_func=lambda x: teams_map.get(x, x))
        match_id = st.text_input("match_id (duy nhất)")
        if st.button("Lưu lịch"):
            if match_id and home and away:
                if not (matches["match_id"].astype(str)==match_id).any():
                    new = pd.DataFrame([[match_id,stage,group,round_,str(date),str(time),venue,home,away,"","","scheduled",""]], columns=matches.columns)
                    dfs["matches"] = pd.concat([matches, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs)
                    st.success("Đã tạo trận đấu")
                    st.cache_data.clear()
                else:
                    st.error("match_id đã tồn tại")
            else:
                st.error("Thiếu match_id hoặc đội thi đấu")

    with st.expander("✏️ Nhập kết quả / thẻ phạt / ghi bàn"):
        if matches.empty:
            st.info("Chưa có trận nào")
            return
        sel = st.selectbox("Chọn trận", matches["match_id"].astype(str).tolist())
        m = matches[matches["match_id"].astype(str)==str(sel)].iloc[0]
        st.caption(f"{teams_map.get(str(m['home_team_id']), m['home_team_id'])} vs {teams_map.get(str(m['away_team_id']), m['away_team_id'])} — {m['date']} {m['time']}")
        hg = st.number_input("Bàn thắng đội chủ nhà", min_value=0, step=1, value=int(str(m.get("home_goals")) or 0))
        ag = st.number_input("Bàn thắng đội khách", min_value=0, step=1, value=int(str(m.get("away_goals")) or 0))
        status = st.selectbox("Trạng thái", ["finished","scheduled","walkover_home","walkover_away","cancelled"], index=0)
        note = st.text_area("Ghi chú", value=str(m.get("notes", "")))
        if st.button("✅ Lưu kết quả trận"):
            idx = dfs["matches"][dfs["matches"]["match_id"].astype(str)==str(sel)].index[0]
            dfs["matches"].at[idx, "home_goals"] = int(hg)
            dfs["matches"].at[idx, "away_goals"] = int(ag)
            dfs["matches"].at[idx, "status"] = status
            dfs["matches"].at[idx, "notes"] = note
            save_excel(DATA_FILE, dfs)
            st.success("Đã lưu kết quả")
            st.cache_data.clear()

        # Events (goals/cards)
        st.markdown("**Sự kiện trận đấu**")
        events = dfs.get("events", pd.DataFrame())
        event_id = st.text_input("event_id (duy nhất)")
        minute = st.number_input("Phút", min_value=0, max_value=120, step=1)
        etype = st.selectbox("Loại sự kiện", ["goal","own_goal","yellow","red","second_yellow"]) 
        team_pick = st.selectbox("Đội", [str(m["home_team_id"]), str(m["away_team_id"])], format_func=lambda x: teams_map.get(x, x))
        # player choices are filtered by team
        team_players = dfs["players"][dfs["players"]["team_id"].astype(str)==str(team_pick)] if not dfs.get("players", pd.DataFrame()).empty else pd.DataFrame()
        player_id = st.selectbox("Cầu thủ", team_players["player_id"].astype(str).tolist(), format_func=lambda pid: team_players.set_index("player_id").get("player_name", pd.Series()).to_dict().get(pid, pid))
        assist_id = st.selectbox("Kiến tạo (nếu có)", [""] + team_players["player_id"].astype(str).tolist(), format_func=lambda pid: (team_players.set_index("player_id").get("player_name", pd.Series()).to_dict().get(pid, "") if pid else ""))
        if st.button("➕ Lưu sự kiện"):
            if event_id and player_id:
                if not (events["event_id"].astype(str)==event_id).any():
                    new = pd.DataFrame([[event_id, sel, minute, team_pick, player_id, etype, assist_id, ""]], columns=events.columns)
                    dfs["events"] = pd.concat([events, new], ignore_index=True)
                    save_excel(DATA_FILE, dfs)
                    st.success("Đã lưu sự kiện")
                    st.cache_data.clear()
                else:
                    st.error("event_id đã tồn tại")
            else:
                st.error("Thiếu event_id hoặc player_id")

# ------------------ Public Views ------------------
def view_standings(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Bảng xếp hạng")
    teams = dfs.get("teams", pd.DataFrame())
    matches = dfs.get("matches", pd.DataFrame())
    events = dfs.get("events", pd.DataFrame())

    cols = st.columns(2)
    with cols[0]:
        st.markdown("### Bảng A")
        ta = compute_group_table("A", teams, matches, events)
        st.dataframe(ta, use_container_width=True)
    with cols[1]:
        st.markdown("### Bảng B")
        tb = compute_group_table("B", teams, matches, events)
        st.dataframe(tb, use_container_width=True)

    # Qualified per group (top 4)
    st.markdown("#### Đội vào vòng loại trực tiếp (Top 4 mỗi bảng)")
    qa = ta.head(4) if 'team_id' in ta.columns else pd.DataFrame()
    qb = tb.head(4) if 'team_id' in tb.columns else pd.DataFrame()
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Bảng A**")
        st.table(qa[["team_name","Pts","GD","GF","FairPlay"]])
    with c2:
        st.write("**Bảng B**")
        st.table(qb[["team_name","Pts","GD","GF","FairPlay"]])


def view_fixtures_results(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Lịch thi đấu & Kết quả")
    matches = dfs.get("matches", pd.DataFrame())
    teams_map = get_ids_map(dfs["teams"], "team_id", "team_name") if not dfs.get("teams", pd.DataFrame()).empty else {}
    if matches.empty:
        st.info("Chưa có lịch")
        return
    # sort by date/time
    def sort_key(row):
        return (str(row.get("date","")), str(row.get("time","")))
    matches = matches.sort_values(by=["date","time"]).reset_index(drop=True)

    for _, m in matches.iterrows():
        home = teams_map.get(str(m["home_team_id"]), m["home_team_id"])
        away = teams_map.get(str(m["away_team_id"]), m["away_team_id"])
        score = f"{int(m['home_goals'] or 0)} - {int(m['away_goals'] or 0)}" if str(m.get("status")) in ["finished","walkover_home","walkover_away"] else "vs"
        st.markdown(f"**{home}** {score} **{away}**  ")
        st.caption(f"{m['date']} {m['time']} • {m['venue']} • {m['round']} • {m['stage'].upper()} • Trạng thái: {m['status']}")
        st.divider()


def view_top_scorers_and_fairplay(dfs: Dict[str, pd.DataFrame]):
    st.subheader("Thống kê cá nhân & kỷ luật")
    events = dfs.get("events", pd.DataFrame())
    players = dfs.get("players", pd.DataFrame())
    teams_map = get_ids_map(dfs["teams"], "team_id", "team_name") if not dfs.get("teams", pd.DataFrame()).empty else {}

    if events.empty:
        st.info("Chưa có dữ liệu sự kiện")
        return

    # Goals (goal + own_goal counts for the scoring team differently — here we count only 'goal' for scorer)
    goals = events[events["event_type"].isin(["goal"])].groupby("player_id").size().reset_index(name="Goals")
    if not goals.empty:
        goals = goals.sort_values("Goals", ascending=False)
        # Merge player names
        pnames = players.set_index("player_id")["player_name"].to_dict()
        ptm = players.set_index("player_id")["team_id"].to_dict()
        goals["Player"] = goals["player_id"].map(lambda x: pnames.get(x, x))
        goals["Team"] = goals["player_id"].map(lambda x: teams_map.get(ptm.get(x, ""), ptm.get(x, "")))
        st.markdown("### Top ghi bàn")
        st.dataframe(goals[["Player","Team","Goals"]], use_container_width=True)

    # Fair-play per team
    fp_map = compute_fairplay(events)
    if fp_map:
        fp_df = pd.DataFrame({"team_id": list(fp_map.keys()), "FairPlay": list(fp_map.values())})
        fp_df["Team"] = fp_df["team_id"].map(lambda x: teams_map.get(x, x))
        st.markdown("### Điểm Fair-play (càng thấp càng tốt)")
        st.dataframe(fp_df[["Team","FairPlay"]].sort_values("FairPlay"), use_container_width=True)


# ------------------ Main App ------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Chỉ admin được nhập dữ liệu • Người xem chỉ có quyền xem • Backend = Excel/Google Sheets")

    dfs = load_excel(DATA_FILE)
    settings = load_settings(dfs)

    tab_public, tab_admin = st.tabs(["Công khai", "Quản trị"])

    with tab_public:
        view_standings(dfs)
        st.divider()
        view_fixtures_results(dfs)
        st.divider()
        view_top_scorers_and_fairplay(dfs)

    with tab_admin:
        if admin_guard():
            admin_team_player_manager(dfs)
            st.divider()
            admin_schedule_results(dfs)
        else:
            st.info("Nhập mật khẩu để vào chế độ quản trị")


if __name__ == "__main__":
    main()
