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
def load_sheets(sheet_name: str = "", sheet_key: str = "") -> Dict[str, pd.DataFrame]:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    from gspread.exceptions import APIError, SpreadsheetNotFound

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    sa_info = dict(st.secrets["gspread_service_account"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)

    try:
        files = client.list_spreadsheet_files()
        if files:
            dbg = pd.DataFrame(files)
            st.caption("🔎 SA nhìn thấy các file (name / id):")
            st.dataframe(dbg[["name", "id"]].head(50), use_container_width=True)
    except Exception as e:
        st.caption(f"⚠️ Không liệt kê được file: {e}")

    try:
        if sheet_key:
            sh = client.open_by_key(sheet_key)
            st.caption(f"✅ Mở bằng KEY: {sheet_key}")
        else:
            sh = client.open(sheet_name)
            st.caption(f"✅ Mở bằng NAME: {sheet_name}")
    except SpreadsheetNotFound:
        st.error("❌ Không tìm thấy file. Kiểm tra lại SHEET_KEY/SHEET_NAME và quyền Share cho Service Account.")
        raise
    except APIError as e:
        try:
            st.error(f"❌ Google APIError: {e.response.status_code} {e.response.reason} — {e.response.text}")
        except Exception:
            st.error(f"❌ Google APIError: {e}")
        raise
    except Exception as e:
        st.error(f"❌ Lỗi mở spreadsheet: {e}")
        raise

    dfs: Dict[str, pd.DataFrame] = {}
    titles = [ws.title for ws in sh.worksheets()]
    for t in titles:
        ws = sh.worksheet(t)
        data = ws.get_all_records()
        dfs[t] = pd.DataFrame(data).fillna("")
    st.caption("Nguồn dữ liệu: **Google Sheets** • Worksheets: " + (", ".join(titles) if titles else "(trống)"))
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

# ================= NEW compute_group_table =================
def compute_group_table(group_name: str, teams_df: pd.DataFrame, matches_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính BXH chuẩn theo điều lệ Thành Dũng 2025:
    Ưu tiên: Đối đầu -> Hiệu số -> Bàn thắng -> FairPlay
    """
    if teams_df.empty or matches_df.empty:
        return pd.DataFrame()

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

    # ---- Hàm phụ: đối đầu trực tiếp ----
    def head_to_head(t1, t2):
        subset = gms[
            ((gms["home_team_id"].astype(str)==t1) & (gms["away_team_id"].astype(str)==t2)) |
            ((gms["home_team_id"].astype(str)==t2) & (gms["away_team_id"].astype(str)==t1))
        ]
        if subset.empty:
            return 0
        pts1 = pts2 = gd1 = gd2 = gf1 = gf2 = 0
        for _, r in subset.iterrows():
            h, a = str(r["home_team_id"]), str(r["away_team_id"])
            hg, ag = as_int(r["home_goals"]), as_int(r["away_goals"])
            if h == t1:
                gf1 += hg; gf2 += ag; gd1 += hg - ag; gd2 += ag - hg
                if hg > ag: pts1 += 3
                elif hg < ag: pts2 += 3
                else: pts1 += 1; pts2 += 1
            elif a == t1:
                gf1 += ag; gf2 += hg; gd1 += ag - hg; gd2 += hg - ag
                if ag > hg: pts1 += 3
                elif ag < hg: pts2 += 3
                else: pts1 += 1; pts2 += 1
        if pts1 != pts2: return 1 if pts1 > pts2 else -1
        if gd1 != gd2: return 1 if gd1 > gd2 else -1
        if gf1 != gf2: return 1 if gf1 > gf2 else -1
        return 0

    # ---- So sánh với ưu tiên theo điều lệ ----
    from functools import cmp_to_key
    def compare(a, b):
        hh = head_to_head(a, b)
        if hh != 0: return -hh  # đối đầu thắng xếp trên
        gd_diff = table.at[b,"GD"] - table.at[a,"GD"]
        if gd_diff != 0: return gd_diff
        gf_diff = table.at[b,"GF"] - table.at[a,"GF"]
        if gf_diff != 0: return gf_diff
        fp_diff = table.at[a,"FairPlay"] - table.at[b,"FairPlay"]
        return fp_diff

    order = sorted(table.index, key=cmp_to_key(compare))
    table = table.loc[order]
    table.insert(0, "Hạng", range(1, len(table)+1))
    return table.reset_index()
