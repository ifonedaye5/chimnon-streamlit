# app.py
import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

st.set_page_config(page_title="⚽ Giải Chim Non Lần 2 — Cup Manager 🏆", layout="wide")

# === BACKGROUND: đặt <img> cố định sau toàn bộ app (cực chắc) ===
BG_URL = "https://drive.google.com/uc?id=1H_06y2X9Vdleg6-VqsWebWF353Gfe21U"

st.markdown(f"""
<style>
/* Cho mọi lớp chính trong suốt để thấy ảnh phía sau */
html, body, .stApp, [data-testid="stAppViewContainer"] {{
  background: transparent !important;
}}
/* Ảnh nền cố định phủ full màn, nằm dưới mọi nội dung */
#app-global-bg-img {{
  position: fixed;
  inset: 0;                  /* top/right/bottom/left: 0 */
  width: 100vw;
  height: 100vh;
  object-fit: cover;         /* phủ kín, không méo */
  z-index: -1;               /* đẩy xuống dưới nội dung */
  opacity: 0.18;             /* chỉnh độ mờ 0.12–0.25 */
  filter: saturate(110%) contrast(105%);
}}
/* Header mờ nhẹ để dễ đọc khi cuộn */
[data-testid="stHeader"] {{
  background: rgba(255,255,255,0.82) !important;
  backdrop-filter: blur(4px);
  border-bottom: 1px solid rgba(0,0,0,0.05);
}}
</style>
<img id="app-global-bg-img" src="{BG_URL}" />
""", unsafe_allow_html=True)





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

def compute_standings(
    teams_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    events_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Tính BXH theo điều lệ:
      1) Đối đầu trực tiếp (Head-to-Head)
      2) Hiệu số bàn thắng (HS / GD)
      3) Bàn thắng ghi được (BT / GF)
      4) Fair-Play (ít hơn xếp trên)

    Chỉ tính KHI trận đã kết thúc (status Finished/Kết thúc) và có đủ tỉ số.
    Trả về các cột (tiếng Việt) giống bản trước: 
      Team ID | Đội | Trận | Thắng | Hòa | Thua | BT | BB | HS | Điểm | FairPlay
    """
    # Bảo vệ dữ liệu đầu vào
    if teams_df is None or teams_df.empty or matches_df is None or matches_df.empty:
        return pd.DataFrame()

    # Chuẩn hóa tên cột
    tdf = teams_df.copy()
    tdf.columns = [c.strip().lower() for c in tdf.columns]

    mdf = matches_df.copy()
    mdf.columns = [c.strip().lower() for c in mdf.columns]

    # Kiểm tra cột bắt buộc
    need_cols = {"home_team_id", "away_team_id", "home_goals", "away_goals"}
    if not need_cols.issubset(set(mdf.columns)):
        return pd.DataFrame()

    # Ép kiểu số nhưng KHÔNG fill 0 để tránh coi trận chưa đá như 0-0
    mdf["home_goals"] = pd.to_numeric(mdf["home_goals"], errors="coerce")
    mdf["away_goals"] = pd.to_numeric(mdf["away_goals"], errors="coerce")

    # Chuẩn hóa trạng thái và lọc chỉ lấy trận đã kết thúc + có tỉ số
    status = mdf.get("status")
    if status is not None:
        status = status.astype(str).str.strip().str.lower()
        FINISHED = {"finished", "kết thúc", "ket thuc", "done", "ft"}
        played_mask = (
            status.isin(FINISHED)
            & mdf["home_goals"].notna()
            & mdf["away_goals"].notna()
        )
    else:
        # Nếu không có cột status thì chỉ tính trận có đủ tỉ số
        played_mask = mdf["home_goals"].notna() & mdf["away_goals"].notna()

    m_played = mdf.loc[played_mask].copy()

    # Sổ thống kê
    points: dict[str, int] = {}
    stats: dict[str, dict] = {}

    def ensure(team_id: str):
        if team_id not in points:
            points[team_id] = 0
        if team_id not in stats:
            stats[team_id] = {"P": 0, "W": 0, "D": 0, "L": 0, "GF": 0, "GA": 0, "GD": 0}

    # Ghi nhận kết quả CHỈ từ m_played
    for _, r in m_played.iterrows():
        h = str(r["home_team_id"]).strip()
        a = str(r["away_team_id"]).strip()
        hg = int(r["home_goals"])
        ag = int(r["away_goals"])
        ensure(h)
        ensure(a)

        # Trận đã đá
        stats[h]["P"] += 1
        stats[a]["P"] += 1

        # Bàn thắng / thua
        stats[h]["GF"] += hg
        stats[h]["GA"] += ag
        stats[a]["GF"] += ag
        stats[a]["GA"] += hg
        stats[h]["GD"] = stats[h]["GF"] - stats[h]["GA"]
        stats[a]["GD"] = stats[a]["GF"] - stats[a]["GA"]

        # Điểm
        if hg > ag:
            points[h] += 3
            stats[h]["W"] += 1
            stats[a]["L"] += 1
        elif hg < ag:
            points[a] += 3
            stats[a]["W"] += 1
            stats[h]["L"] += 1
        else:
            points[h] += 1
            points[a] += 1
            stats[h]["D"] += 1
            stats[a]["D"] += 1

    # Fair-Play
    fair = compute_fairplay(events_df)

    # Xác định cột tên đội để hiển thị
    name_col = (
        "team_name"
        if "team_name" in tdf.columns
        else ("short_name" if "short_name" in tdf.columns else "team_id")
    )

    # Lập bảng kết quả cho TẤT CẢ các đội (kể cả đội chưa đá)
    rows = []
    for _, tr in tdf.iterrows():
        tid = str(tr.get("team_id", "")).strip()
        if not tid:
            continue
        s = stats.get(tid, {"P": 0, "W": 0, "D": 0, "L": 0, "GF": 0, "GA": 0, "GD": 0})
        rows.append(
            {
                "Team ID": tid,
                "Đội": tr.get(name_col, tid),
                "Trận": s["P"],
                "Thắng": s["W"],
                "Hòa": s["D"],
                "Thua": s["L"],
                "BT": s["GF"],
                "BB": s["GA"],
                "HS": s["GD"],
                "Điểm": points.get(tid, 0),
                "FairPlay": fair.get(tid, 0),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # ===== Sắp xếp theo ưu tiên: H2H -> HS -> BT -> Fair-Play =====
    # Chuẩn bị dữ liệu đối đầu: chỉ dùng các trận "đã chơi"
    m_h2h = m_played[["home_team_id", "away_team_id", "home_goals", "away_goals"]].copy()
    m_h2h["home_team_id"] = m_h2h["home_team_id"].astype(str).str.strip()
    m_h2h["away_team_id"] = m_h2h["away_team_id"].astype(str).str.strip()

    from functools import cmp_to_key

    def head_to_head(t1: str, t2: str) -> int:
        """
        So sánh t1 với t2:
        trả về  1 nếu t1 xếp TRÊN t2,
                -1 nếu t1 xếp DƯỚI t2,
                 0 nếu bằng nhau theo H2H.
        """
        sub = m_h2h[
            ((m_h2h["home_team_id"] == t1) & (m_h2h["away_team_id"] == t2))
            | ((m_h2h["home_team_id"] == t2) & (m_h2h["away_team_id"] == t1))
        ]
        if sub.empty:
            return 0

        pts1 = pts2 = 0
        gd1 = gd2 = 0
        gf1 = gf2 = 0

        for _, m in sub.iterrows():
            h, a = m["home_team_id"], m["away_team_id"]
            hg, ag = int(m["home_goals"]), int(m["away_goals"])
            if h == t1:
                gf1 += hg
                gf2 += ag
                gd1 += (hg - ag)
                gd2 += (ag - hg)
                if hg > ag:
                    pts1 += 3
                elif hg < ag:
                    pts2 += 3
                else:
                    pts1 += 1
                    pts2 += 1
            else:  # a == t1
                gf1 += ag
                gf2 += hg
                gd1 += (ag - hg)
                gd2 += (hg - ag)
                if ag > hg:
                    pts1 += 3
                elif ag < hg:
                    pts2 += 3
                else:
                    pts1 += 1
                    pts2 += 1

        if pts1 != pts2:
            return 1 if pts1 > pts2 else -1
        if gd1 != gd2:
            return 1 if gd1 > gd2 else -1
        if gf1 != gf2:
            return 1 if gf1 > gf2 else -1
        return 0

    by_id = df.set_index("Team ID")

    def cmp(a: str, b: str) -> int:
        # 1) H2H
        hh = head_to_head(a, b)
        if hh != 0:
            # head_to_head trả 1 -> a > b (a xếp TRÊN), nhưng sort tăng nên đảo dấu
            return -hh

        # 2) HS (lớn hơn tốt hơn)
        gd_a, gd_b = by_id.at[a, "HS"], by_id.at[b, "HS"]
        if gd_a != gd_b:
            return -1 if gd_a > gd_b else 1

        # 3) BT (lớn hơn tốt hơn)
        gf_a, gf_b = by_id.at[a, "BT"], by_id.at[b, "BT"]
        if gf_a != gf_b:
            return -1 if gf_a > gf_b else 1

        # 4) Fair-Play (ít hơn tốt hơn)
        fp_a, fp_b = by_id.at[a, "FairPlay"], by_id.at[b, "FairPlay"]
        if fp_a != fp_b:
            return -1 if fp_a < fp_b else 1

        # 5) Cuối cùng: Team ID để ổn định
        return -1 if a < b else (1 if a > b else 0)

    order = sorted(df["Team ID"].tolist(), key=cmp_to_key(cmp))
    df = df.set_index("Team ID").loc[order].reset_index()

    # Thêm cột "Hạng" (1..n)
    df.insert(0, "Hạng", range(1, len(df) + 1))

    return df


# ========== 4) UI ==========
st.title("⚽ Giải Chim Non Lần 2 — Cup Manager 🏆")

# with st.expander("🔐 Kết nối & Debug", expanded=True):
    # if DATA_SOURCE.lower() != "sheets":
        # st.error('DATA_SOURCE không phải "sheets". Kiểm tra Secrets.')
    # else:
        # files = list_sa_spreadsheets()
        # st.write("🔎 **SA nhìn thấy các file (tên / id)**")
        # if files:
            # try:
                # st.dataframe(pd.DataFrame(files)[["name","id"]], use_container_width=True, height=180)
            # except Exception:
                # st.dataframe(pd.DataFrame(files), use_container_width=True, height=180)
        # else:
            # st.info("Service Account chưa thấy file nào. Hãy SHARE file Google Sheet cho email SA với quyền Editor.")

        # if not SHEET_KEY:
            # st.error("Chưa có SHEET_KEY trong Secrets (đặt ở cấp gốc, không nằm trong [gspread_service_account]).")
            # st.stop()
        # Thử mở bằng KEY chỉ để xác nhận; không dùng đối tượng sh cho cache
        # try:
            # _client = get_gspread_client()
            # _client.open_by_key(SHEET_KEY)
            # st.success(f"✅ Mở bằng KEY: {SHEET_KEY}")
        # except Exception as e:
            # st.error(f"❌ Không mở được bằng KEY. Kiểm tra đã share đúng email SA.\n\n{e}")
            # st.stop()

# ========== 5) ĐỌC DỮ LIỆU ==========
teams_df   = load_worksheet_df(SHEET_KEY, "teams")
players_df = load_worksheet_df(SHEET_KEY, "players")
matches_df = load_worksheet_df(SHEET_KEY, "matches")
events_df  = load_worksheet_df(SHEET_KEY, "events")
knockout_df = load_worksheet_df(SHEET_KEY, "knockout")

# ========== 6) TABS ==========
tab1, tab2, tab3, tab_gallery = st.tabs([
    "🏆 Bảng xếp hạng",
    "📅 Lịch thi đấu",
    "👥 Cầu thủ & Ghi bàn",
    "📸 Ảnh & Highlight"
])


with tab1:
    st.subheader("Bảng xếp hạng")
    if teams_df.empty or matches_df.empty:
        st.warning("Thiếu sheet 'teams' hoặc 'matches' → chưa thể tính BXH.")
    else:
        # Chuẩn hoá tên cột để lọc nhóm
        tdf = teams_df.copy()
        tdf.columns = [c.strip().lower() for c in tdf.columns]
        # ---- Map team_id -> logo_url (strip để tránh lệch key) ----
     
        # ---- Map team_id -> logo_url (strip + chuẩn hoá link Google Drive) ----
        def _normalize_drive_url(u: str) -> str:
            u = str(u or "").strip()
            if not u:
                return ""
            if "drive.google.com" in u:
                # /file/d/<ID>/view
                if "/file/d/" in u:
                    try:
                        fid = u.split("/file/d/")[1].split("/")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
                # open?id=<ID>
                if "open?id=" in u:
                    try:
                        fid = u.split("open?id=")[1].split("&")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
                # uc?id=<ID>
                if "uc?id=" in u and "export=view" not in u:
                    try:
                        fid = u.split("uc?id=")[1].split("&")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
            return u

        TEAM_LOGOS = {}
        if "logo_url" in tdf.columns and "team_id" in tdf.columns:
            tid = tdf.get("team_id", pd.Series(dtype=str)).astype(str).str.strip()
            lur = (tdf.get("logo_url", pd.Series(dtype=str))
                      .astype(str).str.strip()
                      .apply(_normalize_drive_url))
            TEAM_LOGOS = dict(zip(tid, lur))



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
                # 1) Tính BXH bảng A
                table_a = standings_group("A").copy()
                # --- Ép kiểu số & sắp xếp BXH (A) ---
                for c in ["Điểm", "HS", "BT", "FairPlay"]:
                    if c in table_a.columns:
                        table_a[c] = pd.to_numeric(table_a[c], errors="coerce").fillna(0)

                # Sort: Điểm ↓, HS ↓, BT ↓, FairPlay ↑
                sort_cols = [c for c in ["Điểm", "HS", "BT", "FairPlay"] if c in table_a.columns]
                asc_flags = [False, False, False, True][:len(sort_cols)]
                table_a = table_a.sort_values(by=sort_cols, ascending=asc_flags).reset_index(drop=True)

                # Cấp lại thứ hạng 1..n
                if "rank" in table_a.columns:  # phòng TH bạn đã rename trước đó
                    table_a.drop(columns=["rank"], inplace=True)
                elif "Hạng" in table_a.columns:
                    table_a.drop(columns=["Hạng"], inplace=True)
                table_a.insert(0, "rank", range(1, len(table_a) + 1))


                # 2) Chuẩn hoá tên cột về chuẩn dùng chung
                table_a = table_a.rename(columns={
                    "Team ID": "team_id",
                    "Đội": "team_name",
                    "Hạng": "rank"
                })

                # 3) Thêm cột logo từ sheet teams (TEAM_LOGOS đã tạo ở trên)
                if "team_id" in table_a.columns:
                    table_a["logo"] = table_a["team_id"].astype(str).str.strip().map(TEAM_LOGOS).fillna("")

                # 4) Đưa cột logo đứng ngay trước tên đội (nếu có)
                cols_a = list(table_a.columns)
                if "logo" in cols_a and "team_name" in cols_a:
                    cols_a.insert(cols_a.index("team_name"), cols_a.pop(cols_a.index("logo")))
                    table_a = table_a[cols_a]

                # 5) Hiển thị
                st.dataframe(
                    table_a,
                    column_config={
                        "logo": st.column_config.ImageColumn(" ", width="small"),
                        "team_name": "Đội"
                    },
                    hide_index=True,
                    use_container_width=True
                )

            with c2:
                st.markdown("#### Bảng B")
                # 1) Tính BXH bảng B
                table_b = standings_group("B").copy()
                # --- Ép kiểu số & sắp xếp BXH (B) ---
                for c in ["Điểm", "HS", "BT", "FairPlay"]:
                    if c in table_b.columns:
                        table_b[c] = pd.to_numeric(table_b[c], errors="coerce").fillna(0)

                sort_cols = [c for c in ["Điểm", "HS", "BT", "FairPlay"] if c in table_b.columns]
                asc_flags = [False, False, False, True][:len(sort_cols)]
                table_b = table_b.sort_values(by=sort_cols, ascending=asc_flags).reset_index(drop=True)

                if "rank" in table_b.columns:
                    table_b.drop(columns=["rank"], inplace=True)
                elif "Hạng" in table_b.columns:
                    table_b.drop(columns=["Hạng"], inplace=True)
                table_b.insert(0, "rank", range(1, len(table_b) + 1))


                # 2) Chuẩn hoá tên cột về chuẩn dùng chung
                table_b = table_b.rename(columns={
                    "Team ID": "team_id",
                    "Đội": "team_name",
                    "Hạng": "rank"
                })

                # 3) Thêm cột logo
                if "team_id" in table_b.columns:
                    table_b["logo"] = table_b["team_id"].astype(str).str.strip().map(TEAM_LOGOS).fillna("")

                # 4) Đưa cột logo đứng ngay trước tên đội
                cols_b = list(table_b.columns)
                if "logo" in cols_b and "team_name" in cols_b:
                    cols_b.insert(cols_b.index("team_name"), cols_b.pop(cols_b.index("logo")))
                    table_b = table_b[cols_b]

                # 5) Hiển thị
                st.dataframe(
                    table_b,
                    column_config={
                        "logo": st.column_config.ImageColumn(" ", width="small"),
                        "team_name": "Đội"
                    },
                    hide_index=True,
                    use_container_width=True
                )

        else:
            
            # Gộp lại nhưng có cột 'Bảng' để dễ phân biệt
            sA = standings_group("A").copy(); sA.insert(1, "Bảng", "A")
            sB = standings_group("B").copy(); sB.insert(1, "Bảng", "B")
            merged = pd.concat([sA, sB], ignore_index=True)

            # Chuẩn hóa tên cột về định dạng chung rồi mới map logo
            merged = merged.rename(columns={
                "Team ID": "team_id",
                "Đội": "team_name",
                "Hạng": "rank"
            })

            # Thêm cột logo theo sheet teams
            if "team_id" in merged.columns:
                merged["team_id"] = merged["team_id"].astype(str).str.strip()
                merged["logo"] = merged["team_id"].map(TEAM_LOGOS).fillna("")

                # Đưa cột logo đứng ngay trước tên đội
                cols = list(merged.columns)
                if "logo" in cols and "team_name" in cols:
                    cols.insert(cols.index("team_name"), cols.pop(cols.index("logo")))
                    merged = merged[cols]
                    
            # --- Ép kiểu số & sắp xếp BXH (gộp) ---
            for c in ["Điểm", "HS", "BT", "FairPlay"]:
                if c in merged.columns:
                    merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

            sort_cols = [c for c in ["Điểm", "HS", "BT", "FairPlay"] if c in merged.columns]
            asc_flags = [False, False, False, True][:len(sort_cols)]
            merged = merged.sort_values(by=sort_cols, ascending=asc_flags).reset_index(drop=True)

            # Nếu muốn có cột 'rank' chung cho toàn bộ, thêm:
            if "rank" in merged.columns:
                merged.drop(columns=["rank"], inplace=True)
            elif "Hạng" in merged.columns:
                merged.drop(columns=["Hạng"], inplace=True)
            merged.insert(0, "rank", range(1, len(merged) + 1))


            st.dataframe(
                merged,
                column_config={
                    "logo": st.column_config.ImageColumn(" ", width="small"),
                    "team_name": "Đội",
                    "Bảng": "Bảng"
                },
                use_container_width=True,
                hide_index=True
            )




with tab2:
    st.subheader("Lịch thi đấu")

    # ================== CSS HIỆU ỨNG KNOCKOUT ==================
    st.markdown("""
    <style>
    @keyframes koGlow {
      0%,100% { box-shadow: 0 0 0 rgba(59,130,246,0); }
      50% { box-shadow: 0 0 18px rgba(59,130,246,0.55); }
    }
    @keyframes koWinner {
      0%,100% { text-shadow: 0 0 0 rgba(255,215,0,0); }
      50% { text-shadow: 0 0 10px rgba(255,215,0,0.85); }
    }
    @keyframes koPulse {
      0% { transform: scale(1); }
      50% { transform: scale(1.02); }
      100% { transform: scale(1); }
    }

    .ko-card {
      border:1px solid #e5e7eb;
      border-radius:12px;
      padding:8px 10px;
      margin-bottom:10px;
      background:#fff;
    }
    .ko-finished { animation: koGlow 1.2s ease-in-out infinite; }
    .ko-scheduled { animation: koPulse 1.6s ease-in-out infinite; border-style:dashed; }
    .ko-winner { font-weight:900; color:#92400e; animation: koWinner 1s ease-in-out infinite; }
    </style>
    """, unsafe_allow_html=True)

    # ================== FILTER ==================
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        table_pick = st.selectbox("Chọn bảng", ["Tất cả"] + sorted(matches_df["table"].dropna().unique().tolist()))

    with col2:
        view_mode = st.selectbox("Chế độ hiển thị", ["Danh sách", "Sơ đồ nhánh (Knockout)"])

    with col3:
        round_pick = st.selectbox("Chọn vòng", ["Tất cả"] + sorted(matches_df["round"].dropna().unique().tolist()))

    # ================== FILTER DATA ==================
    mdf = matches_df.copy()
    if table_pick != "Tất cả":
        mdf = mdf[mdf["table"] == table_pick]
    if round_pick != "Tất cả":
        mdf = mdf[mdf["round"] == round_pick]

    # ================== VIEW: DANH SÁCH ==================
    if view_mode == "Danh sách":
        for _, r in mdf.sort_values(["date", "time"]).iterrows():
            st.markdown(render_match_card(r), unsafe_allow_html=True)

    # ================== VIEW: KNOCKOUT ==================
    else:
        if knockout_df.empty:
            st.info("Chưa có dữ liệu vòng Knockout.")
        else:
            ko = knockout_df.copy()
            ko = ko.sort_values(["stage", "order"])

            stages = ko["stage"].dropna().unique().tolist()
            cols = st.columns(len(stages))

            def resolve_slot(slot: str) -> str:
                slot = str(slot or "").strip()
                if slot.startswith("W"):
                    mid = slot[1:]
                    got = mdf[mdf["match_id"] == mid]
                    if not got.empty:
                        r = got.iloc[0]
                        try:
                            return r["home_team"] if int(r["home_goals"]) > int(r["away_goals"]) else r["away_team"]
                        except:
                            return "TBD"
                return slot if slot else "TBD"

            for i, stage in enumerate(stages):
                with cols[i]:
                    st.markdown(f"### {stage}")
                    sub = ko[ko["stage"] == stage]

                    for _, rr in sub.iterrows():
                        home = resolve_slot(rr.get("slot_home_from", ""))
                        away = resolve_slot(rr.get("slot_away_from", ""))

                        score_html = "vs"
                        meta_line = ""
                        status_html = ""
                        mid = str(rr.get("match_id", "")).strip()

                        if mid:
                            got = mdf[mdf["match_id"] == mid]
                            if not got.empty:
                                row_m = got.iloc[0]
                                try:
                                    hg = int(row_m.get("home_goals"))
                                    ag = int(row_m.get("away_goals"))
                                    score_html = f"{hg} – {ag}"
                                except:
                                    pass

                                date = str(row_m.get("date", "")).strip()
                                time_ = str(row_m.get("time", "")).strip()
                                venue = str(row_m.get("venue", "")).strip()
                                meta_line = " • ".join([x for x in [date, time_, venue] if x])

                                status_html = render_status_badge(str(row_m.get("status", "")))

                        # ====== EFFECT ======
                        is_finished = (score_html != "vs")
                        status_class = "ko-finished" if is_finished else "ko-scheduled"

                        winner_home = False
                        winner_away = False
                        if is_finished:
                            try:
                                hg_s, ag_s = score_html.replace("–", "-").split("-")
                                if int(hg_s.strip()) > int(ag_s.strip()):
                                    winner_home = True
                                elif int(ag_s.strip()) > int(hg_s.strip()):
                                    winner_away = True
                            except:
                                pass

                        home_cls = "ko-winner" if winner_home else ""
                        away_cls = "ko-winner" if winner_away else ""

                        card_html = f"""
                        <div class="ko-card {status_class}">
                          <div style="display:flex;justify-content:space-between;gap:8px;font-size:14px;">
                            <div class="{home_cls}" style="flex:1;overflow:hidden;text-overflow:ellipsis;">
                              {home}
                            </div>
                            <div style="font-weight:800;">{score_html}</div>
                            <div class="{away_cls}" style="flex:1;text-align:right;overflow:hidden;text-overflow:ellipsis;">
                              {away}
                            </div>
                          </div>
                          <div style="text-align:center;color:#6b7280;font-size:12px;margin-top:2px;">
                            {meta_line} {status_html}
                          </div>
                          <div style="text-align:center;color:#94a3b8;font-size:11px;margin-top:2px;">
                            {mid}
                          </div>
                        </div>
                        """

                        st.markdown(card_html, unsafe_allow_html=True)








with tab3:
    left, right = st.columns([2,1])

    # ===== CSS cho card trao giải (lấp lánh/nhấp nháy thật) =====
    st.markdown("""
    <style>
    @keyframes glowPulse {
      0%, 100% { box-shadow: 0 0 0 rgba(255,215,0,0.0); transform: scale(1); }
      50%      { box-shadow: 0 0 24px rgba(255,215,0,0.60); transform: scale(1.012); }
    }
    @keyframes shimmerMove {
      0%   { background-position: -200% 0; }
      100% { background-position: 200% 0; }
    }
    .award-card{
      border-radius: 14px;
      padding: 12px 14px;
      margin: 8px 0 14px 0;
      border: 1px solid rgba(255,215,0,0.50);
      background: linear-gradient(90deg,
        rgba(255,215,0,0.16),
        rgba(255,255,255,0.93),
        rgba(255,215,0,0.16));
      background-size: 200% 100%;
      animation: shimmerMove 1.15s linear infinite, glowPulse 1.05s ease-in-out infinite;
    }
    .award-title{
      font-weight: 900;
      font-size: 16px;
      letter-spacing: 0.2px;
    }
    .award-sub{
      color: #475569;
      font-size: 13px;
      margin-top: 2px;
      line-height: 1.35;
    }
    .badge{
      display:inline-block;
      padding: 2px 10px;
      border-radius: 999px;
      font-weight: 900;
      font-size: 12px;
      margin-left: 8px;
      background: rgba(255,215,0,0.22);
      border: 1px solid rgba(255,215,0,0.40);
    }
    </style>
    """, unsafe_allow_html=True)

    # Map team_id -> team_name để hiển thị đẹp
    tdf = teams_df.copy(); tdf.columns = [c.strip().lower() for c in tdf.columns]
    name_map = dict(zip(
        tdf.get("team_id", pd.Series(dtype=str)).astype(str),
        tdf.get("team_name", pd.Series(dtype=str)).astype(str)
    ))

    # ========= BÊN TRÁI: DANH SÁCH CẦU THỦ (giữ nguyên) =========
    with left:
        st.subheader("Danh sách cầu thủ")
        if players_df.empty:
            st.info("Chưa có dữ liệu 'players'.")
        else:
            pdf = players_df.copy()
            pdf.columns = [c.strip().lower() for c in pdf.columns]

            pdf["Đội"] = pdf.get("team_id", "").map(name_map).fillna(pdf.get("team_id", ""))

            # ==== Bộ lọc ====
            colf1, colf2 = st.columns([1.2, 1])
            with colf1:
                team_options = ["Tất cả"] + sorted(
                    [n for n in set(name_map.values()) if isinstance(n, str)]
                )
                team_pick = st.selectbox("Lọc theo đội", team_options, index=0)
            with colf2:
                q = st.text_input("Tìm tên / số áo", "")

            show = pdf.copy()

            if team_pick != "Tất cả":
                show = show[show["Đội"] == team_pick]

            if q.strip():
                qq = q.strip().lower()
                def s(col):
                    return show.get(col, pd.Series(dtype=str)).astype(str).str.lower()
                mask = (
                    s("player_name").str.contains(qq, na=False) |
                    s("shirt_number").str.contains(qq, na=False) |
                    s("player_id").str.contains(qq, na=False)
                )
                show = show[mask]

            if "shirt_number" in show.columns:
                show["__shirt_num__"] = pd.to_numeric(show["shirt_number"], errors="coerce")
                show = show.sort_values(by=["Đội", "__shirt_num__", "player_name"], na_position="last")
            else:
                show = show.sort_values(by=["Đội", "player_name"])

            cols = [c for c in [
                "player_id","player_name","Đội","shirt_number","position","dob","nationality","is_registered"
            ] if c in show.columns]
            display_players = show[cols].rename(columns={
                "player_id": "Mã cầu thủ",
                "player_name": "Cầu thủ",
                "shirt_number": "Số áo",
                "position": "Vị trí",
                "dob": "Ngày sinh",
                "nationality": "Quốc tịch",
                "is_registered": "Đã đăng ký"
            })

            st.dataframe(
                display_players.drop(columns=[c for c in ["__shirt_num__"] if c in display_players.columns]),
                use_container_width=True
            )

    # ========= BÊN PHẢI: FAIR PLAY -> VUA PHÁ LƯỚI -> THẺ PHẠT =========
    with right:

        # ==========================================================
        # 1) FAIR PLAY (card lấp lánh + bảng)
        # ==========================================================
        st.subheader("🤝 Đội Fair Play toàn giải")

        if events_df.empty:
            st.info("Chưa có dữ liệu 'events' để tính Fair Play.")
        else:
            ev2 = events_df.copy()
            ev2.columns = [c.strip().lower() for c in ev2.columns]

            # Điểm fairplay (càng thấp càng tốt)
            fp_all = compute_fairplay(ev2)

            def _cnt(et: str) -> dict:
                if ev2.empty or "event_type" not in ev2.columns or "team_id" not in ev2.columns:
                    return {}
                s = ev2[ev2["event_type"].astype(str).str.lower() == et]
                return s.groupby("team_id").size().to_dict()

            c_y   = _cnt("yellow")
            c_sy  = _cnt("second_yellow")
            c_r   = _cnt("red")
            c_ypr = _cnt("yellow_plus_direct_red")

            team_ids = sorted(set(name_map.keys()) | set(fp_all.keys()))
            rows = []
            for tid in team_ids:
                rows.append({
                    "team_id": tid,
                    "Đội": name_map.get(tid, tid),
                    "Điểm FairPlay": int(fp_all.get(tid, 0)),
                    "Thẻ vàng": int(c_y.get(tid, 0)),
                    "Đỏ gián tiếp (2V)": int(c_sy.get(tid, 0)),
                    "Đỏ trực tiếp": int(c_r.get(tid, 0)),
                    "Vàng+Đỏ": int(c_ypr.get(tid, 0)),
                })

            fp_df = pd.DataFrame(rows)

            if fp_df.empty:
                st.info("Chưa có dữ liệu Fair Play.")
            else:
                fp_df["Tổng đỏ"] = (
                    fp_df["Đỏ trực tiếp"] +
                    fp_df["Đỏ gián tiếp (2V)"] +
                    fp_df["Vàng+Đỏ"]
                )

                # Sort: FairPlay ↑, Tổng đỏ ↑, Thẻ vàng ↑, Đội ↑ (ổn định)
                fp_df = fp_df.sort_values(
                    by=["Điểm FairPlay", "Tổng đỏ", "Thẻ vàng", "Đội"],
                    ascending=[True, True, True, True]
                ).reset_index(drop=True)

                # Hạng đồng hạng theo (FairPlay, Tổng đỏ, Thẻ vàng)
                rank_vals = []
                cur_rank = 1
                prev_key = None
                for i, r in fp_df.iterrows():
                    key = (int(r["Điểm FairPlay"]), int(r["Tổng đỏ"]), int(r["Thẻ vàng"]))
                    if prev_key is None:
                        rank_vals.append(cur_rank)
                        prev_key = key
                        continue
                    if key != prev_key:
                        cur_rank = i + 1
                        prev_key = key
                    rank_vals.append(cur_rank)
                fp_df.insert(0, "Hạng", rank_vals)

                # Card Top 1 (nhấp nháy)
                top_fp = fp_df[fp_df["Hạng"] == 1].copy()
                if not top_fp.empty:
                    best = int(top_fp["Điểm FairPlay"].min())
                    names = " • ".join(top_fp["Đội"].astype(str).tolist())
                    note = "Đồng hạng 1 (chia đôi)" if len(top_fp) >= 2 else "Top 1"
                    st.markdown(f"""
                    <div class="award-card">
                      <div class="award-title">🏆 Đội Fair Play <span class="badge">{note}</span></div>
                      <div class="award-sub">🤝 {best} điểm — {names}</div>
                    </div>
                    """, unsafe_allow_html=True)

                show_fp = fp_df[["Hạng","Đội","Điểm FairPlay","Thẻ vàng","Đỏ gián tiếp (2V)","Đỏ trực tiếp","Vàng+Đỏ"]].copy()
                st.dataframe(show_fp, use_container_width=True, hide_index=True)

        st.divider()

        # ==========================================================
        # 2) VUA PHÁ LƯỚI (card lấp lánh + bảng)
        # ==========================================================
        st.subheader("⚽ Vua phá lưới (tạm tính)")

        if events_df.empty:
            st.info("Chưa có dữ liệu 'events'.")
        else:
            ev = events_df.copy()
            ev.columns = [c.strip().lower() for c in ev.columns]

            if "player_id" in ev.columns and not players_df.empty:
                ev["player_id"] = ev["player_id"].astype(str)

                pmini = players_df.copy()
                pmini.columns = [c.strip().lower() for c in pmini.columns]
                pmini["player_id"] = pmini["player_id"].astype(str)
                pmini["Đội"] = pmini.get("team_id", "").map(name_map).fillna(pmini.get("team_id",""))

                if "event_type" in ev.columns:
                    goals = ev[ev["event_type"].astype(str).str.lower() == "goal"]
                    if goals.empty:
                        st.info("Chưa có bàn thắng nào.")
                    else:
                        top = (goals.groupby("player_id").size()
                               .reset_index(name="Bàn thắng"))

                        top = (pmini.merge(top, how="right", on="player_id")
                                   .rename(columns={
                                       "player_id": "Mã cầu thủ",
                                       "player_name": "Cầu thủ"
                                   }))

                        top = top[["Mã cầu thủ","Cầu thủ","Đội","Bàn thắng"]].sort_values(
                            ["Bàn thắng","Cầu thủ"], ascending=[False, True]
                        ).reset_index(drop=True)

                        top.insert(0, "Hạng", top["Bàn thắng"].rank(method="min", ascending=False).astype(int))

                        max_goals = int(top["Bàn thắng"].max()) if not top.empty else 0
                        top1 = top[top["Bàn thắng"] == max_goals].copy()

                        # Card Top 1 (nhấp nháy)
                        if not top1.empty and max_goals > 0:
                            names = " • ".join([f"{r['Cầu thủ']} ({r['Đội']})" for _, r in top1.iterrows()])
                            note = "Đồng hạng 1 (chia đôi)" if len(top1) >= 2 else "Top 1"
                            st.markdown(f"""
                            <div class="award-card">
                              <div class="award-title">🏆 Vua phá lưới <span class="badge">{note}</span></div>
                              <div class="award-sub">⚽ {max_goals} bàn — {names}</div>
                            </div>
                            """, unsafe_allow_html=True)

                        # Bảng
                        top["Ghi chú"] = ""
                        if max_goals > 0:
                            top.loc[top["Bàn thắng"] == max_goals, "Ghi chú"] = "🏆 Top 1"
                        st.dataframe(
                            top[["Hạng","Cầu thủ","Đội","Bàn thắng","Ghi chú"]],
                            use_container_width=True,
                            hide_index=True
                        )
            else:
                st.info("Thiếu player_id hoặc sheet players để tính vua phá lưới.")

        st.divider()

        # ==========================================================
        # 3) THẺ PHẠT + TIỀN PHẠT (đẩy xuống dưới cùng)
        # ==========================================================
        st.subheader("🟨🟥 Thẻ phạt & Tiền phạt")

        if events_df.empty:
            st.info("Chưa có dữ liệu 'events'.")
        else:
            ev = events_df.copy()
            ev.columns = [c.strip().lower() for c in ev.columns]

            if "player_id" in ev.columns and "player_id" in players_df.columns:
                ev["player_id"] = ev["player_id"].astype(str)

                pmini = players_df.copy()
                pmini.columns = [c.strip().lower() for c in pmini.columns]
                pmini["player_id"] = pmini["player_id"].astype(str)
                pmini["Đội"] = pmini.get("team_id", "").map(name_map).fillna(pmini.get("team_id",""))

                card_types = ["yellow","red","second_yellow","yellow_plus_direct_red"]
                cards = ev[ev.get("event_type","").isin(card_types)]

                if cards.empty:
                    st.info("Chưa có sự kiện thẻ nào.")
                else:
                    card_pvt = (cards.pivot_table(
                                    index="player_id",
                                    columns="event_type",
                                    aggfunc="size",
                                    fill_value=0
                                ).reset_index())
                    card_pvt.columns = [str(c) for c in card_pvt.columns]

                    card_pvt = pmini.merge(card_pvt, how="right", on="player_id")

                    # ----- CẤU HÌNH MỨC PHẠT (đồng) -----
                    FINE_YELLOW = 200_000
                    FINE_SECOND_YELLOW = 300_000
                    FINE_RED = 500_000
                    FINE_YPR = 700_000

                    for c in ["yellow","second_yellow","red","yellow_plus_direct_red"]:
                        if c not in card_pvt.columns:
                            card_pvt[c] = 0

                    card_pvt["Tiền phạt"] = (
                        card_pvt["yellow"] * FINE_YELLOW +
                        card_pvt["second_yellow"] * FINE_SECOND_YELLOW +
                        card_pvt["red"] * FINE_RED +
                        card_pvt["yellow_plus_direct_red"] * FINE_YPR
                    )

                    teams_list = ["Tất cả"] + sorted(
                        pd.Series(pmini.get("Đội", [])).dropna().unique().tolist()
                    )
                    pick_team = st.selectbox("Lọc thẻ & tiền phạt theo đội", teams_list, key="fine_filter_team")

                    show_fines = card_pvt.copy()
                    if pick_team != "Tất cả":
                        show_fines = show_fines[show_fines.get("Đội","") == pick_team]

                    total_fine = int(show_fines["Tiền phạt"].sum())
                    if pick_team != "Tất cả":
                        st.markdown(f"**Tổng tiền phạt của đội _{pick_team}_:** `{total_fine:,} đ`")
                    else:
                        st.markdown(f"**Tổng tiền phạt toàn giải:** `{total_fine:,} đ`")

                    rename_cards = {
                        "player_id": "Mã cầu thủ",
                        "player_name": "Cầu thủ",
                        "yellow": "Thẻ vàng",
                        "red": "Thẻ đỏ trực tiếp",
                        "second_yellow": "Đỏ gián tiếp (2V)",
                        "yellow_plus_direct_red": "Vàng + Đỏ trực tiếp"
                    }
                    show_fines = show_fines.rename(columns=rename_cards)

                    keep = [c for c in [
                        "Mã cầu thủ","Cầu thủ","Đội",
                        "Thẻ vàng","Đỏ gián tiếp (2V)","Thẻ đỏ trực tiếp","Vàng + Đỏ trực tiếp",
                        "Tiền phạt"
                    ] if c in show_fines.columns]

                    st.dataframe(
                        show_fines[keep].sort_values(by="Tiền phạt", ascending=False),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.info("Thiếu player_id hoặc sheet players để thống kê thẻ & tiền phạt.")




with tab_gallery:
    st.subheader("📸 Ảnh & Highlight")

    # ===================== HIGHLIGHTS =====================
    st.markdown("### 🔥 Highlights & Full match")
    try:
        hl_df = load_worksheet_df(SHEET_KEY, "highlights")
        hl_df.columns = [c.strip().lower() for c in hl_df.columns]
        required_hl_cols = {"title", "highlight", "full", "download"}
        if hl_df.empty or not required_hl_cols.issubset(set(hl_df.columns)):
            st.info("Sheet **highlights** thiếu cột hoặc chưa có dữ liệu. Cần các cột: "
                    "`title | highlight | full | download` (tùy chọn: `round`, `match_id`).")
        else:
            # (tuỳ chọn) bộ lọc vòng hoặc match nếu có
            fl1, fl2 = st.columns([1,1])
            with fl1:
                opt_rounds = sorted([x for x in hl_df.get("round", "").dropna().unique().tolist() if str(x).strip()])
                round_sel = st.selectbox("Lọc theo vòng (tuỳ chọn)", ["Tất cả"] + opt_rounds) if opt_rounds else "Tất cả"
            with fl2:
                opt_matches = sorted([x for x in hl_df.get("match_id", "").dropna().unique().tolist() if str(x).strip()])
                match_sel = st.selectbox("Lọc theo match (tuỳ chọn)", ["Tất cả"] + opt_matches) if opt_matches else "Tất cả"

            show_hl = hl_df.copy()
            if round_sel != "Tất cả" and "round" in show_hl.columns:
                show_hl = show_hl[show_hl["round"].astype(str) == str(round_sel)]
            if match_sel != "Tất cả" and "match_id" in show_hl.columns:
                show_hl = show_hl[show_hl["match_id"].astype(str) == str(match_sel)]

            for _, r in show_hl.iterrows():
                title = str(r.get("title","")).strip()
                url_hl = str(r.get("highlight","")).strip()
                url_full = str(r.get("full","")).strip()
                url_dl = str(r.get("download","")).strip()

                if title:
                    st.markdown(f"**{title}**")
                # Nhúng video nếu link YouTube, ngược lại hiển thị link
                if any(host in url_hl for host in ["youtube.com", "youtu.be"]):
                    st.video(url_hl)
                elif url_hl:
                    st.markdown(f"[Xem highlights]({url_hl})")

                c1, c2, c3 = st.columns(3)
                with c1:
                    if url_hl: st.markdown(f"[🔥 Highlights]({url_hl})")
                with c2:
                    if url_full: st.markdown(f"[📺 Full match]({url_full})")
                with c3:
                    if url_dl: st.markdown(f"[📥 Tải tình huống]({url_dl})")
                st.divider()
    except Exception as e:
        st.error(f"Lỗi đọc sheet 'highlights': {e}")

    # ======================== PHOTOS ======================
    st.markdown("### 🖼️ Album ảnh")
    st.caption("Mẹo: Ảnh Google Drive dùng dạng `https://drive.google.com/uc?id=FILE_ID` để hiển thị trực tiếp.")

    try:
        ph_df = load_worksheet_df(SHEET_KEY, "photos")
        ph_df.columns = [c.strip().lower() for c in ph_df.columns]
        if ph_df.empty or "url" not in ph_df.columns:
            st.info("Sheet **photos** thiếu cột hoặc chưa có dữ liệu. Cần các cột: `url | caption` "
                    "(tùy chọn: `round`, `match_id`).")
        else:
            # (tuỳ chọn) bộ lọc
            fl3, fl4 = st.columns([1,1])
            with fl3:
                opt_rounds_p = sorted([x for x in ph_df.get("round", "").dropna().unique().tolist() if str(x).strip()])
                round_sel_p = st.selectbox("Lọc ảnh theo vòng (tuỳ chọn)", ["Tất cả"] + opt_rounds_p) if opt_rounds_p else "Tất cả"
            with fl4:
                opt_matches_p = sorted([x for x in ph_df.get("match_id", "").dropna().unique().tolist() if str(x).strip()])
                match_sel_p = st.selectbox("Lọc ảnh theo match (tuỳ chọn)", ["Tất cả"] + opt_matches_p) if opt_matches_p else "Tất cả"

            show_ph = ph_df.copy()
            if round_sel_p != "Tất cả" and "round" in show_ph.columns:
                show_ph = show_ph[show_ph["round"].astype(str) == str(round_sel_p)]
            if match_sel_p != "Tất cả" and "match_id" in show_ph.columns:
                show_ph = show_ph[show_ph["match_id"].astype(str) == str(match_sel_p)]

            urls = show_ph["url"].fillna("").tolist()
            caps = show_ph.get("caption", "").fillna("").tolist()

            if not urls:
                st.info("Chưa có ảnh để hiển thị.")
            else:
                cols = st.columns(3)
                for i, url in enumerate(urls):
                    if not url: 
                        continue
                    with cols[i % 3]:
                        st.image(url, caption=(caps[i] if i < len(caps) else ""), use_column_width=True)
    except Exception as e:
        st.error(f"Lỗi đọc sheet 'photos': {e}")














