# app.py
import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

st.set_page_config(page_title="⚽ Giải Chim Non Lần 2 — Cup Manager 🏆", layout="wide")

# === BACKGROUND nền chìm toàn trang: phương án chắc chắn ===
BG_URL = "https://drive.google.com/uc?id=1H_06y2X9Vdleg6-VqsWebWF353Gfe21U"

st.markdown(f"""
<style>
/* Đảm bảo root cao full để nền phủ đúng */
html, body, .stApp {{
  height: 100%;
}}

/* Bỏ nền trắng mặc định của vùng nội dung để nhìn thấy ảnh phía sau */
[data-testid="stAppViewContainer"] {{
  background: transparent !important;
}}

/* Phần tử nền cố định phủ toàn màn hình, nằm dưới mọi nội dung */
#global-bg-holder {{
  position: fixed;
  inset: 0;               /* top:0; right:0; bottom:0; left:0 */
  z-index: -1;            /* cho xuống dưới toàn bộ app */
}}

/* Dùng ::before để vẽ background lên holder */
#global-bg-holder::before {{
  content: "";
  position: absolute;
  inset: 0;
  background-image: url('{BG_URL}');
  background-size: cover;
  background-position: center;
  background-attachment: fixed;  /* hiệu ứng parallax mượt */
  background-repeat: no-repeat;
  opacity: 0.18;                 /* Độ mờ (0.12–0.25 đẹp) */
  filter: saturate(110%) contrast(105%);
}}

/* Giữ header dễ đọc */
[data-testid="stHeader"] {{
  background: rgba(255,255,255,0.82) !important;
  backdrop-filter: blur(4px);
  border-bottom: 1px solid rgba(0,0,0,0.05);
}}
</style>

<!-- Phần tử nền đứng độc lập, fixed toàn trang -->
<div id="global-bg-holder"></div>
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
    if matches_df.empty:
        st.info("Chưa có dữ liệu 'matches'.")
    else:
        # Chuẩn hoá cột
        tdf = teams_df.copy();  tdf.columns = [c.strip().lower() for c in tdf.columns]
        mdf = matches_df.copy(); mdf.columns = [c.strip().lower() for c in mdf.columns]
        evdf = events_df.copy(); evdf.columns = [c.strip().lower() for c in evdf.columns]
        # Map team_id -> logo_url (nếu có cột logo_url trong sheet teams)
        # Map team_id -> logo_url (strip + chuẩn hoá link Google Drive)
        def _normalize_drive_url(u: str) -> str:
            u = str(u or "").strip()
            if not u:
                return ""
            if "drive.google.com" in u:
                if "/file/d/" in u:
                    try:
                        fid = u.split("/file/d/")[1].split("/")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
                if "open?id=" in u:
                    try:
                        fid = u.split("open?id=")[1].split("&")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
                if "uc?id=" in u and "export=view" not in u:
                    try:
                        fid = u.split("uc?id=")[1].split("&")[0]
                        return f"https://drive.google.com/thumbnail?id={fid}&sz=w128-h128"
                    except Exception:
                        pass
            return u

        TEAM_LOGOS = {}
        if "logo_url" in tdf.columns:
            TEAM_LOGOS = dict(zip(
                tdf.get("team_id", pd.Series(dtype=str)).astype(str).str.strip(),
                tdf.get("logo_url", pd.Series(dtype=str)).astype(str).str.strip().apply(_normalize_drive_url)
            ))



        # Map team_id -> team_name
        name_map = dict(zip(
            tdf.get("team_id", pd.Series(dtype=str)),
            tdf.get("team_name", pd.Series(dtype=str))
        ))

        # Map player_id -> (player_name, shirt_number, team_id)
        pdf = players_df.copy(); pdf.columns = [c.strip().lower() for c in pdf.columns]
        pmap = {}
        if not pdf.empty and "player_id" in pdf.columns:
            for _, r in pdf.iterrows():
                pid = str(r.get("player_id","")).strip()
                if not pid:
                    continue
                pmap[pid] = (
                    r.get("player_name",""),
                    r.get("shirt_number",""),
                    r.get("team_id",""),
                )

        # Tên đội để hiển thị
        mdf["home_name"] = mdf["home_team_id"].map(name_map).fillna(mdf["home_team_id"])
        mdf["away_name"] = mdf["away_team_id"].map(name_map).fillna(mdf["away_team_id"])

        # ====== Bộ lọc ======
        col1, col2, col3 = st.columns([1,1,1.2])
        with col1:
            grp = st.selectbox("Chọn bảng", ["Tất cả", "A", "B"])
        with col2:
            view_mode = st.selectbox("Chế độ hiển thị", ["Tách theo vòng", "Gộp tất cả", "Sơ đồ nhánh (Knockout)"])
        with col3:
            rounds_all = sorted(pd.Series(mdf.get("round", [])).dropna().unique().tolist())
            rnd = st.selectbox("Chọn vòng", ["Tất cả"] + rounds_all)

        # Áp bộ lọc dữ liệu nền
        show = mdf.copy()
        if grp != "Tất cả":
            show = show[show.get("group", "").astype(str).str.upper() == grp]
        if view_mode == "Gộp tất cả" and rnd != "Tất cả":
            show = show[show.get("round", "") == rnd]

        # Sắp xếp đẹp
        if {"date","time","venue"}.issubset(show.columns):
            show = show.sort_values(by=["date","time","venue","match_id"])

        # ====== CSS cho “thẻ trận đấu” ======
        st.markdown("""
        <style>
        .match-card{
            padding: 10px 14px; border-radius: 12px; border: 1px solid #e9ecef;
            background: #fff; margin-bottom: 8px;
        }
        .match-row{
            display:flex; align-items:center; justify-content:space-between;
            gap: 12px; font-size:18px; line-height:1.35;
        }
        .team{
            flex: 1 1 40%; display:flex; align-items:center; gap:8px; font-weight:600;
            white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
        }
        .score{ flex: 0 0 auto; font-weight:800; min-width:80px; text-align:center; }
        .sub{ color:#6c757d; font-size:12.5px; margin-top:4px; text-align:center; }
        .status-badge{
            display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px;
            border:1px solid #dee2e6; margin-left:6px;
        }
        .status-finished{ background:#ecfdf5; border-color:#bbf7d0; color:#065f46;}
        .status-scheduled{ background:#eff6ff; border-color:#bfdbfe; color:#1e3a8a;}
        .status-live{ background:#fff7ed; border-color:#fed7aa; color:#9a3412;}
        .ev-head{ font-weight:700; margin:6px 0 4px 0; }
        .ev-item{ margin:0 0 2px 0; }
        </style>
        """, unsafe_allow_html=True)

        def render_status_badge(val: str) -> str:
            if not isinstance(val, str):
                return ""
            v = val.strip().lower()
            if v in {"finished","kết thúc","ket thuc","done","ft"}:
                return "<span class='status-badge status-finished'>Finished</span>"
            if v in {"scheduled","chưa đá","pending"}:
                return "<span class='status-badge status-scheduled'>Scheduled</span>"
            if v in {"live","playing"}:
                return "<span class='status-badge status-live'>Live</span>"
            return f"<span class='status-badge'>{val}</span>"

        def match_card(row: pd.Series) -> str:
            home = str(row.get("home_name","")).strip()
            away = str(row.get("away_name","")).strip()
            hg = row.get("home_goals", None)
            ag = row.get("away_goals", None)
        # ==== Lấy logo đội bóng ====
            home_id = str(row.get("home_team_id","")).strip()
            away_id = str(row.get("away_team_id","")).strip()
            home_logo = TEAM_LOGOS.get(home_id, "")
            away_logo = TEAM_LOGOS.get(away_id, "")

            def team_with_logo(name: str, logo_url: str, align_right: bool = False) -> str:
                """Ghép logo và tên đội bóng"""
                if not logo_url:
                    return name
                if align_right:
                    return (f"<span style='display:inline-flex;align-items:center;gap:8px;'>"
                            f"<span>{name}</span>"
                            f"<img src='{logo_url}' width='22' height='22' "
                            f"style='object-fit:contain;border-radius:50%;'/>"
                            f"</span>")
                else:
                    return (f"<span style='display:inline-flex;align-items:center;gap:8px;'>"
                            f"<img src='{logo_url}' width='22' height='22' "
                            f"style='object-fit:contain;border-radius:50%;'/>"
                            f"<span>{name}</span>"
                            f"</span>")

            home_html = team_with_logo(home, home_logo, align_right=False)
            away_html = team_with_logo(away, away_logo, align_right=True)

            try:
                hg_i = int(hg) if pd.notna(hg) else None
                ag_i = int(ag) if pd.notna(ag) else None
            except Exception:
                hg_i = ag_i = None
            score_html = f"{hg_i} – {ag_i}" if (hg_i is not None and ag_i is not None) else "vs"

            date = str(row.get("date","")).strip()
            time_ = str(row.get("time","")).strip()
            venue = str(row.get("venue","")).strip()
            meta = " • ".join([x for x in [date, time_, venue] if x])
            status_html = render_status_badge(str(row.get("status","")).strip())

            return f"""
            <div class='match-card'>
              <div class='match-row'>
                <div class='team' style='justify-content:flex-start;'>{home_html}</div>
                <div class='score'>{score_html}</div>
                <div class='team' style='justify-content:flex-end; text-align:right;'>{away_html}</div>
              </div>
              <div class='sub'>{meta} {status_html}</div>
            </div>
            """


        # ====== Helpers: dựng danh sách sự kiện theo đội ======
        def format_event_item(ev: dict) -> str:
            et = str(ev.get("event_type","")).lower()
            icon = ""
            if et == "goal":
                icon = "⚽"
            elif et in {"yellow", "yellow_card"}:
                icon = "🟨"
            elif et in {"red", "red_card"}:
                icon = "🟥"
            elif et in {"second_yellow"}:
                icon = "🟨🟨"
            elif et in {"yellow_plus_direct_red"}:
                icon = "🟨➕🟥"

            minute = str(ev.get("minute","")).strip()
            pid = str(ev.get("player_id","")).strip()
            pname, shirt, _tid = pmap.get(pid, ("", "", ""))
            if not pname:
                pname = ev.get("player_name", pid)

            left = f"{shirt}. {pname}".strip(". ").strip()
            right = f"({minute}')" if minute else ""
            return f"<div class='ev-item'>{icon} {left} {right}</div>"

        def render_events_for_match(match_row: pd.Series):
            if evdf.empty or "match_id" not in evdf.columns:
                st.info("Chưa có dữ liệu sự kiện cho trận này.")
                return
            mid = match_row.get("match_id", "")
            if not mid:
                st.info("Thiếu match_id để tra cứu sự kiện.")
                return

            ev = evdf[evdf["match_id"].astype(str) == str(mid)].copy()
            if ev.empty:
                st.info("Chưa ghi nhận sự kiện nào.")
                return

            ev["__min"] = pd.to_numeric(ev.get("minute"), errors="coerce")
            ev = ev.sort_values(["__min", "event_type"], na_position="last")

            home_id = str(match_row.get("home_team_id",""))
            away_id = str(match_row.get("away_team_id",""))

            colL, colR = st.columns(2)
            with colL:
                st.markdown(f"**{match_row.get('home_name','')}**")
                home_ev = ev[ev.get("team_id","").astype(str) == home_id]
                if home_ev.empty:
                    st.write("—")
                else:
                    html = ["<div class='ev-head'>Sự kiện</div>"]
                    for _, e in home_ev.iterrows():
                        html.append(format_event_item(e))
                    st.markdown("\n".join(html), unsafe_allow_html=True)

            with colR:
                st.markdown(f"**{match_row.get('away_name','')}**")
                away_ev = ev[ev.get("team_id","").astype(str) == away_id]
                if away_ev.empty:
                    st.write("—")
                else:
                    html = ["<div class='ev-head'>Sự kiện</div>"]
                    for _, e in away_ev.iterrows():
                        html.append(format_event_item(e))
                    st.markdown("\n".join(html), unsafe_allow_html=True)

        # ====== helpers cho knockout ======
        def norm_round(val: str) -> str:
            if not isinstance(val, str):
                return ""
            v = val.strip().lower()
            maps = {
                "1/8": ["1/8", "vong 1/8", "r16", "round of 16", "16"],
                "Tứ kết": ["tứ kết", "tu ket", "qf", "quarterfinal", "8"],
                "Bán kết": ["bán kết", "ban ket", "sf", "semifinal", "4"],
                "Chung kết": ["chung kết", "chung ket", "final", "f"],
                "Tranh hạng 3": ["tranh hạng 3", "tranh hang 3", "3rd", "third", "3p", "3rd place"],
            }
            for k, arr in maps.items():
                if v in arr:
                    return k
            return val.strip().title()

        def small_card(row: pd.Series) -> str:
            hg = row.get("home_goals"); ag = row.get("away_goals")
            try:
                hg_i = int(hg) if pd.notna(hg) else None
                ag_i = int(ag) if pd.notna(ag) else None
            except Exception:
                hg_i = ag_i = None
            score_html = f"{hg_i} – {ag_i}" if (hg_i is not None and ag_i is not None) else "vs"
            date = str(row.get("date","")).strip()
            time_ = str(row.get("time","")).strip()
            meta = " • ".join([x for x in [date, time_] if x])
            return f"""
            <div style='border:1px solid #e9ecef;border-radius:10px;padding:8px 10px;margin-bottom:8px;background:#fff;'>
              <div style='display:flex;justify-content:space-between;gap:8px;font-size:14px;'>
                <div style='flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>{row.get("home_name","")}</div>
                <div style='font-weight:700;'>{score_html}</div>
                <div style='flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:right;'>{row.get("away_name","")}</div>
              </div>
              <div style='text-align:center;color:#6c757d;font-size:12px;margin-top:2px;'>{meta}</div>
            </div>
            """

        # ====== Hiển thị ======
        if view_mode == "Sơ đồ nhánh (Knockout)":
            # Ưu tiên đọc sheet 'knockout' nếu đã load vào biến toàn cục
            ko_df = globals().get("knockout_df", pd.DataFrame())
            # Nếu không có, fallback: lấy từ matches nơi stage không chứa 'vòng bảng'
            if ko_df.empty:
                s = show.copy()
                s_stage = s.get("stage", pd.Series(dtype=str)).astype(str).str.lower()
                knockout = s[~s_stage.str.contains("vòng bảng|vong bang|group", na=False)].copy()
                if knockout.empty:
                    st.info("Chưa có dữ liệu vòng loại trực tiếp (knockout).")
                else:
                    knockout["round_norm"] = knockout.get("round","").apply(norm_round)
                    order = ["1/8","Tứ kết","Bán kết","Chung kết","Tranh hạng 3"]
                    rounds_present = [r for r in order if r in knockout["round_norm"].unique().tolist()]
                    if not rounds_present:
                        rounds_present = sorted(knockout["round_norm"].dropna().unique().tolist())
                    cols = st.columns(len(rounds_present)) if rounds_present else st.columns(1)
                    for i, rname in enumerate(rounds_present):
                        with cols[i]:
                            st.markdown(f"#### {rname}")
                            subr = knockout[knockout["round_norm"] == rname].copy()
                            if {"date","time"}.issubset(subr.columns):
                                subr = subr.sort_values(by=["date","time","match_id"])
                            for _, r in subr.iterrows():
                                st.markdown(small_card(r), unsafe_allow_html=True)
            else:
                # Đọc theo cấu hình slot trong sheet 'knockout'
                ko = ko_df.copy()
                ko.columns = [c.strip().lower() for c in ko.columns]
                for c in ["ko_id","round","match_id","slot_home_from","slot_away_from","notes"]:
                    if c not in ko.columns:
                        ko[c] = ""

                # Lấy standings hiện thời để resolve A1, B4...
                slot_to_team = {}
                try:
                    stand = compute_standings(teams_df, matches_df, events_df).copy()
                    stand.columns = [x.strip().lower() for x in stand.columns]
                    grp_col = "group" if "group" in stand.columns else "bảng"
                    team_col = "team_name" if "team_name" in stand.columns else ("đội" if "đội" in stand.columns else "team_id")
                    if "pos" in stand.columns:
                        pos_col = "pos"
                    elif "rank" in stand.columns:
                        pos_col = "rank"
                    elif "thứ hạng" in stand.columns:
                        pos_col = "thứ hạng"
                    else:
                        stand["pos"] = stand.groupby(grp_col).cumcount()+1
                        pos_col = "pos"
                    for _, rr in stand.dropna(subset=[grp_col]).iterrows():
                        slot_to_team[f"{str(rr[grp_col]).strip().upper()}{int(rr[pos_col])}"] = str(rr[team_col])
                except Exception:
                    pass

                mm = mdf.copy()
                win_by_match, lose_by_match = {}, {}
                for _, r in mm.iterrows():
                    mid = str(r.get("match_id","")).strip()
                    try:
                        hg = int(r.get("home_goals")); ag = int(r.get("away_goals"))
                    except Exception:
                        continue
                    if not mid or hg == ag:
                        continue
                    hname = name_map.get(r.get("home_team_id",""), r.get("home_team_id",""))
                    aname = name_map.get(r.get("away_team_id",""), r.get("away_team_id",""))
                    if hg > ag:
                        win_by_match[mid] = hname; lose_by_match[mid] = aname
                    else:
                        win_by_match[mid] = aname; lose_by_match[mid] = hname

                def resolve_slot(s: str) -> str:
                    s = str(s).strip()
                    if not s: return ""
                    S = s.upper()
                    if len(S) in (2,3) and S[0].isalpha() and S[1:].isdigit():
                        return slot_to_team.get(S, s)
                    if S.startswith("WINNER "):
                        mid = s.split()[-1];  return win_by_match.get(mid, s)
                    if S.startswith("LOSER "):
                        mid = s.split()[-1];  return lose_by_match.get(mid, s)
                    return s

                order = ["1/8","Tứ kết","Bán kết","Chung kết","Tranh hạng 3"]
                ko["round_norm"] = ko["round"].apply(norm_round)
                rounds_present = [r for r in order if r in ko["round_norm"].unique().tolist()]
                if not rounds_present:
                    rounds_present = sorted(ko["round_norm"].dropna().unique().tolist())
                cols = st.columns(len(rounds_present)) if rounds_present else st.columns(1)
                for i, rn in enumerate(rounds_present):
                    with cols[i]:
                        st.markdown(f"#### {rn}")
                        subr = ko[ko["round_norm"] == rn].copy().sort_values(by=["ko_id","match_id"])
                        for _, rr in subr.iterrows():
                            # hiển thị theo slot (A1, B4, Winner M201, ...)
                            home = resolve_slot(rr.get("slot_home_from",""))
                            away = resolve_slot(rr.get("slot_away_from",""))
                            # cố lấy tỉ số ở matches nếu có match_id
                            score_html = "vs"
                            mid = str(rr.get("match_id","")).strip()
                            if mid:
                                got = mdf[mdf.get("match_id","") == mid]
                                if not got.empty:
                                    try:
                                        hg = int(got.iloc[0].get("home_goals")); ag = int(got.iloc[0].get("away_goals"))
                                        score_html = f"{hg} – {ag}"
                                    except Exception:
                                        pass
                            card_html = f"""
                            <div style='border:1px solid #e9ecef;border-radius:10px;padding:8px 10px;margin-bottom:8px;background:#fff;'>
                              <div style='display:flex;justify-content:space-between;gap:8px;font-size:14px;'>
                                <div style='flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>{home}</div>
                                <div style='font-weight:700;'>{score_html}</div>
                                <div style='flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:right;'>{away}</div>
                              </div>
                              <div style='text-align:center;color:#6c757d;font-size:12px;margin-top:2px;'>
                                {mid} {rr.get("notes","") or ""}
                              </div>
                            </div>
                            """
                            st.markdown(card_html, unsafe_allow_html=True)

        elif view_mode == "Tách theo vòng":
            if show.empty:
                st.info("Không có trận nào khớp bộ lọc.")
            else:
                rounds = sorted(pd.Series(show.get("round", [])).dropna().unique().tolist())
                if not rounds:
                    st.info("Không tìm thấy cột hoặc giá trị 'round' — hiển thị gộp tất cả.")
                    for _, row in show.iterrows():
                        st.markdown(match_card(row), unsafe_allow_html=True)
                        with st.expander(f"Chi tiết trận {row.get('match_id','')}", expanded=False):
                            render_events_for_match(row)
                else:
                    for r in rounds:
                        sub = show[show.get("round", "") == r].copy()
                        st.markdown(f"### Vòng {r}")
                        for _, row in sub.iterrows():
                            st.markdown(match_card(row), unsafe_allow_html=True)
                            with st.expander(f"Chi tiết trận {row.get('match_id','')}", expanded=False):
                                render_events_for_match(row)

                        # --- TỔNG HỢP VÒNG ---
                        sub_calc = sub.copy()
                        sub_calc["home_goals"] = pd.to_numeric(sub_calc.get("home_goals"), errors="coerce")
                        sub_calc["away_goals"] = pd.to_numeric(sub_calc.get("away_goals"), errors="coerce")
                        played = sub_calc.dropna(subset=["home_goals", "away_goals"])

                        n_matches = len(sub)
                        n_played  = len(played)
                        gf_home   = int(played["home_goals"].sum()) if n_played else 0
                        gf_away   = int(played["away_goals"].sum()) if n_played else 0
                        goals_tot = gf_home + gf_away
                        avg_goals = (goals_tot / n_played) if n_played else 0.0

                        home_wins = int((played["home_goals"] > played["away_goals"]).sum())
                        away_wins = int((played["home_goals"] < played["away_goals"]).sum())
                        draws     = int((played["home_goals"] == played["away_goals"]).sum())

                        yellow = sy = red = ypr = 0
                        try:
                            if not evdf.empty and "event_type" in evdf.columns:
                                mids = sub.get("match_id", pd.Series(dtype=str)).astype(str).unique().tolist()
                                ev_round = evdf[evdf["match_id"].astype(str).isin(mids)]
                                if not ev_round.empty:
                                    ct = ev_round["event_type"].str.lower().value_counts()
                                    yellow = int(ct.get("yellow", 0))
                                    sy     = int(ct.get("second_yellow", 0))
                                    red    = int(ct.get("red", 0))
                                    ypr    = int(ct.get("yellow_plus_direct_red", 0))
                        except Exception:
                            pass

                        import pandas as _pd
                        summary_df = _pd.DataFrame([
                            ("Số trận (vòng này)", n_matches),
                            ("Trận đã có tỉ số", n_played),
                            ("Tổng bàn thắng", goals_tot),
                            ("Bàn chủ nhà", gf_home),
                            ("Bàn đội khách", gf_away),
                            ("TB bàn/trận", f"{avg_goals:.2f}"),
                            ("Chủ nhà thắng", home_wins),
                            ("Đội khách thắng", away_wins),
                            ("Hòa", draws),
                            ("Thẻ vàng", yellow),
                            ("Đỏ gián tiếp (2V)", sy),
                            ("Đỏ trực tiếp", red),
                            ("Vàng + Đỏ trực tiếp", ypr),
                        ], columns=["Chỉ số", f"Vòng {r}"])
                        st.markdown("**Tổng hợp vòng**")
                        st.dataframe(summary_df, use_container_width=True, hide_index=True)
                        st.divider()

        else:
            if show.empty:
                st.info("Không có trận nào khớp bộ lọc.")
            else:
                for _, row in show.iterrows():
                    st.markdown(match_card(row), unsafe_allow_html=True)
                    with st.expander(f"Chi tiết trận {row.get('match_id','')}", expanded=False):
                        render_events_for_match(row)






with tab3:
    left, right = st.columns([2,1])

    # Map team_id -> team_name để hiển thị đẹp
    tdf = teams_df.copy(); tdf.columns = [c.strip().lower() for c in tdf.columns]
    name_map = dict(zip(tdf.get("team_id", pd.Series(dtype=str)),
                        tdf.get("team_name", pd.Series(dtype=str))))

    # ========= BÊN TRÁI: DANH SÁCH CẦU THỦ =========
    # ========= BÊN TRÁI: DANH SÁCH CẦU THỦ (có lọc) =========
    with left:
        st.subheader("Danh sách cầu thủ")
        if players_df.empty:
            st.info("Chưa có dữ liệu 'players'.")
        else:
            pdf = players_df.copy()
            pdf.columns = [c.strip().lower() for c in pdf.columns]

            # Map team_id -> team_name (dùng lại name_map đã tạo phía trên tab3)
            # name_map được tạo ngay trước đó:
            # name_map = dict(zip(tdf.get("team_id", pd.Series(dtype=str)),
            #                     tdf.get("team_name", pd.Series(dtype=str))))
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

            # Lọc theo đội
            if team_pick != "Tất cả":
                show = show[show["Đội"] == team_pick]

            # Tìm nhanh theo tên, số áo, mã cầu thủ
            if q.strip():
                qq = q.strip().lower()
                def s(col):  # helper an toàn
                    return show.get(col, pd.Series(dtype=str)).astype(str).str.lower()
                mask = (
                    s("player_name").str.contains(qq, na=False) |
                    s("shirt_number").str.contains(qq, na=False) |
                    s("player_id").str.contains(qq, na=False)
                )
                show = show[mask]

            # Sắp xếp mặc định theo Đội -> Số áo (nếu có)
            if "shirt_number" in show.columns:
                show["__shirt_num__"] = pd.to_numeric(show["shirt_number"], errors="coerce")
                show = show.sort_values(by=["Đội", "__shirt_num__", "player_name"], na_position="last")
            else:
                show = show.sort_values(by=["Đội", "player_name"])

            # Chọn & đổi tên cột sang tiếng Việt
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

            st.dataframe(display_players.drop(columns=[c for c in ["__shirt_num__"] if c in display_players.columns]),
                         use_container_width=True)


    # ========= BÊN PHẢI: THỐNG KÊ =========
    with right:
        st.subheader("Thống kê ghi bàn / thẻ")
        if events_df.empty:
            st.info("Chưa có dữ liệu 'events'.")
        else:
            ev = events_df.copy()
            ev.columns = [c.strip().lower() for c in ev.columns]

            # Chuẩn kiểu để merge an toàn
            if "player_id" in ev.columns and "player_id" in players_df.columns:
                ev["player_id"] = ev["player_id"].astype(str)
                pmini = players_df.copy()
                pmini.columns = [c.strip().lower() for c in pmini.columns]
                pmini["player_id"] = pmini["player_id"].astype(str)
                pmini["Đội"] = pmini.get("team_id", "").map(name_map).fillna(pmini.get("team_id",""))

                # ==== Top ghi bàn ====
                if "event_type" in ev.columns:
                    goals = ev[ev["event_type"].str.lower() == "goal"]
                    if not goals.empty:
                        top = (goals.groupby("player_id").size()
                               .reset_index(name="Bàn thắng"))
                        top = (pmini.merge(top, how="right", on="player_id")
                                     .rename(columns={
                                         "player_id": "Mã cầu thủ",
                                         "player_name": "Cầu thủ"
                                     })
                               )
                        top = top[["Mã cầu thủ","Cầu thủ","Đội","Bàn thắng"]].sort_values(
                            "Bàn thắng", ascending=False
                        )
                        st.markdown("**Vua phá lưới (tạm tính)**")
                        st.dataframe(top, use_container_width=True)
                    else:
                        st.info("Chưa có bàn thắng nào.")

                                # ==== Thẻ phạt + TIỀN PHẠT theo đội ====
                card_types = ["yellow","red","second_yellow","yellow_plus_direct_red"]
                cards = ev[ev.get("event_type","").isin(card_types)]
                if not cards.empty:
                    # Pivot đếm số thẻ / cầu thủ
                    card_pvt = (cards.pivot_table(index="player_id",
                                                  columns="event_type",
                                                  aggfunc="size",
                                                  fill_value=0)
                                      .reset_index())
                    card_pvt.columns = [str(c) for c in card_pvt.columns]

                    # Merge thông tin cầu thủ + tên đội
                    card_pvt = pmini.merge(card_pvt, how="right", on="player_id")

                    # ----- CẤU HÌNH MỨC PHẠT (đ đơn vị: đồng) -----
                    FINE_YELLOW = 200_000                # thẻ vàng
                    FINE_SECOND_YELLOW = 300_000         # thẻ đỏ gián tiếp (2 vàng)
                    FINE_RED = 500_000                   # thẻ đỏ trực tiếp
                    # TH NOTE: 'yellow_plus_direct_red' không nêu trong điều lệ tiền phạt.
                    # Ở đây mình giả định = Vàng (200k) + Đỏ trực tiếp (500k) = 700k.
                    # Nếu bạn muốn = 500k thôi, đổi FINE_YPR = 500_000 là xong.
                    FINE_YPR = 700_000                   # vàng + đỏ trực tiếp (giả định)

                    # Bảo vệ cột có thể thiếu
                    for c in ["yellow","second_yellow","red","yellow_plus_direct_red"]:
                        if c not in card_pvt.columns:
                            card_pvt[c] = 0

                    # Tính tổng tiền phạt cho từng cầu thủ
                    card_pvt["Tiền phạt"] = (
                        card_pvt["yellow"] * FINE_YELLOW +
                        card_pvt["second_yellow"] * FINE_SECOND_YELLOW +
                        card_pvt["red"] * FINE_RED +
                        card_pvt["yellow_plus_direct_red"] * FINE_YPR
                    )

                    # === BỘ LỌC THEO ĐỘI để xem đội phải nộp bao nhiêu ===
                    teams_list = ["Tất cả"] + sorted(
                        pd.Series(pmini.get("Đội", [])).dropna().unique().tolist()
                    )
                    pick_team = st.selectbox("Lọc thẻ & tiền phạt theo đội", teams_list, key="fine_filter_team")

                    show_fines = card_pvt.copy()
                    if pick_team != "Tất cả":
                        show_fines = show_fines[show_fines.get("Đội","") == pick_team]

                    # Tổng tiền phạt của đội (hoặc toàn giải)
                    total_fine = int(show_fines["Tiền phạt"].sum())
                    if pick_team != "Tất cả":
                        st.markdown(f"**Tổng tiền phạt của đội _{pick_team}_:** `{total_fine:,} đ`")
                    else:
                        st.markdown(f"**Tổng tiền phạt toàn giải:** `{total_fine:,} đ`")

                    # Đổi tên cột cho bảng chi tiết
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

                    # Sắp theo Tiền phạt giảm dần
                    st.markdown("**Thẻ phạt (tạm tính) & Tiền phạt theo cầu thủ**")
                    st.dataframe(
                        show_fines[keep]
                            .sort_values(by="Tiền phạt", ascending=False),
                        use_container_width=True
                    )
                else:
                    st.info("Chưa có sự kiện thẻ nào.")

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

