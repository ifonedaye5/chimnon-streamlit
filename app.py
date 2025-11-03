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
st.title("Giải Chim Non Lần 2 — League Manager")

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
        evdf = events_df.copy(); evdf.columns = [c.strip().lower() for c in evdf.columns]

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
            view_mode = st.selectbox("Chế độ hiển thị", ["Tách theo vòng", "Gộp tất cả"])
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
                <div class='team' style='justify-content:flex-start;'>{home}</div>
                <div class='score'>{score_html}</div>
                <div class='team' style='justify-content:flex-end; text-align:right;'>{away}</div>
              </div>
              <div class='sub'>{meta} {status_html}</div>
            </div>
            """

        # ====== Helpers: dựng danh sách sự kiện theo đội ======
        def format_event_item(ev: dict) -> str:
            # icon theo loại sự kiện
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
            # fallback nếu thiếu tên
            if not pname:
                pname = ev.get("player_name", pid)

            # hiển thị "số áo. tên (phút)"
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

            # Lọc theo match_id
            ev = evdf[evdf["match_id"].astype(str) == str(mid)].copy()
            if ev.empty:
                st.info("Chưa ghi nhận sự kiện nào.")
                return

            # Ép kiểu phút để sắp xếp
            ev["__min"] = pd.to_numeric(ev.get("minute"), errors="coerce")
            ev = ev.sort_values(["__min", "event_type"], na_position="last")

            # Chia 2 cột theo đội
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

                # ====== Hiển thị ======
        if view_mode == "Tách theo vòng":
            if show.empty:
                st.info("Không có trận nào khớp bộ lọc.")
            else:
                rounds = sorted(pd.Series(show.get("round", [])).dropna().unique().tolist())
                for r in rounds:
                    sub = show[show.get("round", "") == r].copy()
                    st.markdown(f"### Vòng {r}")

                    # --- Hiển thị từng trận ---
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

                    # --- Thẻ theo vòng ---
                    yellow = sy = red = ypr = 0
                    try:
                        if not evdf.empty and "match_id" in evdf.columns:
                            mids = sub.get("match_id", pd.Series(dtype=str)).astype(str).unique().tolist()
                            ev_round = evdf[evdf["match_id"].astype(str).isin(mids)]
                            if not ev_round.empty and "event_type" in ev_round.columns:
                                ct = ev_round["event_type"].str.lower().value_counts()
                                yellow = int(ct.get("yellow", 0))
                                sy     = int(ct.get("second_yellow", 0))
                                red    = int(ct.get("red", 0))
                                ypr    = int(ct.get("yellow_plus_direct_red", 0))
                    except Exception:
                        pass

                    summary_rows = [
                        ("Số trận (vòng này)",      n_matches),
                        ("Trận đã có tỉ số",        n_played),
                        ("Tổng bàn thắng",          goals_tot),
                        ("Bàn chủ nhà",             gf_home),
                        ("Bàn đội khách",           gf_away),
                        ("TB bàn/trận",             f"{avg_goals:.2f}"),
                        ("Chủ nhà thắng",           home_wins),
                        ("Đội khách thắng",         away_wins),
                        ("Hòa",                     draws),
                        ("Thẻ vàng",                yellow),
                        ("Đỏ gián tiếp (2V)",       sy),
                        ("Đỏ trực tiếp",            red),
                        ("Vàng + Đỏ trực tiếp",     ypr),
                    ]
                    import pandas as _pd
                    summary_df = _pd.DataFrame(summary_rows, columns=["Chỉ số", f"Vòng {r}"])

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


