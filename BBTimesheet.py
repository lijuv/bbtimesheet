# app.py
# BB Timesheet → Payroll (NZ, Fortnightly) — Streamlit Community Cloud ready
#
# Key features:
# - Upload Excel workbook; each employee is a sheet
# - Global sheet filter (search + include/exclude regex) so "Daily sales" etc. are excluded
# - Hard validation: only sheets that parse as timesheets become employees
# - Robust header-row detection (headers may not be row 1)
# - Robust column synonym mapping: Login/Logout, Clock In/Out, Start/Finish, Work Date, etc.
# - Robust time parsing: "16:30", "9 AM", Excel fractions, datetimes
# - Cycle selection: choose ANY date (e.g., June 2024) and it auto-computes the fortnight from a fixed anchor
# - Computes pay: Ordinary + Public Holiday (1.5x if worked) + optional Holiday Pay (e.g., 8%)
# - PAYE: annualised bracket approximation + ACC levy (close; not IR340 table lookup)
# - Overlap detection (only when shift start/end times exist)
# - Export: Pay Summary + Overlaps + per-employee details
#
# Recommended for Streamlit Cloud:
# - Add runtime.txt: python-3.11
# - Pin requirements.txt versions

from __future__ import annotations

import io
import re
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from math import floor
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st


# -----------------------------
# NZ PAYROLL CONSTANTS (adjust if needed)
# -----------------------------
TAX_BRACKETS_FROM_2025_04_01 = [
    (0.00, 15600.00, 0.105),
    (15600.00, 53500.00, 0.175),
    (53500.00, 78100.00, 0.30),
    (78100.00, 180000.00, 0.33),
    (180000.00, float("inf"), 0.39),
]
ACC_EARNERS_LEVY_RATE = 0.0167
ACC_MAX_EARNINGS = 152790.00
PAY_PERIODS_PER_YEAR = 26  # fortnightly


# -----------------------------
# Helpers
# -----------------------------
def _floor2(x: float) -> float:
    return floor(float(x) * 100.0) / 100.0


def excel_serial_to_date(x: float) -> date:
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=float(x))).date()


def parse_break_to_minutes(v) -> int:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        vv = float(v)
        if 0 < vv < 1:
            return int(round(vv * 24 * 60))
        return int(round(vv))
    if isinstance(v, timedelta):
        return int(v.total_seconds() // 60)
    if isinstance(v, str):
        s = v.strip().lower()
        if not s:
            return 0
        m = re.search(r"(\d+)\s*(mins?|minutes?|m)\b", s)
        if m:
            return int(m.group(1))
        m = re.match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            return hh * 60 + mm
        try:
            f = float(s)
            if f <= 8:
                return int(round(f * 60))
            return int(round(f))
        except Exception:
            return 0
    return 0


def parse_time_value(v) -> Optional[time]:
    """
    Accepts:
    - time/datetime/pandas Timestamp/timedelta
    - Excel fraction of a day
    - strings: "16:30", "16:30:00", "9 AM", "9:15 pm"
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, time):
        return v
    if isinstance(v, datetime):
        return v.time()
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().time()
    if isinstance(v, timedelta):
        secs = int(v.total_seconds())
        hh = (secs // 3600) % 24
        mm = (secs % 3600) // 60
        ss = secs % 60
        return time(hh, mm, ss)

    if isinstance(v, (int, float, np.integer, np.floating)):
        vv = float(v)
        if 0 <= vv < 1:
            secs = int(round(vv * 24 * 3600))
            hh = (secs // 3600) % 24
            mm = (secs % 3600) // 60
            ss = secs % 60
            return time(hh, mm, ss)
        return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        # HH:MM(:SS)
        m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ss = int(m.group(3) or 0)
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return time(hh, mm, ss)

        # 9 AM / 9:15 pm
        m = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*([aApP][mM])\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2) or 0)
            ap = m.group(3).lower()
            if hh == 12:
                hh = 0
            if ap == "pm":
                hh += 12
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return time(hh, mm, 0)

        # fallback
        try:
            ts = pd.to_datetime(s)
            return ts.to_pydatetime().time()
        except Exception:
            return None

    return None


def parse_duration_to_hours(v) -> Optional[float]:
    """For Total Hours fields: numeric, Excel fraction, '11:30(:00)', '11.5'."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, timedelta):
        return v.total_seconds() / 3600.0
    if isinstance(v, (int, float, np.integer, np.floating)):
        vv = float(v)
        if 0 < vv < 1:
            return vv * 24.0
        return vv
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        m = re.match(r"^(\d{1,3}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ss = int(m.group(3) or 0)
            return hh + mm / 60.0 + ss / 3600.0
        try:
            return float(s)
        except Exception:
            return None
    return None


def normalise_sheet_name_to_employee(sheet_name: str) -> str:
    s = re.sub(r"\s+", " ", str(sheet_name)).strip()
    s = re.sub(r"^(timesheet|time sheet)\s*[-:]*\s*", "", s, flags=re.IGNORECASE).strip()
    return s or str(sheet_name).strip()


def cycle_from_date(anchor: date, any_date: date) -> Tuple[int, date, date]:
    """Compute fortnight cycle for any_date aligned to anchor."""
    delta_days = (any_date - anchor).days
    k = math.floor(delta_days / 14)
    start = anchor + timedelta(days=14 * k)
    end = start + timedelta(days=13)
    return k, start, end


def annual_income_tax(income_annual: float) -> float:
    tax = 0.0
    for lo, hi, rate in TAX_BRACKETS_FROM_2025_04_01:
        if income_annual > lo:
            taxable = min(income_annual, hi) - lo
            tax += taxable * rate
        else:
            break
    return tax


def compute_paye_fortnightly(gross_fortnight: float) -> float:
    """Approx PAYE + ACC levy. Replace with IR340 table for exact Xero matching."""
    annual_income = float(gross_fortnight) * PAY_PERIODS_PER_YEAR
    tax_a = annual_income_tax(annual_income)
    acc_a = min(annual_income, ACC_MAX_EARNINGS) * ACC_EARNERS_LEVY_RATE
    return _floor2((_floor2(tax_a) + _floor2(acc_a)) / PAY_PERIODS_PER_YEAR)


def get_nz_public_holidays(years: List[int]) -> Dict[date, str]:
    """Uses holidays package if installed; else returns empty."""
    try:
        import holidays  # type: ignore

        nz = holidays.country_holidays("NZ", years=years)
        return {d: str(name) for d, name in nz.items()}
    except Exception:
        return {}


def find_header_row(df_raw: pd.DataFrame) -> int:
    """Locate header row in messy sheets."""
    def norm_cell(x) -> str:
        return str(x).strip().lower()

    for i in range(min(len(df_raw), 80)):
        row = df_raw.iloc[i].apply(norm_cell).tolist()
        has_date = any(("date" == c) or ("work date" in c) or (c.startswith("date")) for c in row)
        has_in = any(("login" in c) or ("clock in" in c) or (c in ("in", "in time")) or ("start" in c) for c in row)
        has_out = any(("logout" in c) or ("clock out" in c) or (c in ("out", "out time")) or ("finish" in c) or ("end" in c) for c in row)
        has_total = any(("total" in c and ("hour" in c or "hrs" in c)) or (c in ("total hours", "hours")) for c in row)
        if has_date and (has_total or (has_in and has_out)):
            return i

    for i in range(min(len(df_raw), 80)):
        if df_raw.iloc[i].notna().sum() >= 2:
            return i
    return 0


def standardise_timesheet_df(df_any: pd.DataFrame, dayfirst: bool) -> pd.DataFrame:
    """
    Returns columns:
      Date (datetime)
      ShiftStart (datetime or NaT)
      ShiftEnd (datetime or NaT)
      BreakMinutes (int)
      Hours (float)
    """
    # Detect embedded header row
    if len(df_any.columns) >= 1 and all(str(c).lower().startswith("unnamed") for c in df_any.columns):
        df_raw = df_any.copy()
        hdr_idx = find_header_row(df_raw)
        header = df_raw.iloc[hdr_idx].astype(str).str.strip()
        df = df_raw.iloc[hdr_idx + 1 :].copy()
        df.columns = header
    else:
        df = df_any.copy()

    # Map column names (wide synonyms)
    col_map = {}
    for c in df.columns:
        cl = str(c).strip().lower()

        if cl == "date" or "work date" in cl or cl.startswith("date"):
            col_map[c] = "Date"
        elif ("login" in cl) or ("clock in" in cl) or (cl in ("in", "in time")) or ("start" in cl) or ("time in" in cl):
            col_map[c] = "Login"
        elif ("logout" in cl) or ("clock out" in cl) or (cl in ("out", "out time")) or ("finish" in cl) or ("end" in cl) or ("time out" in cl):
            col_map[c] = "Logout"
        elif ("break" in cl) or ("meal" in cl) or ("lunch" in cl):
            col_map[c] = "Break"
        elif (("total" in cl and ("hour" in cl or "hrs" in cl)) or cl in ("total hours", "hours", "worked hours")):
            col_map[c] = "Total Hours"
        else:
            col_map[c] = str(c).strip()

    df = df.rename(columns=col_map)

    keep = [c for c in ["Date", "Login", "Logout", "Break", "Total Hours"] if c in df.columns]
    df = df[keep].copy()

    df = df[df.notna().sum(axis=1) > 0].copy()

    # Parse Date
    def _to_date(v) -> Optional[date]:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, date) and not isinstance(v, datetime):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime().date()
        if isinstance(v, (int, float, np.integer, np.floating)):
            return excel_serial_to_date(float(v))
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            # Try robust parse
            try:
                return pd.to_datetime(s, dayfirst=dayfirst, errors="raise").date()
            except Exception:
                # strip non-date junk and retry
                s2 = re.sub(r"[^0-9A-Za-z/\- ]", " ", s)
                s2 = re.sub(r"\s+", " ", s2).strip()
                try:
                    return pd.to_datetime(s2, dayfirst=dayfirst, errors="raise").date()
                except Exception:
                    return None
        return None

    df["D"] = df["Date"].apply(_to_date)

    # Parse times
    df["Login_t"] = df["Login"].apply(parse_time_value) if "Login" in df.columns else None
    df["Logout_t"] = df["Logout"].apply(parse_time_value) if "Logout" in df.columns else None

    # Break
    df["BreakMinutes"] = df["Break"].apply(parse_break_to_minutes) if "Break" in df.columns else 0

    # Total hours
    th = df["Total Hours"].apply(parse_duration_to_hours) if "Total Hours" in df.columns else None

    hours_list = []
    start_list = []
    end_list = []

    for i in range(len(df)):
        d = df.iloc[i]["D"]
        lt = df.iloc[i]["Login_t"]
        ot = df.iloc[i]["Logout_t"]
        br = int(df.iloc[i]["BreakMinutes"] or 0)
        total_h = None if th is None else th.iloc[i]

        if d is None:
            hours_list.append(np.nan)
            start_list.append(pd.NaT)
            end_list.append(pd.NaT)
            continue

        shift_start = datetime.combine(d, lt) if lt is not None else pd.NaT

        if ot is not None:
            shift_end = datetime.combine(d, ot)
            if lt is not None and ot < lt:
                shift_end = shift_end + timedelta(days=1)  # overnight
        else:
            shift_end = pd.NaT

        # Prefer Total Hours if present and parseable; otherwise compute using times minus break
        if total_h is not None and not (isinstance(total_h, float) and np.isnan(total_h)):
            hrs = float(total_h)
        elif lt is not None and ot is not None:
            diff = (shift_end - shift_start).total_seconds() / 3600.0
            hrs = max(diff - (br / 60.0), 0.0)
        else:
            hrs = np.nan

        hours_list.append(hrs)
        start_list.append(shift_start)
        end_list.append(shift_end)

    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(df["D"], errors="coerce"),
            "ShiftStart": pd.to_datetime(start_list, errors="coerce"),
            "ShiftEnd": pd.to_datetime(end_list, errors="coerce"),
            "BreakMinutes": pd.Series(df["BreakMinutes"]).fillna(0).astype(int),
            "Hours": pd.to_numeric(hours_list, errors="coerce"),
        }
    )

    # Keep only rows with valid date
    out = out.dropna(subset=["Date"]).copy()

    return out


def compute_overlaps(shifts: pd.DataFrame) -> pd.DataFrame:
    """Compute overlaps only where ShiftStart/ShiftEnd exist."""
    if shifts.empty:
        return pd.DataFrame(columns=["Date", "EmployeeA", "EmployeeB", "OverlapStart", "OverlapEnd", "OverlapHours"])

    s = shifts.dropna(subset=["ShiftStart", "ShiftEnd"]).copy()
    if s.empty:
        return pd.DataFrame(columns=["Date", "EmployeeA", "EmployeeB", "OverlapStart", "OverlapEnd", "OverlapHours"])

    s["DateKey"] = s["ShiftStart"].dt.date
    rows = []

    for d, grp in s.groupby("DateKey"):
        grp = grp.sort_values("ShiftStart")
        arr = grp.to_dict("records")
        for i in range(len(arr)):
            for j in range(i + 1, len(arr)):
                a = arr[i]
                b = arr[j]
                if a["Employee"] == b["Employee"]:
                    continue
                start = max(a["ShiftStart"], b["ShiftStart"])
                end = min(a["ShiftEnd"], b["ShiftEnd"])
                if end > start:
                    oh = (end - start).total_seconds() / 3600.0
                    rows.append(
                        {
                            "Date": d,
                            "EmployeeA": a["Employee"],
                            "EmployeeB": b["Employee"],
                            "OverlapStart": start,
                            "OverlapEnd": end,
                            "OverlapHours": round(oh, 2),
                        }
                    )

    if not rows:
        return pd.DataFrame(columns=["Date", "EmployeeA", "EmployeeB", "OverlapStart", "OverlapEnd", "OverlapHours"])
    return pd.DataFrame(rows).sort_values(["Date", "OverlapStart"])


def make_export_excel(summary_df: pd.DataFrame, details_by_emp: Dict[str, pd.DataFrame], overlaps_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Pay Summary")
        overlaps_df.to_excel(writer, index=False, sheet_name="Overlaps")
        for emp, d in details_by_emp.items():
            sheet = re.sub(r"[\[\]\*:/\\\?]", " ", emp)[:31] or "Employee"
            d.to_excel(writer, index=False, sheet_name=sheet)
    return output.getvalue()


@dataclass
class EmpSettings:
    payroll_name: str
    hourly_rate: float
    apply_holiday_pay: bool
    holiday_pay_rate: float
    apply_public_holiday_rules: bool
    tax_code: str


def _is_valid_timesheet(df_std: pd.DataFrame) -> bool:
    """
    A sheet counts as an employee timesheet if:
      - Has at least 1 valid Date
      - AND has either:
         (a) positive Hours total, OR
         (b) at least 1 row with both ShiftStart and ShiftEnd
    """
    if df_std is None or df_std.empty:
        return False
    has_dates = df_std["Date"].notna().sum() > 0
    total_hours = pd.to_numeric(df_std.get("Hours", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
    has_hours = total_hours > 0
    has_times = df_std["ShiftStart"].notna().sum() > 0 and df_std["ShiftEnd"].notna().sum() > 0
    return bool(has_dates and (has_hours or has_times))


# -----------------------------
# Streamlit App
# -----------------------------
st.set_page_config(page_title="BB Timesheet → Payroll (NZ, Fortnightly)", layout="wide")

st.title("BB Timesheet → Payroll (NZ, Fortnightly)")
st.caption(
    "Upload an Excel workbook (one sheet per employee). The app parses shifts, computes pay for the selected fortnight, "
    "checks overlaps, and exports a pay summary."
)

# Upload
xls_file = st.file_uploader("Upload timesheet workbook (.xlsx)", type=["xlsx"])
if not xls_file:
    st.info("Upload a timesheet workbook to begin.")
    st.stop()

xls_bytes = xls_file.getvalue()

try:
    xl = pd.ExcelFile(io.BytesIO(xls_bytes))
    sheetnames = xl.sheet_names
except Exception as e:
    st.error(f"Could not read the Excel file: {e}")
    st.stop()

# Sidebar: parsing preferences (old files often require day-first)
st.sidebar.header("Parsing Options")
dayfirst = st.sidebar.checkbox(
    "Day-first dates (NZ style) e.g. 13/12/2024",
    value=True,
    help="Turn this on if your old timesheets store dates as DD/MM/YYYY.",
)

# Sidebar: Pay cycle selection
st.sidebar.header("Pay Cycle (Fortnight)")

anchor = st.sidebar.date_input(
    "Anchor cycle start date (known fortnight start)",
    value=date(2025, 11, 30),
    help="All cycles (past/future) align to this anchor. If anchor is wrong by 1 day, every cycle will shift.",
)

mode = st.sidebar.radio("Cycle selection method", ["Pick a date (auto)", "Manual cycle index"], index=0)

if mode == "Pick a date (auto)":
    any_date = st.sidebar.date_input(
        "Pick any date inside the cycle you want (works for past years too)",
        value=date.today(),
    )
    cycle_index, cycle_start, cycle_end = cycle_from_date(anchor, any_date)
    st.sidebar.write(f"Auto cycle index: **{cycle_index}**")
else:
    cycle_index = st.sidebar.number_input("Cycle index (0 = anchor)", min_value=-5000, max_value=5000, value=0, step=1)
    cycle_start = anchor + timedelta(days=int(cycle_index) * 14)
    cycle_end = cycle_start + timedelta(days=13)

st.sidebar.write(f"**Cycle:** {cycle_start} → {cycle_end} (14 days)")

# -----------------------------
# Global sheet filters (prevents Daily sales etc.)
# -----------------------------
st.subheader("1) Select employee sheets (global filter)")

with st.expander("Sheet filter controls", expanded=True):
    colA, colB = st.columns([2, 2])
    with colA:
        sheet_search = st.text_input(
            "Search sheets (case-insensitive substring)",
            value="",
            placeholder="e.g. time, gaurav, saurav",
        )
        include_regex = st.text_input(
            "Include regex (optional)",
            value="",
            placeholder=r"e.g. (?i)timesheet|time\s*sheet|gaurav|saurav",
        )
    with colB:
        default_exclude_regex = r"(?i)daily\s*sales|sales|summary|config|setup|template|instructions?|roster|report|dashboard|pays?lip|export|data|lookup|mapping"
        auto_exclude = st.checkbox("Auto-exclude common non-employee sheets", value=True)
        exclude_regex = st.text_input(
            "Exclude regex (optional)",
            value=default_exclude_regex if auto_exclude else "",
            help="Sheets matching this regex are removed from selection.",
        )

    def apply_filters(names: List[str]) -> List[str]:
        out = names[:]

        if sheet_search.strip():
            ss = sheet_search.strip().lower()
            out = [n for n in out if ss in n.lower()]

        if include_regex.strip():
            try:
                ir = re.compile(include_regex)
                out = [n for n in out if ir.search(n)]
            except re.error:
                st.warning("Include regex is invalid. Ignoring it.")

        if exclude_regex.strip():
            try:
                er = re.compile(exclude_regex)
                out = [n for n in out if not er.search(n)]
            except re.error:
                st.warning("Exclude regex is invalid. Ignoring it.")

        return out

    filtered_sheetnames = apply_filters(sheetnames)

    likely_pat = re.compile(r"(?i)\b(timesheet|time\s*sheet)\b")
    likely_timesheet_sheets = [s for s in filtered_sheetnames if likely_pat.search(s)]

    c1, c2, c3 = st.columns([1, 1, 2])
    if c1.button("Select ALL (filtered)"):
        st.session_state["chosen_sheets"] = filtered_sheetnames
    if c2.button("Select NONE"):
        st.session_state["chosen_sheets"] = []
    if c3.button("Select ONLY likely timesheets"):
        st.session_state["chosen_sheets"] = likely_timesheet_sheets

chosen_default = st.session_state.get("chosen_sheets", likely_timesheet_sheets or filtered_sheetnames)

chosen_sheets = st.multiselect(
    "Sheets to include (after filters)",
    options=filtered_sheetnames,
    default=chosen_default,
)

if not chosen_sheets:
    st.warning("Select at least one sheet.")
    st.stop()

# -----------------------------
# Parse selected sheets; accept only valid timesheets as employees
# -----------------------------
emp_frames: Dict[str, pd.DataFrame] = {}
parse_errors: Dict[str, str] = {}
skipped_non_timesheets: List[str] = []
sheet_stats: List[Dict[str, object]] = []

for sh in chosen_sheets:
    try:
        df_sh = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sh)
        df_std = standardise_timesheet_df(df_sh, dayfirst=dayfirst)

        ok = _is_valid_timesheet(df_std)
        total_hours = pd.to_numeric(df_std.get("Hours", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()

        sheet_stats.append(
            {
                "Sheet": sh,
                "RowsParsed": int(len(df_std)),
                "ValidDates": int(df_std["Date"].notna().sum()) if "Date" in df_std.columns else 0,
                "HasStartEnd": bool(df_std["ShiftStart"].notna().sum() > 0 and df_std["ShiftEnd"].notna().sum() > 0),
                "TotalHours": float(round(total_hours, 2)),
                "AcceptedAsEmployee": ok,
            }
        )

        if not ok:
            skipped_non_timesheets.append(sh)
            continue

        emp = normalise_sheet_name_to_employee(sh)
        df_std["Employee"] = emp
        emp_frames[emp] = df_std

    except Exception as e:
        parse_errors[sh] = str(e)

# Show parsing summary
with st.expander("Parsing summary (what got accepted vs skipped)", expanded=False):
    if sheet_stats:
        st.dataframe(pd.DataFrame(sheet_stats).sort_values(["AcceptedAsEmployee", "Sheet"], ascending=[False, True]), use_container_width=True)
    if skipped_non_timesheets:
        st.write("Skipped (not timesheets):", skipped_non_timesheets)
    if parse_errors:
        st.write("Errors:")
        for sh, msg in parse_errors.items():
            st.warning(f"{sh}: {msg}")

if not emp_frames:
    st.error(
        "No employee timesheets were detected after filtering + validation. "
        "Use the parsing summary above to see why sheets were skipped."
    )
    st.stop()

# Combine all shifts
all_shifts = pd.concat(emp_frames.values(), ignore_index=True)
all_shifts["DateOnly"] = all_shifts["Date"].dt.date

# Filter to cycle
mask = (all_shifts["DateOnly"] >= cycle_start) & (all_shifts["DateOnly"] <= cycle_end)
cycle_shifts = all_shifts.loc[mask].copy()

# Public holidays
years_needed = sorted({cycle_start.year, cycle_end.year})
holiday_map = get_nz_public_holidays(years_needed)
cycle_shifts["IsPublicHoliday"] = cycle_shifts["DateOnly"].apply(lambda d: d in holiday_map)
cycle_shifts["HolidayName"] = cycle_shifts["DateOnly"].apply(lambda d: holiday_map.get(d, ""))

# Top metrics
m1, m2, m3, m4 = st.columns(4)
m1.metric("Cycle start", str(cycle_start))
m2.metric("Cycle end", str(cycle_end))
m3.metric("Employees accepted", str(len(emp_frames)))
m4.metric("Shift rows in cycle", str(len(cycle_shifts)))

# Diagnostics for old timesheets
with st.expander("Diagnostics (use this if an old timesheet says 'No payable shifts')", expanded=False):
    st.write("If cycle rows = 0, it means either:")
    st.write("- Dates did not parse (often day-first issue), or")
    st.write("- You selected the wrong anchor/cycle date, or")
    st.write("- The old sheet headers are not being mapped, producing empty Hours.")
    st.write("Preview of parsed rows in the selected cycle (first 50):")
    st.dataframe(cycle_shifts.head(50), use_container_width=True)

# Settings table
st.subheader("2) Enter employee settings (rates and rules)")

default_rows = []
for emp in sorted(emp_frames.keys()):
    default_rows.append(
        {
            "Sheet/Employee": emp,
            "Payroll Name": emp,
            "Hourly Rate": 0.0,
            "Apply Holiday Pay (8%)": False,
            "Holiday Pay Rate": 0.08,
            "Apply Public Holiday Rules (1.5x if worked)": True,
            "Tax Code": "M",
        }
    )

settings_df = st.data_editor(
    pd.DataFrame(default_rows),
    hide_index=True,
    use_container_width=True,
    column_config={
        "Hourly Rate": st.column_config.NumberColumn(min_value=0.0, step=0.5, format="%.2f"),
        "Holiday Pay Rate": st.column_config.NumberColumn(min_value=0.0, max_value=0.2, step=0.01, format="%.2f"),
        "Apply Holiday Pay (8%)": st.column_config.CheckboxColumn(),
        "Apply Public Holiday Rules (1.5x if worked)": st.column_config.CheckboxColumn(),
    },
)

settings_map: Dict[str, EmpSettings] = {}
for _, r in settings_df.iterrows():
    settings_map[str(r["Sheet/Employee"])] = EmpSettings(
        payroll_name=str(r["Payroll Name"]),
        hourly_rate=float(r["Hourly Rate"] or 0.0),
        apply_holiday_pay=bool(r["Apply Holiday Pay (8%)"]),
        holiday_pay_rate=float(r["Holiday Pay Rate"] or 0.0),
        apply_public_holiday_rules=bool(r["Apply Public Holiday Rules (1.5x if worked)"]),
        tax_code=str(r["Tax Code"] or "M"),
    )

missing_rates = [emp for emp, s in settings_map.items() if s.hourly_rate <= 0]
if missing_rates:
    st.warning("Set Hourly Rate for: " + ", ".join(missing_rates))

# IMPORTANT: This is the exact place your previous error came from:
# If cycle_shifts is empty (or has Hours=0 everywhere), summary_rows becomes empty.
# That produces the message: "No payable shifts were detected..."
if cycle_shifts.empty or pd.to_numeric(cycle_shifts["Hours"], errors="coerce").fillna(0).sum() == 0:
    st.error(
        "No payable shifts were detected for the selected cycle. "
        "This usually means the old timesheet has different headers or date/time formats, "
        "or the cycle (anchor/date) selection does not match the timesheet dates."
    )
    st.info("Action checklist:")
    st.write("1) Turn ON/OFF 'Day-first dates' in the sidebar and re-upload.")
    st.write("2) Confirm Anchor cycle start is correct.")
    st.write("3) Pick a date inside the desired cycle (e.g., June 2024) and re-check cycle rows.")
    st.write("4) Open 'Parsing summary' to see whether the sheet was accepted and whether TotalHours > 0.")
    st.stop()

# Overlaps
overlaps_df = compute_overlaps(cycle_shifts[["Employee", "ShiftStart", "ShiftEnd"]].copy())

# Compute pay per employee
summary_rows = []
details_by_emp: Dict[str, pd.DataFrame] = {}

for emp, grp in cycle_shifts.groupby("Employee"):
    s = settings_map.get(emp, EmpSettings(emp, 0.0, False, 0.08, True, "M"))
    g = grp.copy()

    g["Hours"] = pd.to_numeric(g["Hours"], errors="coerce").fillna(0.0)

    ph_hours = float(g.loc[g["IsPublicHoliday"], "Hours"].sum())
    normal_hours = float(g.loc[~g["IsPublicHoliday"], "Hours"].sum())

    rate = float(s.hourly_rate)
    ordinary_pay = normal_hours * rate
    public_holiday_pay = ph_hours * rate * (1.5 if s.apply_public_holiday_rules else 1.0)
    holiday_pay = (ordinary_pay + public_holiday_pay) * float(s.holiday_pay_rate) if s.apply_holiday_pay else 0.0

    gross = ordinary_pay + public_holiday_pay + holiday_pay
    paye = compute_paye_fortnightly(gross)
    take_home = gross - paye

    days_paid = int((g["Hours"] > 0).sum())

    summary_rows.append(
        {
            "Employee (Sheet)": emp,
            "Employee (Payroll)": s.payroll_name,
            "Days Paid": days_paid,
            "Ordinary Hours": round(normal_hours, 2),
            "Public Holiday Hours": round(ph_hours, 2),
            "Hourly Rate": round(rate, 2),
            "Ordinary Pay": round(ordinary_pay, 2),
            "Public Holiday Pay": round(public_holiday_pay, 2),
            "Holiday Pay": round(holiday_pay, 2),
            "Gross Pay": round(gross, 2),
            "PAYE (approx)": round(paye, 2),
            "Take Home": round(take_home, 2),
            "Tax Code": s.tax_code,
        }
    )

    g2 = g.copy()
    g2["PayType"] = np.where(g2["IsPublicHoliday"], "Public Holiday", "Ordinary")
    g2["BasePay"] = np.where(
        g2["IsPublicHoliday"],
        g2["Hours"] * rate * (1.5 if s.apply_public_holiday_rules else 1.0),
        g2["Hours"] * rate,
    ).round(2)
    g2["Date"] = g2["Date"].dt.date

    details_by_emp[emp] = g2[
        ["Employee", "Date", "ShiftStart", "ShiftEnd", "BreakMinutes", "Hours", "PayType", "HolidayName", "BasePay"]
    ].sort_values(["Date", "ShiftStart"], na_position="last")

summary_df = pd.DataFrame(summary_rows).sort_values("Employee (Sheet)")

st.subheader("3) Pay run summary (computed)")
st.dataframe(summary_df, use_container_width=True)

st.subheader("4) Overlaps (same time, different employees)")
if overlaps_df.empty:
    st.success("No overlaps detected (or shift start/end times were not available in the timesheet).")
else:
    st.dataframe(overlaps_df, use_container_width=True)

st.subheader("5) Employee shift details (cycle)")
tabs = st.tabs([emp for emp in sorted(details_by_emp.keys())])
for tab, emp in zip(tabs, sorted(details_by_emp.keys())):
    with tab:
        st.dataframe(details_by_emp[emp], use_container_width=True)

st.subheader("6) Export")
export_bytes = make_export_excel(summary_df, details_by_emp, overlaps_df)
st.download_button(
    "Download Excel (Pay Summary + Overlaps + Employee Details)",
    data=export_bytes,
    file_name=f"payroll_summary_{cycle_start}_to_{cycle_end}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption(
    "Note: PAYE here is computed using an annualised bracket approximation + ACC levy. "
    "If you require exact cent-matching with Xero, implement IR340 fortnightly table lookup."
)
