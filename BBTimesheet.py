# app.py
# Timesheet → Payroll (NZ, Fortnightly) — Streamlit Community Cloud ready
# - Reads an uploaded Excel workbook where each employee is a sheet
# - Robustly detects headers (even if the real header row is not row 1)
# - Supports multiple column name variants (Login/Logout, Clock In/Out, Start/Finish, etc.)
# - Cycle selection works for ANY past/future date using a fixed 14-day anchor cycle
# - Detects overlaps between employees
# - Computes ordinary + public-holiday pay (1.5x if worked) + optional 8% holiday pay
# - Estimates PAYE using annualised IRD bracket method + ACC earners levy (close; not IR340 table lookup)
# - Exports a single Excel file with Summary, Overlaps, and per-employee details
#
# IMPORTANT: Streamlit Cloud should run Python 3.11 (add runtime.txt = "python-3.11")

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
# These are the IRD individual tax brackets from 1 Apr 2025 onwards (annual). (Approx PAYE method)
TAX_BRACKETS_FROM_2025_04_01 = [
    (0.00, 15600.00, 0.105),
    (15600.00, 53500.00, 0.175),
    (53500.00, 78100.00, 0.30),
    (78100.00, 180000.00, 0.33),
    (180000.00, float("inf"), 0.39),
]

# ACC earners levy (1 Apr 2025 to 31 Mar 2026)
ACC_EARNERS_LEVY_RATE = 0.0167
ACC_MAX_EARNINGS = 152790.00

PAY_PERIODS_PER_YEAR = 26  # fortnightly


# -----------------------------
# Utility helpers
# -----------------------------
def _floor2(x: float) -> float:
    return floor(float(x) * 100.0) / 100.0


def _round2(x: float) -> float:
    return float(pd.Series([x]).round(2).iloc[0])


def excel_serial_to_date(x: float) -> date:
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=float(x))).date()


def parse_break_to_minutes(v) -> int:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        if 0 < float(v) < 1:
            return int(round(float(v) * 24 * 60))
        return int(round(float(v)))
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
    Accepts: datetime/time/pandas Timestamp/timedelta, Excel fraction, strings like:
      - "16:30", "16:30:00"
      - "9 AM", "9:00 PM", "09:15 am"
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

        # "9 AM" / "9:15 pm"
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

        # try pandas parser
        try:
            ts = pd.to_datetime(s)
            return ts.to_pydatetime().time()
        except Exception:
            return None

    return None


def parse_duration_to_hours(v) -> Optional[float]:
    """
    For Total Hours fields: supports numeric, Excel fraction, "11:30:00", "11:30"
    """
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
    """Return (cycle_index, cycle_start, cycle_end) for a given date using 14-day cycles."""
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
    """
    Approx PAYE = (annual tax + annual ACC) / 26 (floored to cents).
    If you need cent-perfect matching to Xero, replace this with IR340 table lookup.
    """
    annual_income = float(gross_fortnight) * PAY_PERIODS_PER_YEAR
    tax_a = annual_income_tax(annual_income)
    acc_a = min(annual_income, ACC_MAX_EARNINGS) * ACC_EARNERS_LEVY_RATE
    tax_a = _floor2(tax_a)
    acc_a = _floor2(acc_a)
    paye = (tax_a + acc_a) / PAY_PERIODS_PER_YEAR
    return _floor2(paye)


def get_nz_public_holidays(years: List[int]) -> Dict[date, str]:
    """
    Uses holidays package if available; otherwise returns empty map.
    """
    try:
        import holidays  # type: ignore
        nz = holidays.country_holidays("NZ", years=years)
        return {d: str(name) for d, name in nz.items()}
    except Exception:
        return {}


def find_header_row(df_raw: pd.DataFrame) -> int:
    """
    Attempt to locate the real header row in messy sheets.
    Heuristic: look for a row containing something like 'date' + login/clock in/start + logout/clock out/finish.
    """
    def norm_cell(x) -> str:
        return str(x).strip().lower()

    for i in range(min(len(df_raw), 80)):
        row = df_raw.iloc[i].apply(norm_cell).tolist()
        has_date = any(c == "date" or "date" == c for c in row)
        has_in = any(("login" in c) or ("clock in" in c) or (c == "in") or ("start" in c) for c in row)
        has_out = any(("logout" in c) or ("clock out" in c) or (c == "out") or ("finish" in c) or ("end" in c) for c in row)
        has_total = any(("total" in c and ("hour" in c or "hrs" in c)) or c in ("total hours", "hours") for c in row)
        if has_date and (has_total or (has_in and has_out)):
            return i

    # fallback: first row with at least 2 non-empty cells
    for i in range(min(len(df_raw), 80)):
        if df_raw.iloc[i].notna().sum() >= 2:
            return i
    return 0


def standardise_timesheet_df(df_any: pd.DataFrame) -> pd.DataFrame:
    """
    Returns clean DF with:
      Date (datetime64[ns] date at midnight)
      ShiftStart (datetime)
      ShiftEnd (datetime)
      BreakMinutes (int)
      Hours (float)
    """
    # If columns are mostly Unnamed, treat as raw with header embedded
    if len(df_any.columns) >= 1 and all(str(c).lower().startswith("unnamed") for c in df_any.columns):
        df_raw = df_any.copy()
        hdr_idx = find_header_row(df_raw)
        header = df_raw.iloc[hdr_idx].astype(str).str.strip()
        df = df_raw.iloc[hdr_idx + 1 :].copy()
        df.columns = header
    else:
        df = df_any.copy()

    # Normalize column names with wide synonyms
    col_map = {}
    for c in df.columns:
        cl = str(c).strip().lower()

        # Date
        if cl == "date" or "date" == cl:
            col_map[c] = "Date"
        elif "work date" in cl:
            col_map[c] = "Date"

        # Login / Start
        elif ("login" in cl) or ("clock in" in cl) or (cl in ("in", "in time")) or ("start" in cl) or ("time in" in cl):
            col_map[c] = "Login"

        # Logout / Finish
        elif ("logout" in cl) or ("clock out" in cl) or (cl in ("out", "out time")) or ("finish" in cl) or ("end" in cl) or ("time out" in cl):
            col_map[c] = "Logout"

        # Break
        elif ("break" in cl) or ("meal" in cl) or ("lunch" in cl):
            col_map[c] = "Break"

        # Total Hours
        elif (("total" in cl and ("hour" in cl or "hrs" in cl)) or (cl == "total hours") or (cl == "hours") or ("worked hours" in cl)):
            col_map[c] = "Total Hours"

        else:
            col_map[c] = str(c).strip()

    df = df.rename(columns=col_map)

    # Keep only relevant columns if present
    keep = [c for c in ["Date", "Login", "Logout", "Break", "Total Hours"] if c in df.columns]
    df = df[keep].copy()

    # Drop empty rows
    df = df[df.notna().sum(axis=1) > 0].copy()

    # Parse dates
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
            return excel_serial_to_date(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return pd.to_datetime(s, dayfirst=False).date()
            except Exception:
                return None
        return None

    df["D"] = df["Date"].apply(_to_date)

    # Parse times
    df["Login_t"] = df["Login"].apply(parse_time_value) if "Login" in df.columns else None
    df["Logout_t"] = df["Logout"].apply(parse_time_value) if "Logout" in df.columns else None

    # Break minutes
    df["BreakMinutes"] = df["Break"].apply(parse_break_to_minutes) if "Break" in df.columns else 0

    # Total hours if present
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
            "Date": pd.to_datetime(df["D"]),
            "ShiftStart": start_list,
            "ShiftEnd": end_list,
            "BreakMinutes": df["BreakMinutes"].astype(int),
            "Hours": hours_list,
        }
    )

    # Keep rows where at least Date + (Hours or times) exist
    out = out.dropna(subset=["Date"]).copy()
    # Remove rows with no hours and no times
    out = out[~(out["Hours"].isna() & out["ShiftStart"].isna() & out["ShiftEnd"].isna())].copy()

    return out


def compute_overlaps(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    shifts columns: Employee, ShiftStart, ShiftEnd
    Returns overlaps:
      Date, EmployeeA, EmployeeB, OverlapStart, OverlapEnd, OverlapHours
    """
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

    return pd.DataFrame(rows).sort_values(["Date", "OverlapStart"]) if rows else pd.DataFrame(
        columns=["Date", "EmployeeA", "EmployeeB", "OverlapStart", "OverlapEnd", "OverlapHours"]
    )


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
    holiday_pay_rate: float  # e.g. 0.08
    apply_public_holiday_rules: bool
    tax_code: str  # informational only in this version


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="BB Timesheet → Payroll (NZ, Fortnightly)", layout="wide")

st.title("BB Timesheet → Payroll (NZ, Fortnightly)")
st.caption(
    "Upload an Excel workbook (one sheet per employee). The app parses shifts, computes pay for the selected fortnight, "
    "checks overlaps, and exports a pay summary."
)

# Upload workbook
xls_file = st.file_uploader("Upload timesheet workbook (.xlsx)", type=["xlsx"])
if not xls_file:
    st.info("Upload a timesheet workbook to begin.")
    st.stop()

xls_bytes = xls_file.getvalue()

# Read workbook sheets
try:
    xl = pd.ExcelFile(io.BytesIO(xls_bytes))
    sheetnames = xl.sheet_names
except Exception as e:
    st.error(f"Could not read the Excel file: {e}")
    st.stop()

# Sidebar: cycle selection (supports any past/future date)
st.sidebar.header("Pay Cycle (Fortnight)")

anchor = st.sidebar.date_input(
    "Anchor cycle start date (known fortnight start)",
    value=date(2025, 11, 30),
    help="This must be a real start date of your payroll fortnight. All past/future cycles align to this anchor.",
)

mode = st.sidebar.radio(
    "Cycle selection method",
    ["Pick a date (auto)", "Manual cycle index"],
    index=0,
)

if mode == "Pick a date (auto)":
    any_date = st.sidebar.date_input(
        "Pick any date inside the cycle you want (works for past years too)",
        value=date.today(),
    )
    cycle_index, cycle_start, cycle_end = cycle_from_date(anchor, any_date)
    st.sidebar.write(f"Auto cycle index: **{cycle_index}**")
else:
    cycle_index = st.sidebar.number_input(
        "Cycle index (0 = anchor fortnight)",
        min_value=-5000,
        max_value=5000,
        value=0,
        step=1,
    )
    cycle_start = anchor + timedelta(days=int(cycle_index) * 14)
    cycle_end = cycle_start + timedelta(days=13)

st.sidebar.write(f"**Cycle:** {cycle_start} → {cycle_end} (14 days)")

# Sheet selection (default: all sheets)
st.subheader("1) Select employee sheets")
default_sheets = sheetnames  # safest for older formats
chosen_sheets = st.multiselect("Sheets to include", options=sheetnames, default=default_sheets)

if not chosen_sheets:
    st.warning("Select at least one sheet.")
    st.stop()

# Parse selected sheets
emp_frames: Dict[str, pd.DataFrame] = {}
parse_errors: Dict[str, str] = {}

for sh in chosen_sheets:
    try:
        df_sh = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sh)
        emp = normalise_sheet_name_to_employee(sh)
        df_std = standardise_timesheet_df(df_sh)
        df_std["Employee"] = emp
        emp_frames[emp] = df_std
    except Exception as e:
        parse_errors[sh] = str(e)

if parse_errors:
    with st.expander("Sheet parse warnings (click to expand)"):
        for sh, msg in parse_errors.items():
            st.warning(f"{sh}: {msg}")

if not emp_frames:
    st.error("No usable sheets were parsed. This workbook format may be too different. Use Diagnostics below to inspect.")
    st.stop()

# Combine all shifts
all_shifts = pd.concat(emp_frames.values(), ignore_index=True)

# Add shift timestamps where possible
# (some rows may have Hours only; keep them but overlaps require ShiftStart/ShiftEnd)
all_shifts["ShiftStart"] = pd.to_datetime(all_shifts["ShiftStart"], errors="coerce")
all_shifts["ShiftEnd"] = pd.to_datetime(all_shifts["ShiftEnd"], errors="coerce")
all_shifts["DateOnly"] = all_shifts["Date"].dt.date

# Filter to cycle
mask = (all_shifts["DateOnly"] >= cycle_start) & (all_shifts["DateOnly"] <= cycle_end)
cycle_shifts = all_shifts.loc[mask].copy()

# Public holidays
years_needed = sorted({cycle_start.year, cycle_end.year})
holiday_map = get_nz_public_holidays(years_needed)
cycle_shifts["IsPublicHoliday"] = cycle_shifts["DateOnly"].apply(lambda d: d in holiday_map)
cycle_shifts["HolidayName"] = cycle_shifts["DateOnly"].apply(lambda d: holiday_map.get(d, ""))

# Quick metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Cycle start", str(cycle_start))
c2.metric("Cycle end", str(cycle_end))
c3.metric("Employees parsed", str(len(emp_frames)))
c4.metric("Shift rows in cycle", str(len(cycle_shifts)))

# Diagnostics for “old timesheet didn’t work”
with st.expander("Diagnostics (use if an old timesheet does not parse)"):
    st.write("Chosen sheets:", chosen_sheets)
    st.write("Detected employees:", sorted(emp_frames.keys()))
    st.write("Parsed shifts (first 50 rows):")
    st.dataframe(cycle_shifts.head(50), use_container_width=True)

# Employee settings editor
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

# Compute overlaps (only possible where shift times exist)
overlaps_df = compute_overlaps(
    cycle_shifts[["Employee", "ShiftStart", "ShiftEnd"]].copy()
)

# Compute pay per employee
summary_rows = []
details_by_emp: Dict[str, pd.DataFrame] = {}

# Group even if some rows only have Hours
for emp, grp in cycle_shifts.groupby("Employee"):
    s = settings_map.get(emp, EmpSettings(emp, 0.0, False, 0.08, True, "M"))
    g = grp.copy()

    # If Hours is missing, treat as 0 to avoid NaN sum surprises
    g["Hours"] = pd.to_numeric(g["Hours"], errors="coerce").fillna(0.0)

    # buckets
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

    # detail
    g2 = g.copy()
    g2["PayType"] = np.where(g2["IsPublicHoliday"], "Public Holiday", "Ordinary")
    g2["BasePay"] = np.where(
        g2["IsPublicHoliday"],
        g2["Hours"] * rate * (1.5 if s.apply_public_holiday_rules else 1.0),
        g2["Hours"] * rate,
    )
    g2["BasePay"] = g2["BasePay"].round(2)
    g2["Date"] = g2["Date"].dt.date

    details_by_emp[emp] = g2[
        ["Employee", "Date", "ShiftStart", "ShiftEnd", "BreakMinutes", "Hours", "PayType", "HolidayName", "BasePay"]
    ].sort_values(["Date", "ShiftStart"], na_position="last")

summary_df = pd.DataFrame(summary_rows)

# Guard: old timesheet might parse nothing in a cycle → avoid KeyError
if summary_df.empty:
    st.error(
        "No payable shifts were detected for the selected cycle. "
        "This usually means the old timesheet has different headers or date/time formats."
    )
    st.write("Try:")
    st.write("- Select a different sheet set")
    st.write("- Use Diagnostics to see parsed rows")
    st.write("- Check the anchor date and the picked cycle date")
    st.stop()

summary_df = summary_df.sort_values("Employee (Sheet)")

# Display summary + overlaps
st.subheader("3) Pay run summary (computed)")
st.dataframe(summary_df, use_container_width=True)

st.subheader("4) Overlaps (same time, different employees)")
if overlaps_df.empty:
    st.success("No overlaps detected (or shift times were not available in the old format).")
else:
    st.dataframe(overlaps_df, use_container_width=True)

# Employee details
st.subheader("5) Employee shift details (cycle)")
tabs = st.tabs([emp for emp in sorted(details_by_emp.keys())])
for tab, emp in zip(tabs, sorted(details_by_emp.keys())):
    with tab:
        st.dataframe(details_by_emp[emp], use_container_width=True)

# Export
st.subheader("6) Export")
export_bytes = make_export_excel(summary_df, details_by_emp, overlaps_df)
st.download_button(
    "Download Excel (Pay Summary + Overlaps + Employee Details)",
    data=export_bytes,
    file_name=f"payroll_summary_{cycle_start}_to_{cycle_end}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption(
    "Notes: PAYE is computed using an annualised bracket approximation plus ACC levy. "
    "If you need exact cent-matching with Xero/IRD fortnightly tables, upgrade to IR340 table lookup."
)
