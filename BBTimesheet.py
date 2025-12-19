from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from math import floor
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------
# NZ PAYROLL CONSTANTS (2025/26)
# -----------------------------
# IRD "From 1 April 2025" individual tax brackets. (Applies to 2025/26 tax year.)
# Source: IRD tax rates for individuals (from 1 April 2025). See citations in assistant message.
TAX_BRACKETS_FROM_2025_04_01 = [
    (0.00, 15600.00, 0.105),
    (15600.00, 53500.00, 0.175),
    (53500.00, 78100.00, 0.30),
    (78100.00, 180000.00, 0.33),
    (180000.00, float("inf"), 0.39),
]

# ACC earners' levy (includes GST) and maximum liable earnings for 1 Apr 2025 to 31 Mar 2026.
ACC_EARNERS_LEVY_RATE = 0.0167
ACC_MAX_EARNINGS = 152790.00

# Fortnightly pay frequency
PAY_PERIODS_PER_YEAR = 26


# -----------------------------
# Helpers
# -----------------------------
def _floor2(x: float) -> float:
    """Floor to cents (conservative)."""
    return floor(x * 100.0) / 100.0


def _round2(x: float) -> float:
    return float(pd.Series([x]).round(2).iloc[0])


def excel_serial_to_date(x: float) -> date:
    # Excel serial date origin (Windows): 1899-12-30
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=float(x))).date()


def parse_break_to_minutes(v) -> int:
    """Break column examples: NaN, 30, '30 mins', '00:30:00'."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        # if it's a fraction (Excel time), treat as days
        if 0 < v < 1:
            return int(round(v * 24 * 60))
        return int(round(v))
    if isinstance(v, timedelta):
        return int(v.total_seconds() // 60)
    if isinstance(v, str):
        s = v.strip().lower()
        if not s:
            return 0
        # "30 mins", "30 min", "30m"
        m = re.search(r"(\d+)\s*(mins?|minutes?|m)\b", s)
        if m:
            return int(m.group(1))
        # "00:30:00"
        m = re.match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            return hh * 60 + mm
        # "0.5" hours maybe
        try:
            f = float(s)
            # treat as minutes if big; else hours
            if f <= 8:
                return int(round(f * 60))
            return int(round(f))
        except Exception:
            return 0
    return 0


def parse_time_value(v) -> Optional[time]:
    """Accepts time, datetime, string, excel fractions."""
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
        # Excel time fraction of a day
        if 0 <= float(v) < 1:
            secs = int(round(float(v) * 24 * 3600))
            hh = (secs // 3600) % 24
            mm = (secs % 3600) // 60
            ss = secs % 60
            return time(hh, mm, ss)
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ss = int(m.group(3) or 0)
            return time(hh, mm, ss)
    return None


def parse_duration_to_hours(v) -> Optional[float]:
    """For Total Hours column like '11:30:00' or numeric hours."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, timedelta):
        return v.total_seconds() / 3600.0
    if isinstance(v, (int, float, np.integer, np.floating)):
        # If it looks like an Excel time fraction, convert to hours
        if 0 < float(v) < 1:
            return float(v) * 24.0
        # else assume already hours
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # HH:MM:SS
        m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ss = int(m.group(3) or 0)
            return hh + mm / 60.0 + ss / 3600.0
        # "5.5" etc
        try:
            return float(s)
        except Exception:
            return None
    return None


def normalise_sheet_name_to_employee(sheet_name: str) -> str:
    s = re.sub(r"\s+", " ", sheet_name).strip()
    # remove common prefixes
    s = re.sub(r"^(timesheet|time sheet)\s*[-:]*\s*", "", s, flags=re.IGNORECASE).strip()
    return s or sheet_name.strip()


def find_header_row(df_raw: pd.DataFrame) -> int:
    """Find row index containing 'Date' and 'Login' tokens."""
    for i in range(min(len(df_raw), 60)):
        row = df_raw.iloc[i].astype(str).str.strip().str.lower()
        if any(x == "date" for x in row.values) and any("login" in x for x in row.values):
            return i
    # fallback: first non-empty row
    for i in range(min(len(df_raw), 60)):
        if df_raw.iloc[i].notna().sum() >= 2:
            return i
    return 0


def standardise_timesheet_df(df_any: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a clean DF with columns:
    - EmployeeDate (date)
    - Login (time)
    - Logout (time)
    - BreakMinutes (int)
    - Hours (float)
    - ShiftStart (datetime)
    - ShiftEnd (datetime)
    """
    # If columns are all Unnamed, treat as raw with header rows inside.
    if all(str(c).lower().startswith("unnamed") for c in df_any.columns):
        df_raw = df_any.copy()
        hdr_idx = find_header_row(df_raw)
        header = df_raw.iloc[hdr_idx].astype(str).str.strip()
        df = df_raw.iloc[hdr_idx + 1 :].copy()
        df.columns = header
    else:
        df = df_any.copy()

    # Normalise column names
    col_map = {}
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "date":
            col_map[c] = "Date"
        elif "login" in cl or "clock in" in cl or cl == "in":
            col_map[c] = "Login"
        elif "logout" in cl or "clock out" in cl or cl == "out":
            col_map[c] = "Logout"
        elif "break" in cl:
            col_map[c] = "Break"
        elif "total" in cl and "hour" in cl:
            col_map[c] = "Total Hours"
        elif cl == "total hours":
            col_map[c] = "Total Hours"
        elif "hours" == cl:
            col_map[c] = "Total Hours"
        else:
            col_map[c] = str(c)

    df = df.rename(columns=col_map)

    # Keep relevant columns if present
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
            # excel serial
            return excel_serial_to_date(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return pd.to_datetime(s).date()
            except Exception:
                return None
        return None

    df["EmployeeDate"] = df["Date"].apply(_to_date)

    # Parse times
    df["Login_t"] = df["Login"].apply(parse_time_value) if "Login" in df.columns else None
    df["Logout_t"] = df["Logout"].apply(parse_time_value) if "Logout" in df.columns else None

    # Break
    df["BreakMinutes"] = df["Break"].apply(parse_break_to_minutes) if "Break" in df.columns else 0

    # Hours calculation
    total_hours = df["Total Hours"].apply(parse_duration_to_hours) if "Total Hours" in df.columns else None

    hours_list = []
    start_list = []
    end_list = []

    for i in range(len(df)):
        d = df.iloc[i]["EmployeeDate"]
        lt = df.iloc[i]["Login_t"]
        ot = df.iloc[i]["Logout_t"]
        br = int(df.iloc[i]["BreakMinutes"] or 0)
        th = None if total_hours is None else total_hours.iloc[i]

        if d is None:
            hours_list.append(np.nan)
            start_list.append(pd.NaT)
            end_list.append(pd.NaT)
            continue

        # shift start/end
        if lt is not None:
            shift_start = datetime.combine(d, lt)
        else:
            shift_start = pd.NaT

        if ot is not None:
            shift_end = datetime.combine(d, ot)
            # handle overnight (logout < login)
            if lt is not None and ot < lt:
                shift_end = shift_end + timedelta(days=1)
        else:
            shift_end = pd.NaT

        # choose hours source:
        # - if Total Hours is present and parseable, trust it (assume already accounts for breaks)
        # - else compute diff between logout/login minus break
        if th is not None and not (isinstance(th, float) and np.isnan(th)):
            hrs = float(th)
        elif lt is not None and ot is not None:
            diff = (shift_end - shift_start).total_seconds() / 3600.0
            hrs = max(diff - (br / 60.0), 0.0)
        else:
            hrs = np.nan

        hours_list.append(hrs)
        start_list.append(shift_start)
        end_list.append(shift_end)

    df["Hours"] = hours_list
    df["ShiftStart"] = start_list
    df["ShiftEnd"] = end_list

    out = df[["EmployeeDate", "ShiftStart", "ShiftEnd", "BreakMinutes", "Hours"]].copy()
    out = out.rename(columns={"EmployeeDate": "Date"})
    out = out.dropna(subset=["Date"]).copy()
    out["Date"] = pd.to_datetime(out["Date"])
    return out


def get_nz_public_holidays(years: List[int]) -> Dict[date, str]:
    """
    Uses holidays package if available. Returns map {date: name}.
    """
    try:
        import holidays  # type: ignore
        nz = holidays.country_holidays("NZ", years=years)
        return {d: str(name) for d, name in nz.items()}
    except Exception:
        return {}


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
    PAYE ≈ (annual income tax + annual ACC levy) / 26 with conservative cent-flooring.
    """
    annual_income = gross_fortnight * PAY_PERIODS_PER_YEAR
    tax_a = annual_income_tax(annual_income)
    acc_a = min(annual_income, ACC_MAX_EARNINGS) * ACC_EARNERS_LEVY_RATE

    # conservative flooring to cents on annual components
    tax_a = _floor2(tax_a)
    acc_a = _floor2(acc_a)

    paye = (tax_a + acc_a) / PAY_PERIODS_PER_YEAR
    return _floor2(paye)


@dataclass
class EmpSettings:
    payroll_name: str
    hourly_rate: float
    apply_holiday_pay: bool
    holiday_pay_rate: float  # e.g. 0.08
    apply_public_holiday_rules: bool
    tax_code: str  # currently informational
    # future: KiwiSaver, student loan, etc.


def parse_pay_summary_pdf(pdf_bytes: bytes) -> Dict[str, Dict[str, float]]:
    """
    Very lightweight parser for your processed pay summary PDF to support validation/QA.
    Returns:
      {
        "Employee Name": {"Gross": ..., "PAYE": ..., "TakeHome": ..., "Days": ..., "OrdHours": ..., "Rate": ..., "HolidayPay": ...}
      }
    """
    out: Dict[str, Dict[str, float]] = {}
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        return out

    # Normalize spaces
    t = re.sub(r"[ \t]+", " ", text)

    # Employee blocks: Name then numbers, depends on formatting.
    # We'll specifically extract patterns visible in your PDF.
    # Example: "Gaurav Sachin Kolwankar 1,231.20 1,037.20 8"
    emp_header_pat = re.compile(r"([A-Za-z][A-Za-z ]+?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+(\d+)")
    for m in emp_header_pat.finditer(t):
        name = m.group(1).strip()
        gross = float(m.group(2).replace(",", ""))
        take = float(m.group(3).replace(",", ""))
        days = float(m.group(4))
        out[name] = {"Gross": gross, "TakeHome": take, "Days": days}

    # Ordinary hours lines: "Ordinary Hours 47.50 hours 24.00 1,140.00"
    ord_pat = re.compile(r"Ordinary Hours\s+(\d+(?:\.\d+)?)\s+hours\s+(\d+(?:\.\d+)?)\s+([\d,]+\.\d{2})")
    ord_matches = ord_pat.findall(t)

    # Holiday pay line: "Holiday Pay 8% 91.20"
    hol_pat = re.compile(r"Holiday Pay\s+(\d+)%\s+(\d+(?:\.\d+)?)")

    # PAYE line: "PAYE (194.00)"
    paye_pat = re.compile(r"PAYE\s+\((\d+(?:\.\d+)?)\)")

    # We attach these sequentially per employee appearance (best-effort).
    emp_names = list(out.keys())
    emp_idx = 0

    for oh, rate, total in ord_matches:
        if emp_idx >= len(emp_names):
            break
        nm = emp_names[emp_idx]
        out[nm]["OrdHours"] = float(oh)
        out[nm]["Rate"] = float(rate)
        out[nm]["OrdPay"] = float(total.replace(",", ""))
        emp_idx += 1

    # Holiday pay may exist only for some employees; assign by proximity: try per employee name in text segment.
    for nm in emp_names:
        # search within a window after the name
        pos = t.find(nm)
        if pos >= 0:
            seg = t[pos : pos + 600]
            mh = hol_pat.search(seg)
            if mh:
                out[nm]["HolidayPayRatePct"] = float(mh.group(1))
                out[nm]["HolidayPay"] = float(mh.group(2))

            mp = paye_pat.search(seg)
            if mp:
                out[nm]["PAYE"] = float(mp.group(1))

    return out


def compute_overlaps(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    shifts columns: Employee, ShiftStart, ShiftEnd, Date, Hours
    Returns overlaps with minutes.
    """
    rows = []
    if shifts.empty:
        return pd.DataFrame(columns=["Date", "EmployeeA", "EmployeeB", "OverlapStart", "OverlapEnd", "OverlapHours"])

    # Compare per date bucket (use start date)
    shifts = shifts.copy()
    shifts["DateKey"] = shifts["ShiftStart"].dt.date

    for d, grp in shifts.groupby("DateKey"):
        grp = grp.sort_values("ShiftStart")
        arr = grp.to_dict("records")
        for i in range(len(arr)):
            for j in range(i + 1, len(arr)):
                a = arr[i]
                b = arr[j]
                # if different employees
                if a["Employee"] == b["Employee"]:
                    continue
                s = max(a["ShiftStart"], b["ShiftStart"])
                e = min(a["ShiftEnd"], b["ShiftEnd"])
                if pd.isna(s) or pd.isna(e):
                    continue
                if e > s:
                    oh = (e - s).total_seconds() / 3600.0
                    rows.append(
                        {
                            "Date": d,
                            "EmployeeA": a["Employee"],
                            "EmployeeB": b["Employee"],
                            "OverlapStart": s,
                            "OverlapEnd": e,
                            "OverlapHours": round(oh, 2),
                        }
                    )

    return pd.DataFrame(rows)


def make_export_excel(summary_df: pd.DataFrame, details_by_emp: Dict[str, pd.DataFrame], overlaps_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Pay Summary")
        overlaps_df.to_excel(writer, index=False, sheet_name="Overlaps")
        for emp, d in details_by_emp.items():
            sheet = re.sub(r"[\[\]\*:/\\\?]", " ", emp)[:31] or "Employee"
            d.to_excel(writer, index=False, sheet_name=sheet)
    return output.getvalue()


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="LiquorPanda Timesheet → Payroll (NZ, Fortnightly)", layout="wide")

st.title("LiquorPanda Timesheet → Payroll (NZ, Fortnightly)")
st.caption(
    "Reads an Excel workbook with one sheet per employee, calculates hours, detects overlaps, and produces a PAYE-based pay summary."
    st.caption(
    "Created by Liju Varghese(Aitechs Solutions)"
)

with st.expander("Reference: your processed pay summary (for QA)"):
    st.write(
        "Your provided pay summary shows for 30 Nov 2025 – 13 Dec 2025: "
        "Gaurav has 47.50 ordinary hours at $24 with 8% holiday pay, and Saurav has 97.00 hours at $28. "
        "PAYE and take-home are listed there for comparison."
    )

# Uploads
col_up1, col_up2 = st.columns([2, 1])
with col_up1:
    xls_file = st.file_uploader("Upload timesheet Excel (.xlsx)", type=["xlsx"])
with col_up2:
    pdf_file = st.file_uploader("Optional: Upload processed pay summary PDF (for validation)", type=["pdf"])

if not xls_file:
    st.info("Upload your timesheet Excel file to begin.")
    st.stop()

# Cycle selection (anchor-based)
st.sidebar.header("Pay Cycle (Fortnight)")
anchor = st.sidebar.date_input("Anchor cycle start date", value=date(2025, 11, 30))
cycle_index = st.sidebar.number_input(
    "Cycle index (0 = anchor fortnight, 1 = next, -1 = previous)",
    min_value=-200,
    max_value=200,
    value=0,
    step=1,
)
cycle_start = anchor + timedelta(days=int(cycle_index) * 14)
cycle_end = cycle_start + timedelta(days=13)
st.sidebar.write(f"**Cycle:** {cycle_start} → {cycle_end} (14 days)")

# Read workbook
xls_bytes = xls_file.getvalue()
xl = pd.ExcelFile(io.BytesIO(xls_bytes))
sheetnames = xl.sheet_names

# Pick timesheet-like sheets automatically, allow override
auto_timesheet_sheets = [s for s in sheetnames if re.search(r"\btime\s*sheet\b|\btimesheet\b", s, flags=re.IGNORECASE)]
if not auto_timesheet_sheets:
    auto_timesheet_sheets = sheetnames

chosen_sheets = st.multiselect(
    "Sheets to include (employee sheets)",
    options=sheetnames,
    default=auto_timesheet_sheets,
)

# Load & standardise all selected sheets
emp_frames: Dict[str, pd.DataFrame] = {}
for sh in chosen_sheets:
    if sh.strip().lower() in {"daily sales", "sheet1", "sheet2"}:
        continue  # skip obvious non-timesheet sheets
    try:
        df_sh = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sh)
        emp = normalise_sheet_name_to_employee(sh)
        df_std = standardise_timesheet_df(df_sh)
        df_std["Employee"] = emp
        emp_frames[emp] = df_std
    except Exception as e:
        st.warning(f"Could not read sheet '{sh}': {e}")

if not emp_frames:
    st.error("No usable employee sheets found.")
    st.stop()

# Combine and filter by cycle
all_shifts = pd.concat(emp_frames.values(), ignore_index=True)
all_shifts = all_shifts.dropna(subset=["ShiftStart", "ShiftEnd"])
all_shifts["Date"] = all_shifts["ShiftStart"].dt.date
mask = (all_shifts["ShiftStart"].dt.date >= cycle_start) & (all_shifts["ShiftStart"].dt.date <= cycle_end)
cycle_shifts = all_shifts.loc[mask].copy()

# Holidays
years_needed = sorted({cycle_start.year, cycle_end.year})
holiday_map = get_nz_public_holidays(years_needed)
cycle_shifts["IsPublicHoliday"] = cycle_shifts["Date"].apply(lambda d: d in holiday_map)
cycle_shifts["HolidayName"] = cycle_shifts["Date"].apply(lambda d: holiday_map.get(d, ""))

# Employee settings table
st.sidebar.header("Employee Settings")
default_rows = []
for emp in sorted(emp_frames.keys()):
    # pragmatic defaults (edit in UI)
    rate_guess = 0.0
    # basic hints
    if emp.strip().lower() == "gaurav":
        rate_guess = 24.0
    if emp.strip().lower() == "saurav":
        rate_guess = 28.0
    default_rows.append(
        {
            "Sheet/Employee": emp,
            "Payroll Name": emp,  # you can expand to full name
            "Hourly Rate": rate_guess,
            "Apply Holiday Pay (8%)": (emp.strip().lower() == "gaurav"),
            "Holiday Pay Rate": 0.08,
            "Apply Public Holiday Rules": True,
            "Tax Code": "M",
        }
    )

settings_df = pd.DataFrame(default_rows)

settings_df = st.sidebar.data_editor(
    settings_df,
    hide_index=True,
    use_container_width=True,
    column_config={
        "Hourly Rate": st.column_config.NumberColumn(min_value=0.0, step=0.5, format="%.2f"),
        "Holiday Pay Rate": st.column_config.NumberColumn(min_value=0.0, max_value=0.2, step=0.01, format="%.2f"),
        "Apply Holiday Pay (8%)": st.column_config.CheckboxColumn(),
        "Apply Public Holiday Rules": st.column_config.CheckboxColumn(),
    },
)

settings_map: Dict[str, EmpSettings] = {}
for _, r in settings_df.iterrows():
    settings_map[str(r["Sheet/Employee"])] = EmpSettings(
        payroll_name=str(r["Payroll Name"]),
        hourly_rate=float(r["Hourly Rate"] or 0.0),
        apply_holiday_pay=bool(r["Apply Holiday Pay (8%)"]),
        holiday_pay_rate=float(r["Holiday Pay Rate"] or 0.0),
        apply_public_holiday_rules=bool(r["Apply Public Holiday Rules"]),
        tax_code=str(r["Tax Code"] or "M"),
    )

# Validate rates
missing_rates = [emp for emp, s in settings_map.items() if s.hourly_rate <= 0]
if missing_rates:
    st.warning(f"Set Hourly Rate for: {', '.join(missing_rates)} (left sidebar).")

# Overlaps
overlaps_df = compute_overlaps(cycle_shifts[["Employee", "ShiftStart", "ShiftEnd", "Hours"]].assign(Date=cycle_shifts["Date"]))
overlaps_df = overlaps_df.sort_values(["Date", "OverlapStart"]) if not overlaps_df.empty else overlaps_df

# Compute pay per employee
summary_rows = []
details_by_emp: Dict[str, pd.DataFrame] = {}

for emp, grp in cycle_shifts.groupby("Employee"):
    s = settings_map.get(emp, EmpSettings(emp, 0.0, False, 0.08, True, "M"))
    g = grp.copy()

    # hours buckets
    ph_hours = float(g.loc[g["IsPublicHoliday"], "Hours"].sum())
    normal_hours = float(g.loc[~g["IsPublicHoliday"], "Hours"].sum())

    # pay buckets
    rate = float(s.hourly_rate)
    ordinary_pay = normal_hours * rate

    if s.apply_public_holiday_rules:
        public_holiday_pay = ph_hours * rate * 1.5
    else:
        public_holiday_pay = ph_hours * rate  # treat as ordinary

    holiday_pay = (ordinary_pay + public_holiday_pay) * float(s.holiday_pay_rate) if s.apply_holiday_pay else 0.0

    gross = ordinary_pay + public_holiday_pay + holiday_pay

    # PAYE (approx; close to IRD tables for standard cases)
    paye = compute_paye_fortnightly(gross)
    take_home = gross - paye

    days_paid = int((g["Hours"].fillna(0) > 0).sum())

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
            "PAYE": round(paye, 2),
            "Take Home": round(take_home, 2),
            "Tax Code": s.tax_code,
        }
    )

    # detail view
    g2 = g.copy()
    g2["ShiftHours"] = g2["Hours"].round(2)
    g2["PayType"] = np.where(g2["IsPublicHoliday"], "Public Holiday", "Ordinary")
    g2["BasePay"] = np.where(
        g2["IsPublicHoliday"],
        g2["Hours"] * rate * (1.5 if s.apply_public_holiday_rules else 1.0),
        g2["Hours"] * rate,
    )
    g2["BasePay"] = g2["BasePay"].round(2)
    details_by_emp[emp] = g2[
        ["Employee", "ShiftStart", "ShiftEnd", "Date", "ShiftHours", "PayType", "HolidayName", "BasePay"]
    ].sort_values("ShiftStart")

summary_df = pd.DataFrame(summary_rows).sort_values("Employee (Sheet)")

# Top metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Cycle start", str(cycle_start))
c2.metric("Cycle end", str(cycle_end))
c3.metric("Employees included", str(summary_df.shape[0]))
c4.metric("Overlap instances", str(0 if overlaps_df.empty else overlaps_df.shape[0]))

st.subheader("Pay Run Summary (computed)")
st.dataframe(summary_df, use_container_width=True)

st.subheader("Overlaps (same time, different employees)")
if overlaps_df.empty:
    st.success("No overlaps detected in this cycle.")
else:
    st.dataframe(overlaps_df, use_container_width=True)

# Optional PDF validation
if pdf_file:
    st.subheader("Validation vs Processed Pay Summary PDF")
    pdf_bytes = pdf_file.getvalue()
    parsed = parse_pay_summary_pdf(pdf_bytes)

    if not parsed:
        st.warning("Could not parse the PDF text (try exporting as text-based PDF, not scanned).")
    else:
        # Build comparison table by matching payroll name
        comp_rows = []
        for _, r in summary_df.iterrows():
            payroll_name = r["Employee (Payroll)"]
            # exact or contains match
            match_key = None
            for k in parsed.keys():
                if k.strip().lower() == str(payroll_name).strip().lower():
                    match_key = k
                    break
            if not match_key:
                for k in parsed.keys():
                    if str(payroll_name).strip().lower() in k.strip().lower() or k.strip().lower() in str(payroll_name).strip().lower():
                        match_key = k
                        break

            if not match_key:
                continue

            exp = parsed[match_key]
            comp_rows.append(
                {
                    "Employee (Payroll)": payroll_name,
                    "Expected Gross": exp.get("Gross", np.nan),
                    "Computed Gross": r["Gross Pay"],
                    "Diff Gross": round(float(r["Gross Pay"]) - float(exp.get("Gross", 0.0)), 2),
                    "Expected PAYE": exp.get("PAYE", np.nan),
                    "Computed PAYE": r["PAYE"],
                    "Diff PAYE": round(float(r["PAYE"]) - float(exp.get("PAYE", 0.0)), 2),
                    "Expected TakeHome": exp.get("TakeHome", np.nan),
                    "Computed TakeHome": r["Take Home"],
                    "Diff TakeHome": round(float(r["Take Home"]) - float(exp.get("TakeHome", 0.0)), 2),
                    "Expected Days": exp.get("Days", np.nan),
                    "Computed Days": r["Days Paid"],
                }
            )

        if comp_rows:
            comp_df = pd.DataFrame(comp_rows)
            st.dataframe(comp_df, use_container_width=True)
        else:
            st.info("No matching employees found between computed summary and PDF names. Update Payroll Name in settings.")

# Employee details
st.subheader("Employee Shift Details (cycle)")
tabs = st.tabs([emp for emp in sorted(details_by_emp.keys())])
for tab, emp in zip(tabs, sorted(details_by_emp.keys())):
    with tab:
        st.dataframe(details_by_emp[emp], use_container_width=True)

# Export
st.subheader("Export")
export_bytes = make_export_excel(summary_df, details_by_emp, overlaps_df)
st.download_button(
    "Download Excel (Pay Summary + Overlaps + Employee Details)",
    data=export_bytes,
    file_name=f"payroll_summary_{cycle_start}_to_{cycle_end}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption(
    "Public holiday identification uses the Python 'holidays' library where available. "
    "Public holiday pay is calculated as 1.5x for worked hours; alternative day accrual is noted conceptually but not booked as cash."
)


