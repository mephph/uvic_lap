"""
Utilities for LAP timesheets
"""
import calendar
import datetime
import json
import logging
import os.path
import pathlib
import re
from pickle import UnpicklingError

import numpy as np
import pandas as pd

__all__ = ["concat_pay_periods", "load_timesheet", "parse_timesheet"]

########################################################################################
# Module constants and defaults

with open("pay_periods.json", "r") as f:
    _PAY_PERIODS = json.load(f)

with open("positions.json", "r") as f:
    _POSITIONS = json.load(f)

# calendar.month_abbr is a calendar.py-specific type which doesn't support searching.
_MONTH_ABBRS = tuple(map(str.lower, calendar.month_abbr))
_TODAY = datetime.date.today()

# Pattern to match recognizable time strings and extract their relevant components. The
# second section must be explicitly repeated (instead of using {0,2}) to give all
# mathces the same number of groups, and to make sure group 2 always means minute, etc.
#
# Time strings in LAP timesheets are varied. All of the following have come up and are
# good enough to be understood: 4, 4:15, 4:15:, 4:15:00pm, 4h15, 4PM, 4p, 4:15 Pm,
# 16:15, 16:15:
_TIME_REGEX = re.compile(
    # 1 or 2 digits. Capture the digits in group 1.
    r"(\d{1,2})"
    # A non-digit separator (like h or :) and two digits. Capture the digits in group 2.
    r"(?:[^\d](\d{2}))?"
    # A non-digit separator (like h or :) and two digits. Capture the digits in group 3.
    r"(?:[^\d](\d{2}))?"
    # Non-digit, non-letter separators
    r"[^\d\w]*?"
    # A or P, uppercase or lowercase and optional M. Capture the A or P in group 4.
    r"(?:([AaPp])[Mm]?)?"
)

# Internal-use names for columns.
_COLUMN_SHORT_NAMES = {
    "Position": "position",
    "Last Name": "last",
    "First Name": "first",
    "Class Tutored": "class",
    "Month": "month",
    "Day": "day",
    "Start Time": "start",
    "End Time": "end",
    "Duration": "duration",
    "Notes": "notes",
}
_COLUMN_LONG_NAMES = {v: k for k, v in _COLUMN_SHORT_NAMES.items()}

# Arguments for Pandas ExcelFile.parse and related Excel functions.
_EXCEL = {
    # 0-indexed row of sheet which contains column names.
    "header": 1,
    # List of column names to use.
    "usecols": [
        "Position",
        "Last Name",
        "First Name",
        "Class Tutored",
        "Month",
        "Day",
        "Start Time",
        "End Time",
        "Duration",
        "Notes",
    ],
}


########################################################################################
# Utility functions


def _int_or_zero(x):
    """Convert a string to an int, or None to 0.

    Args:
        x: A string or None

    Return:
        int(x) if x is a string or 0 if x is None.
    """
    if x is not None:
        return int(x)
    else:
        return 0


def _str_to_time(string):
    """Parse a string representing a time of day.

    Args:
        string: Any string

    Return:
        A datetime.time or None if the string isn't a valid time.
    """
    try:
        match = _TIME_REGEX.fullmatch(string.strip())
    except (TypeError, AttributeError):
        return np.nan

    if match is not None:
        # Groups are hour, minute, second, a/p.
        groups = match.groups()
        # The second, third, and fourth groups could be None if string is '4'.
        hour, minute, second = map(_int_or_zero, groups[0:3])

        if groups[3] is not None and groups[3].lower() == "p" and hour < 12:
            hour += 12

        return datetime.time(hour, minute, second)

    return np.nan


def _normalize_time(time):
    """Time like '4:15 PM'.

    Args:
        time: A datetime.time

    Return:
        A string like '4:15 PM' or numpy.nan"""
    try:
        return time.strftime("%I:%M %p").lstrip("0")
    except (TypeError, AttributeError):
        return np.nan


def _round_to_multiple(x, base=1):
    """Round to the nearest multiple.

    Args:
        x: Number to round
        base: Round to nearest multiple of this

    Return:
        Nearest multiple"""
    return round(x / base) * base


########################################################################################
# Entry parsing


def _parse_month(string):
    """Parse a string representing a month into an int 1-12.

    Only the first three characters of the string are considered, so 'Feb', 'febr', and
    'february' will all return 2.

    Args:
        string: Any string

    Return:
        An int 1-12 or numpy.nan if the string is invalid.
    """
    try:
        # Three characters are enough to identify any month. The slice and lower call
        # can be handled by Series.str.slice and Series.str.lower, but vectorization
        # would be more complicated.
        return _MONTH_ABBRS.index(string[:3].lower())
    except ValueError:
        return np.nan


def _parse_date(series):
    """Parse a Pandas Series with year, month, and day entries into a datetime.date.

    Args:
        series: A Pandas Series year, month, and day entries.

    Return:
        A datetime.date or pandas.NaT if the year, month, and day year aren't valid.
    """
    try:
        return datetime.date(**series)
    except (TypeError, ValueError):
        return pd.NaT


########################################################################################
# Timesheet processing


def concat_pay_periods(workbook):
    """Concatenate all pay periods into one DataFrame.

    Entries are not processed or cast into other types.

    Args:
        workbook: A Pandas ExcelFile

    Return:
        DataFrame with columns in _EXCEL["usecols"], 'original_line', and '_period'.
    """
    period_names = [name for name in workbook.sheet_names if name in _PAY_PERIODS]
    period_sheets = []

    for name in period_names:
        sheet = workbook.parse(
            name, header=_EXCEL["header"], usecols=_EXCEL["usecols"], dtype=str
        )

        # The timesheet has irrelevant entries in rows 0-12. workbook.parse will
        # properly ignore the entries, but will produce empty rows if the data columns
        # are empty. Also, filled rows can be interspersed with unfilled rows.
        sheet.dropna(how="all", inplace=True)
        sheet.rename(columns=_COLUMN_SHORT_NAMES, inplace=True)

        # Record original line number and pay period for error messages. The +2 is
        # necessary because Excel rows are 1-indexed while Pandas DataFrames are
        # 0-indexed and the first row of the DataFrame is one after the header.
        sheet["_original_line"] = sheet.index + _EXCEL["header"] + 2
        sheet["_period"] = name

        period_sheets.append(sheet)

    return pd.concat(period_sheets, ignore_index=True)


def load_timesheet(path):
    """Load a timesheet Excel file into a DataFrame.

    The timesheet DataFrame will be pickled after loading, and the pickled version will
    be loaded if it is newer than the Excel file.

    Args:
        path: String path of Excel file

    Return:
        Pandas DataFrame containing all timesheet entries.
    """
    xlsx_path = pathlib.Path(path)
    pickle_path = xlsx_path.with_suffix(".pkl")

    # Load the pickled version if it exists and isn't older.
    if os.path.exists(pickle_path):
        if os.path.getmtime(pickle_path) >= os.path.getmtime(xlsx_path):
            try:
                return pd.read_pickle(pickle_path)
            except UnpicklingError:
                pass

    timesheet = concat_pay_periods(pd.ExcelFile(path))

    # Add year and provider name columns to the DataFrame.
    try:
        # Timesheet filenames are SemesterYear_PAYROLL_LastName_FirstName_VNumber.xlsx.
        semester, _, last, first, vnumber = pathlib.Path(path).stem.split("_")
        timesheet["provider"] = f"{first} {last}"

        try:
            # Semester part of filename is like 'Fall2019'.
            year = int(semester[-4:])
            timesheet["year"] = year
        except ValueError:
            logging.error(f"Unexpected semester format in filename: {semester}")
            timesheet["year"] = np.nan
    except ValueError:
        logging.error(f"Unexpected filename format: {path.stem}")
        timesheet["provider"] = np.nan

    timesheet.to_pickle(pickle_path)

    return timesheet


def parse_timesheet(ts):
    """Parse timesheet entries.

    Args:
        ts: A timesheet DataFrame

    Return:
        DataFrame with parsed values"""
    parsed = pd.DataFrame(index=ts.index)

    parsed["month"] = ts["month"].dropna().apply(_parse_month)
    parsed["day"] = pd.to_numeric(ts["day"], errors="coerce")

    parsed["date"] = (
        # Combine year, month, and day columns.
        pd.concat([ts["year"], parsed[["month", "day"]]], axis=1)
        .dropna()
        # Cast to int in case month or day were floats because of NaN entries.
        .astype(int)
        # Convert to datetime.date.
        .apply(_parse_date, axis=1)
        .dropna()
        # Put in YYYY-MM-DD form.
        .apply(datetime.date.isoformat)
    )

    parsed["start"] = ts["start"].apply(_str_to_time).apply(_normalize_time)
    parsed["start_dt"] = pd.to_datetime(parsed["date"] + " " + parsed["start"])

    parsed["end"] = ts["end"].apply(_str_to_time).apply(_normalize_time)
    parsed["end_dt"] = pd.to_datetime(parsed["date"] + " " + parsed["end"])

    # The times will always be on the same day, so the duration can be computed even if
    # the date is missing or invalid.
    parsed["duration"] = _round_to_multiple(
        # Convert to NumPy datetime for easy subtraction. The order must be 'start',
        # 'end' because DataFrame.diff goes in column order.
        parsed[["start", "end"]].dropna().astype("datetime64[ns]")
        # Subtract along rows. There are only two columns so the difference is in the
        # second.
        .diff(axis=1).iloc[:, 1]
        # Convert to fractional hours and round to nearest multiple of 15 minutes.
        .dt.seconds / 3600,
        0.25,
    )

    return parsed[
        ["month", "day", "date", "start", "end", "start_dt", "end_dt", "duration"]
    ]
