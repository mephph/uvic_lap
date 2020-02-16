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
from pickle import PicklingError, UnpicklingError

import pandas as pd

########################################################################################
# Module constants and defaults

with open("pay_periods.json", "r") as f:
    _PAY_PERIODS = json.load(f)

# The Excel sheets ignore case in position names, so "Tutor" and "tutor" can appear.
# Convert everything to lower case for matching.
with open("positions.json", "r") as f:
    _POSITIONS = {key.lower(): value for key, value in json.load(f).items()}

# calendar.month_abbr is a calendar.py-specific type which doesn't support searching.
_MONTH_ABBRS = tuple(map(str.lower, calendar.month_abbr))

# Pattern to match recognizable time strings and extract their relevant components. The
# second section must be explicitly repeated (instead of using {0,2}) to give all
# matches the same number of groups, and to make sure group 2 always means minute, etc.
#
# Note that this will match invalid strings like "35:74 PM".
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
        A datetime.time or numpy.nan if the string isn't a valid time.
    """
    try:
        match = _TIME_REGEX.fullmatch(string.strip())
    # string.strip() isn't a string or string isn't a string.
    except (TypeError, AttributeError):
        return pd.NA

    if match is not None:
        # Groups are hour, minute, second, a/p. The second, third, and fourth groups
        groups = match.groups()
        # could be None if string is like '4'.
        hour, minute, second = map(_int_or_zero, groups[0:3])

        # Add 12 hours to times like "1:00 PM"
        if groups[3] is not None and groups[3].lower() == "p" and hour < 12:
            hour += 12

        try:
            return datetime.time(hour, minute, second)
        # Hour, minute, and second aren't a valid time.
        except ValueError:
            return pd.NA

    # String doesn't match regex.
    return pd.NA


def _normalize_time(time):
    """Time like '4:15 PM'.

    Args:
        time: A datetime.time

    Return:
        A string like '4:15 PM' or numpy.nan
    """
    try:
        # lstrip('0') removes the leading zero from times like "02:30 PM". There are no
        # cross-platform directives for the hour with no leading zero.
        return time.strftime("%I:%M %p").lstrip("0")
    except (TypeError, AttributeError):
        return pd.NA


def _round_to_multiple(x, base=1):
    """Round to the nearest multiple.

    >>> _round_to_multiple(3.4, 0.5)
    3.5

    Args:
        x: Number to round
        base: Round to nearest multiple of this

    Return:
        Nearest multiple
    """
    return round(x / base) * base


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
        return pd.NA


########################################################################################
# Timesheet errors


def missing_position(ts):
    return ts["position"].isna()


def unknown_position(ts):
    return ts["position"].notna() & ~ts["position"].astype(str).lower().isin(_POSITIONS)


def invalid_month(ts, parsed):
    return ts["month"].notna() & parsed["month"].isna()


def invalid_day(ts, parsed):
    return ts["day"].notna() & parsed["day"].isna()


def invalid_date(ts, parsed):
    return parsed[["month", "day"]].notna().all(axis=1) & parsed["date"].isna()


def invalid_start(ts, parsed):
    return ts["start"].notna() & parsed["start"].isna()


def invalid_end(ts, parsed):
    return ts["end"].notna() & parsed["end"].isna()


def invalid_duration(ts, parsed):
    return ts["duration"].notna() & parsed["duration"].isna()


def wrong_duration(ts, parsed):
    return parsed["computed_duration"].notna() & (
        parsed["duration"] != parsed["computed_duration"]
    )


def duration_not_quarter_hour(ts, parsed):
    return parsed["duration"] != parsed["rounded_duration"]


########################################################################################
# Timesheet processing


def concat_pay_periods(workbook):
    """Concatenate all pay periods into one DataFrame.

    Entries are left as dtype 'object', though Pandas will still silenty convert values
    in some homogeneous columns.

    Args:
        workbook: A Pandas ExcelFile

    Return:
        DataFrame with columns in _EXCEL["usecols"], 'original_line', and '_period'.
    """
    # The names of all pay period sheet names in the workbook.
    period_names = [name for name in workbook.sheet_names if name in _PAY_PERIODS]
    period_sheets = []

    for name in period_names:
        # Use dtype=object because Pandas sets the dtype of columns containing only
        # numbers to int or float (even with dtype=object or dtype='object'). Specifying
        # dtype='string' raises an error with columns of numeric dtype.
        sheet = workbook.parse(
            name, header=_EXCEL["header"], usecols=_EXCEL["usecols"], dtype=object
        )

        # The timesheet has irrelevant entries in unused columns of early rows.
        # workbook.parse will properly ignore the entries, but will produce empty rows
        # if the data columns are empty. Also, filled rows can be interspersed with
        # unfilled rows.
        sheet.dropna(how="all", inplace=True)
        sheet.rename(columns=_COLUMN_SHORT_NAMES, inplace=True)

        # Record original row number and pay period for error messages. The +2 is
        # necessary because Excel rows are 1-indexed while Pandas DataFrames are
        # 0-indexed and the first row of the DataFrame is one after the header.
        sheet["row"] = sheet.index + _EXCEL["header"] + 2

        sheet["period"] = name
        sheet["period"] = sheet["period"].astype("string")

        period_sheets.append(sheet)

    return pd.concat(period_sheets, ignore_index=True)


def load_timesheet(path, load_pickle=True, save_pickle=True):
    """Load a timesheet Excel file into a DataFrame.

    The timesheet DataFrame will be pickled after loading, and the pickled version will
    be loaded if it is newer than the Excel file.

    Args:
        path: String path of Excel file
        load_pickle: Whether to load previously pickled version of timesheet
        save_pickle: Whether to pickle the timesheet once loaded

    Return:
        Pandas DataFrame containing all timesheet entries.
    """
    xlsx_path = pathlib.Path(path)
    pickle_path = xlsx_path.with_suffix(".pkl")

    # Load the pickled version if it exists and isn't older.
    if load_pickle and os.path.exists(pickle_path):
        if os.path.getmtime(pickle_path) >= os.path.getmtime(xlsx_path):
            try:
                return pd.read_pickle(pickle_path)
            except UnpicklingError:
                logging.error(f"Couldn't unpickle {pickle_path}. Loading original.")

    timesheet = concat_pay_periods(pd.ExcelFile(path))

    # Add year, provider name, pay period, and period end date columns.
    try:
        # Timesheet filenames are SemesterYear_PAYROLL_LastName_FirstName_VNumber.xlsx.
        semester, _, last, first, vnumber = pathlib.Path(path).stem.split("_")
        timesheet["provider"] = f"{first} {last}"

        try:
            # Semester part of filename is like 'Fall2019'.
            year = int(semester[-4:])
            timesheet["year"] = year
        # Last four digits aren't a valid integer.
        except ValueError:
            logging.error(f"Unexpected semester format in filename: {semester}")
            timesheet["year"] = pd.NA
    # Incorrect number of parts in filename.
    except ValueError:
        logging.error(f"Unexpected filename format: {path.stem}")
        timesheet["provider"] = pd.NA

    # string and Int32 are nullable.
    timesheet["provider"] = timesheet["provider"].astype("string")
    timesheet["year"] = timesheet["year"].astype("Int32")

    # Look-up month and day of the end of the pay period. The year isn't stored in
    # _PAY_PERIODS in case timesheets are read after the relevant semester ends. This is
    # a bit ugly because the year is stored outside the file in the filename.
    period_end = pd.DataFrame(
        timesheet["period"].apply(lambda p: _PAY_PERIODS[p]["last"]).tolist(),
        columns=["month", "day"],
    )
    period_end["year"] = timesheet["year"]
    timesheet["period_end"] = pd.to_datetime(period_end)

    if save_pickle:
        try:
            timesheet.to_pickle(pickle_path)
        except PicklingError:
            logging.error(f"Couldn't pickle to {pickle_path}")

    return timesheet


def parse_timesheet(ts):
    """Parse timesheet entries.

    Args:
        ts: A timesheet DataFrame

    Return:
        DataFrame with parsed entries
    """
    parsed = pd.DataFrame(index=ts.index)

    # name will be NaN if first or last are NaN.
    parsed["student"] = ts["first"] + " " + ts["last"]

    parsed["month"] = ts["month"].dropna().apply(_parse_month)

    # Coerce errors to NaN.
    parsed["day"] = pd.to_numeric(ts["day"], errors="coerce")

    # Date in YYYY-MM-DD format.
    parsed["date"] = pd.to_datetime(
        pd.concat([ts["year"], parsed[["month", "day"]]], axis=1).dropna().astype(int)
    ).dt.strftime("%Y-%m-%d")

    # Times in 'HH:MM AM/PM' format.
    parsed["start"] = ts["start"].apply(_str_to_time).apply(_normalize_time)
    parsed["end"] = ts["end"].apply(_str_to_time).apply(_normalize_time)

    # Datetimes for sorting and detecting overlapping appointments.
    parsed["start_dt"] = pd.to_datetime(parsed["date"] + " " + parsed["start"])
    parsed["end_dt"] = pd.to_datetime(parsed["date"] + " " + parsed["end"])

    # Coerce errors to NaN.
    parsed["duration"] = pd.to_numeric(ts["duration"], errors="coerce")

    # The times will always be on the same day, so the duration can be computed even if
    # the date is missing or invalid.
    parsed["computed_duration"] = (
        # Convert to NumPy datetime for easy subtraction. The order must be 'start',
        # 'end' because DataFrame.diff does current - previous.
        parsed[["start", "end"]].dropna().astype("datetime64[ns]")
        # Subtract along rows. There are only two columns so the difference is in the
        # second.
        .diff(axis=1).iloc[:, 1].dt.seconds
        # Convert seconds to hours.
        / 3600
    )

    # Duration rounded to nearest quarter hour.
    parsed["rounded_duration"] = _round_to_multiple(parsed["duration"], 0.25)

    return parsed


def _error_series(row, message_template):
    """Series containing information about a timesheet error.

    Args:
        row: A row of a timesheet DataFrame
        message_template: A string template for formatting with row values

    Return:
        Series with 'provider', 'period', 'row', and 'error' columns.
    """
    return pd.Series(
        {
            "provider": row["provider"],
            "period": row["period"],
            "row": row["excel_row"],
            "error": message_template.format(**row),
        }
    )


def _error_messages(rows, message_template):
    """Message and information about each row of a DataFrame.

    Args:
        df: A timesheet DataFrame
        message_template: A  string template for formatting with row values

    Return:
        DataFrame with 'provider', 'period', 'row', and 'error' columns, or None.
    """
    result = rows.apply(_error_series, axis=1, message_template=message_template)

    # If the result of apply is empty the DataFrame has the columns of rows. This causes
    # an error when the results of different calls to detect_errors are concatenated.
    # Instead, return None since pandas.concat ignores all None values.
    return result if len(result) else None


def detect_errors(ts, parsed=None):
    """All errors in a timesheet.

    Args:
        ts: A timesheet DataFrame
        parsed: The result of parse_timesheet(ts)

    Return:
        DataFrame with 'provider', 'period', 'row', and 'error' columns, or None.
    """
    if parsed is None:
        parsed = parse_timesheet(ts)

    # A list of tuples like (mask, message template).
    error_types = [
        # Missing position
        (ts["position"].isna(), "Missing position"),
        # Unknown position (not a key in _POSITIONS)
        (
            ts["position"].notna()
            & ~ts["position"].astype(str).str.lower().isin(_POSITIONS),
            "Unrecognized position: {position}",
        ),
        # Missing entries
        # Unexpected entries
        # Invalid month
        (ts["month"].notna() & parsed["month"].isna(), "Invalid month: {month}"),
        # Invalid day
        (ts["day"].notna() & parsed["day"].isna(), "Invalid day: {day}"),
        # Invalid date
        (
            parsed[["month", "day"]].notna().all(axis=1) & parsed["date"].isna(),
            "Invalid date: {month} {day}",
        ),
        # Invalid start time
        (ts["start"].notna() & parsed["start"].isna(), "Invalid start time: {start}",),
        # Invalid end time
        (ts["end"].notna() & parsed["end"].isna(), "Invalid end time: {end}",),
        # Invalid duration
        (
            ts["duration"].notna() & parsed["duration"].isna(),
            "Invalid duration: {duration}",
        ),
        # Duration doesn't match computed duration
        (
            parsed["computed_duration"].notna()
            & (parsed["duration"] != parsed["computed_duration"]),
            "Duration doesn't match times: {duration} and {start} to {end}",
        ),
        # Duration isn't multiple of 0.25
        (
            parsed["duration"] != parsed["rounded_duration"],
            "Duration isn't rounded to 15 minutes: {duration}",
        ),
        # Date after end of pay period
        # Student name not in list of matches
        # Class name doesn't match match
        # Meeting overlaps another without being noted
    ]

    try:
        return pd.concat(
            [_error_messages(ts[mask], message) for mask, message in error_types],
            ignore_index=True,
        )
    # No errors found.
    except ValueError:
        return None
