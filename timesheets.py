"""Utilities for LAP timesheets."""
import calendar
import json
import logging
import os.path
import pathlib
import re
from collections import namedtuple
from enum import Enum
from functools import wraps
from itertools import chain
from pickle import PicklingError, UnpicklingError

import pandas as pd

########################################################################################
# Module constants and defaults

# 0-indexed row of sheet which contains column names.
EXCEL_HEADER = 1
# List of column names to use.
EXCEL_USECOLS = [
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
]
# Internal-use names for columns.
COLUMN_SHORT_NAMES = {
    "Position": "position",
    "Last Name": "last",
    "First Name": "first",
    # 'class' conflicts with the class keyword in several settings.
    "Class Tutored": "course",
    "Month": "month",
    "Day": "day",
    "Start Time": "start",
    "End Time": "end",
    "Duration": "duration",
    "Notes": "notes",
}
COLUMN_LONG_NAMES = {v: k for k, v in COLUMN_SHORT_NAMES.items()}

# calendar.month_abbr is a calendar.py-specific type which doesn't support searching.
# Convert it to a tuple for index method.
MONTH_ABBRS = tuple(map(str.lower, calendar.month_abbr))

# Pattern to match recognizable time strings and extract their relevant components. The
# second section must be explicitly repeated (instead of using {0,2}) to give all
# matches the same number of groups, and to make sure group 2 always means minute, etc.
#
# Note that this will match invalid time strings like "35:74 PM".
#
# Time strings in LAP timesheets are varied. All of the following have come up and are
# good enough to be understood: 4, 4:15, 4:15:, 4:15:00pm, 4h15, 4PM, 4p, 4:15 Pm,
# 16:15, 16:15:
TIME_REGEX = re.compile(
    # 1 or 2 digits. Capture digits in 'hour'.
    r"(?P<hour>\d{1,2})"
    # A non-digit separator (like h or :) and two digits. Capture digits in 'minute'.
    r"(?:[^\d](?P<minute>\d{2}))?"
    # A non-digit separator (like h or :) and two digits. Capture digits in 'second'.
    r"(?:[^\d](?P<second>\d{2}))?"
    # Non-digit, non-letter separators
    r"[^\d\w]*"
    # A or P, uppercase or lowercase and optional M. Capture 'A' or 'P' in 'ampm'.
    r"(?:(?P<ampm>[AaPp])[Mm]?)?"
)

with open("pay_periods.json", "r") as f:
    PAY_PERIODS = json.load(f)

# The Excel sheets ignore case in position names, so "Tutor" and "tutor" can appear.
# Convert everything to lower case for matching.
with open("positions.json", "r") as f:
    POSITIONS = {key.lower(): value for key, value in json.load(f).items()}

# Replace all required and forbidden column names with short equivalents.
for _, position in POSITIONS.items():
    for key in ["required", "forbidden"]:
        position[key] = list(map(COLUMN_SHORT_NAMES.get, position[key]))


########################################################################################
# Classes

# Levels of timesheet entry errors.
ErrorLevel = Enum("ErrorLevel", ("COMMENT", "WARNING", "CRITICAL"))


class TSError(namedtuple("TSError", ("mask", "message", "level"))):
    """A timesheet error, including level, timesheet mask, and message template."""

    @classmethod
    def comment(cls, mask, message):  # noqa: D401
        """Data in timesheet is not in error but requires a comment."""
        """

        Args:
            mask: A Boolean mask of relevant timesheet rows
            message: Template for error messages

        Return:
            TSError with level 'COMMENT'
        """
        return cls(mask, message, ErrorLevel.COMMENT)

    @classmethod
    def warning(cls, mask, message):
        """Incorrect but recoverable data in timesheet.

        Args:
            mask: A Boolean mask of relevant timesheet rows
            message: Template for error messages

        Return:
            TSError with level 'WARNING'
        """
        return cls(mask, message, ErrorLevel.WARNING)

    @classmethod
    def critical(cls, mask, message):
        """Irrecoverable error in timesheet.

        Args:
            mask: A Boolean mask of relevant timesheet rows
            message: Template for error messages

        Return:
            TSError with level 'WARNING'
        """
        return cls(mask, message, ErrorLevel.CRITICAL)


########################################################################################
# Utility functions


def replace_errors(func, errors, replacement=pd.NA):
    """Wrap function to replace exceptions with error values.

    >>> replace_errors([0, 1, 2].index, ValueError)(3)
    <NA>

    >>> list(map(replace_errors(int, (ValueError, TypeError)), ['', [], '1']))
    [<NA>, <NA>, 1]

    Args:
        func: Any callable
        errors: An exception class or a tuple of classes
        replacement: Value with which to replace exceptions

    Return:
        Wrapped function which replaces exceptions
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except errors:
            return replacement

    return wrapper


def round_to_multiple(x, base=1):
    """Round to the nearest multiple.

    >>> round_to_multiple(3.4, 0.5)
    3.5

    Args:
        x: Number to round
        base: Round to nearest multiple of this

    Return:
        Nearest multiple of the base
    """
    return round(x / base) * base


def fix_wrong_na(df):
    """Replace incorrect pd.NA values in-place, preserving dtypes.

    Unpickling pd.NA values results in a different singleton. See
    https://github.com/pandas-dev/pandas/issues/31847 .

    Args:
        df: A DataFrame
    """

    def replace_na(x):
        if isinstance(x, type(pd.NA)):
            return pd.NA
        else:
            return x

    for column in df.columns:
        fixed = df[column].apply(replace_na)
        df[column] = fixed.astype(df[column].dtype)


########################################################################################
# Timesheet entry parsing


def str_to_int(series):
    """Parse strings into ints.

    pd.to_numeric can't convert non-int values like '1.2' to pd.NA.

    Args:
        series: A Pandas series

    Return:
        A Pandas series of dtype Int32, possibly with nulls
    """
    # int raises ValueError for strings like 'a' and TypeError for values like [].
    return series.apply(replace_errors(int, (ValueError, TypeError))).astype("Int32")


def str_to_month(series):
    """Parse a strings representing months into month numbers.

    Only the first three characters of the string are considered, so 'Feb', 'febr', and
    'february' will all return 2.

    Args:
        series: A string series

    Return:
        An Int32 series, possibly with nulls
    """
    return (
        series.dropna()
        # Three characters are enough to identify any month.
        .str[0:3]
        .str.lower()
        # index raises ValueError if the string isn't in the list.
        .apply(replace_errors(MONTH_ABBRS.index, ValueError))
        .astype("Int32")
    )


def drop_empty_strings(series):
    """Replace whitespace-only strings with pd.NA.

    Args:
        series: A string series

    Return:
        A string series, possibly with nulls
    """
    return series.dropna().str.strip().replace("", pd.NA).astype("string")


def normalize_time_string(series):
    """Normalize time strings to 24-hour HH:MM or pd.NA.

    Args:
        series: A string series

    Return:
        A string series, possibly with nulls
    """
    matches = series.str.extract(TIME_REGEX)

    hour = matches["hour"]
    minute = matches["minute"]
    # The 'ampm' group only contains 'a' or 'p'.
    # If the string doesn't contain anything like 'am' or 'pm' then matches['ampm'] is
    # NA, and the entire string argument to to_datetime is NA. fillna("") avoids that.
    ampm = (matches["ampm"] + "m").fillna("")
    times = pd.to_datetime(hour + ":" + minute + ampm, errors="coerce")

    return times.dt.strftime("%H:%M").astype("string")


def parse_time_string(series, year=None, month=None, day=None):
    """Parse time strings to Pandas timestamps.

    Args:
        series: A string series

    Return:
        A Pandas timestamp series, possibly with nulls
    """
    matches = series.str.extract(TIME_REGEX)

    hour = matches["hour"]
    minute = matches["minute"]
    # The 'ampm' group only contains 'a' or 'p'.
    # If the string doesn't contain anything like 'am' or 'pm' then matches['ampm'] is
    # NA, and the entire string argument to to_datetime is NA. fillna("") avoids that.
    ampm = (matches["ampm"] + "m").fillna("")

    if year is not None and month is not None and day is not None:
        dates = year + "-" + month + "-" + day
    else:
        dates = ""

    times = hour + ":" + minute + ampm

    return pd.to_datetime(dates + times, errors="coerce")


def normalize_position(series):
    """Normalize position names to lowercase and replace invalid names with pd.NA.

    Args:
        series: A string series

    Return:
        A string series, possibly with nulls
    """
    position = series.str.lower()

    return position[position.isin(POSITIONS)]


########################################################################################
# Error detection
#
# Error detection functions return a list of TSErrors.


def position_errors(ts, parsed):
    """Detect missing and invalid entries in the Position column.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    errors = []

    missing_position = ts["position"].isna()
    if missing_position.sum():
        errors.append(TSError.critical(missing_position, "Missing entry: Position"))

    invalid_position = ~missing_position & parsed["position"].isna()
    if invalid_position.sum():
        errors.append(
            TSError.critical(invalid_position, "Invalid position: {position}")
        )

    return errors


def required_entry_errors(ts, parsed):
    """Detect missing entries which are required for the position.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    errors = []

    for position in POSITIONS:
        position_mask = (parsed["position"] == position).fillna(False)

        for column in POSITIONS[position]["required"]:
            has_entry = ts[column].notna().fillna(False)
            missing = position_mask & ~has_entry

            if missing.sum():
                message = f"Missing required entry: {column}"
                errors.append(TSError.critical(missing, message))

    return errors


def forbidden_entry_errors(ts, parsed):
    """Detect entries which are present but forbidden for the position.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    errors = []

    for position in POSITIONS:
        position_mask = (parsed["position"] == position).fillna(False)

        for column in POSITIONS[position]["forbidden"]:
            has_entry = ts[column].notna().fillna(False)
            forbidden_entry = position_mask & has_entry

            if forbidden_entry.sum():
                long_name = COLUMN_LONG_NAMES[column]
                message = f"{long_name} should be blank instead of: {{{column}}}"
                errors.append(TSError.warning(forbidden_entry, message))

    return errors


def invalid_entry_errors(ts, parsed):
    """Detect entries which are invalid for their type.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    errors = []

    for column in COLUMN_LONG_NAMES:
        invalid_entry = (ts[column].notna() & parsed[column].isna()).fillna(False)

        if invalid_entry.sum():
            long_name = COLUMN_LONG_NAMES[column]
            message = f"{long_name} entry not understood: {{{column}}}"
            errors.append(TSError.critical(invalid_entry, message))

    return errors


def date_errors(ts, parsed):
    """Detect errors in the date, like March 35.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    errors = []

    has_date_components = parsed[["year", "month", "day"]].notna().all(axis=1)
    invalid_date = parsed["date"].isna()
    invalid_day = has_date_components & invalid_date

    if invalid_day.sum():
        errors.append(
            TSError.critical(invalid_day, "Invalid day for {month} {year}: {day}")
        )

    return errors


def duration_errors(ts, parsed):
    """Detect incorrect duration for the entry's start and end times.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        List of TSError, possibly empty
    """
    times = parsed[["start", "end"]]
    times_present = times.notna().all(axis=1)
    return times_present


def timesheet_errors(ts, parsed):
    """Detect and combine all timesheet errors.

    Args:
        ts: A timesheet DataFrame
        parsed: A parsed timesheet DataFrame

    Return:
        A DataFrame with 'message', 'level', and 'row' columns.
    """
    error_functions = [
        position_errors,
        required_entry_errors,
        forbidden_entry_errors,
        invalid_entry_errors,
        date_errors,
    ]

    error_dfs = []

    # The error functions return lists and chain.from_iterable concatenates them.
    for error in chain.from_iterable(func(ts, parsed) for func in error_functions):
        messages = ts[error.mask].apply(lambda row: error.message.format(**row), axis=1)
        error_df = messages.to_frame(name="message").astype("string")
        error_df["level"] = error.level
        error_df["row"] = error_df.index
        error_dfs.append(error_df)

    return pd.concat(error_dfs, ignore_index=True)


########################################################################################
# Timesheet processing


def concat_pay_periods(workbook):
    """Concatenate all pay periods into one DataFrame.

    All values from the Excel sheet are converted to string to make parsing and error
    checks easier and consistent.

    Args:
        workbook: A Pandas ExcelFile

    Return:
        DataFrame with columns in EXCEL["usecols"], 'row', and 'period'
    """
    # The names of all pay period sheet names in the workbook.
    period_names = [name for name in workbook.sheet_names if name in PAY_PERIODS]
    period_sheets = []

    for name in period_names:
        sheet = workbook.parse(name, header=EXCEL_HEADER, usecols=EXCEL_USECOLS)

        # The timesheet has irrelevant entries in unused columns of early rows.
        # workbook.parse will properly ignore the entries, but will produce empty rows
        # if the data columns are empty. Also, filled rows can be interspersed with
        # unfilled rows.
        sheet.dropna(how="all", inplace=True)
        sheet.rename(columns=COLUMN_SHORT_NAMES, inplace=True)

        # Convert all columns to strings. Skip NA so they aren't converted to 'nan'.
        # astype(str) is necessary first in case the column contains numeric values.
        for col_name in sheet.columns:
            column = sheet[col_name]
            sheet[col_name] = column[column.notna()].apply(str).astype("string")

        # Record original row number and pay period for error messages. The +2 is
        # necessary because Excel rows are 1-indexed while Pandas DataFrames are
        # 0-indexed and the first row of the DataFrame is one after the header.
        sheet["row"] = sheet.index + EXCEL_HEADER + 2

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
        load_pickle: Load perviously pickled version if it is newer
        save_pickle: Pickle the timesheet once loaded

    Return:
        Pandas DataFrame containing all timesheet entries.
    """
    xlsx_path = pathlib.Path(path)
    pickle_path = xlsx_path.with_suffix(".pkl")

    # Load the pickled version if it exists and isn't older.
    if load_pickle and os.path.exists(pickle_path):
        if os.path.getmtime(pickle_path) >= os.path.getmtime(xlsx_path):
            try:
                timesheet = pd.read_pickle(pickle_path)
                fix_wrong_na(timesheet)

                return timesheet
            except UnpicklingError:
                logging.critical(f"Couldn't unpickle {pickle_path}. Loading original.")

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
            logging.critical(f"Unexpected semester format in filename: {semester}")
            timesheet["year"] = pd.NA
    # Incorrect number of parts in filename.
    except ValueError:
        logging.critical(f"Unexpected filename format: {path.stem}")
        timesheet["provider"] = pd.NA

    # string and Int32 are nullable.
    timesheet["provider"] = timesheet["provider"].astype("string")
    timesheet["year"] = timesheet["year"].astype("Int32")

    # Look-up month and day of the end of the pay period. The year isn't stored in
    # PAY_PERIODS in case timesheets are read after the relevant semester ends. This is
    # a bit ugly because the year is stored outside the file in the filename.
    period_end = pd.DataFrame(
        timesheet["period"].apply(lambda p: PAY_PERIODS[p]["last"]).tolist(),
        columns=["month", "day"],
    )
    period_end["year"] = timesheet["year"]
    timesheet["period_end"] = pd.to_datetime(period_end)

    if save_pickle:
        try:
            timesheet.to_pickle(pickle_path)
        except PicklingError:
            logging.critical(f"Couldn't pickle to {pickle_path}")

    return timesheet


def parse_timesheet(ts):
    """Parse timesheet entries.

    Args:
        ts: A timesheet DataFrame

    Return:
        DataFrame with parsed entries
    """
    parsed = pd.DataFrame(index=ts.index)

    parsed["position"] = normalize_position(ts["position"])
    parsed["last"] = drop_empty_strings(ts["last"])
    parsed["first"] = drop_empty_strings(ts["first"])
    parsed["course"] = drop_empty_strings(ts["course"])
    parsed["month"] = str_to_month(ts["month"])
    parsed["day"] = str_to_int(ts["day"])
    parsed["start"] = normalize_time_string(ts["start"])
    parsed["end"] = normalize_time_string(ts["end"])
    parsed["duration"] = pd.to_numeric(ts["duration"], errors="coerce")
    parsed["notes"] = drop_empty_strings(ts["notes"])

    unchanged_columns = ["row", "period", "provider", "year", "period_end"]
    parsed[unchanged_columns] = ts[unchanged_columns]

    parsed["date"] = (
        pd.to_datetime(parsed[["year", "month", "day"]].dropna(), errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .astype("string")
    )

    return parsed
