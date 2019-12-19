""""
Utilities for CAL timesheets
"""
import calendar
import datetime as dt
import json

import dateparser
import pandas as pd

########################################################################################
# Module constants and defaults

with open("pay_periods.json") as f:
    _PAY_PERIODS = json.load(f)

with open("positions.json") as f:
    _POSITIONS = json.load(f)

# calendar.month_name is a calendar.py-specific type which only supports __get__ and
# __len__, not searching.
_MONTH_NAMES = tuple(map(str.lower, calendar.month_name))
_TODAY = dt.date.today()
_CURRENT_YEAR = dt.date.today().year

# Default arguments for Pandas ExcelFile.parse and related functions.
_EXCEL_HEADER_ROW = 1
_EXCEL_USE_COLUMNS = [
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
_EXCEL_COLUMN_TYPES = {
    "Duration": float,
}
_EXCEL_COLUMN_FILLNA = {
    "Position": "",
    "Last Name": "",
    "First Name": "",
    "Class Tutored": "",
    "Month": "",
    "Start Time": "",
    "End Time": "",
    "Notes": "",
}

_COLUMN_SHORT_NAMES = {
    "Position": "position",
    "Last Name": "last",
    "First Name": "first",
    "Class Tutored": "course",
    "Month": "month",
    "Day": "day",
    "Start Time": "start",
    "End Time": "end",
    "Duration": "duration",
    "Notes": "notes",
}
_COLUMN_LONG_NAMES = {v: k for k, v in _COLUMN_SHORT_NAMES.items()}

# Convert positions to lower case and required columns to short names.
_POSITIONS = {k.lower(): v for k, v in _POSITIONS.items()}
for position, data in _POSITIONS.items():
    data["required"] = list(map(_COLUMN_SHORT_NAMES.get, data["required"]))

########################################################################################
# Error detection


def _parse_time(string):
    """Try to parse a string as a time.

    Args:
        string: Any string

    Returns:
        A datetime.datetime object or raises a ValueError"""
    fixed = string
    # Replace 'A' with 'AM'
    if fixed[-1] in ["A", "a", "P", "p"]:
        fixed = fixed + "m"
    # Replace '4' with '4:00'
    elif len(fixed) <= 2:
        try:
            int(fixed)
            fixed = fixed + ":00"
        except ValueError:
            pass

    parsed = dateparser.parse(fixed)

    # DateParser sometimes returns None if the string is invalid, and strange strings
    # like '3:30:PM' can be interpreted as dates in 1900.
    if parsed is None or parsed.year < 2000:
        raise ValueError(f"DateParser failed to parse time: {string}")

    return parsed


def _compute_duration(start, end):
    if start > end:
        start += pd.to_timedelta("12:00:00")

    return (end - start).seconds / 3600


def _errors_in_row(row, matches=None):
    """Find all errors in a timesheet row.

    Args:
        row: A timesheet row as a Pandas Series
        matches: (optional) List of (last, first) names the provider is matched with

    Returns:
        A DataFrame with columns (pay_period, row_in_sheet, error)
    """
    errors = []

    for checker in [_errors_position, _errors_date_time]:
        errors.extend(checker(row))

    # Checking the match is a special case as it requires a list of matches.
    if matches is not None:
        errors.extend(_errors_match(row, matches))

    # The DataFrame records the original pay period and row.
    errors_df = pd.DataFrame(errors, columns=["error"])
    errors_df.insert(0, "_pay_period", row["_pay_period"])
    errors_df.insert(1, "_row_in_sheet", row["_row_in_sheet"])

    return errors_df


def _errors_position(row):
    """Check for errors in Position column.

    Args:
        row: A timesheet row as a Pandas Series

    Returns:
        List (possibly empty) of error description strings.
    """
    errors = []
    position = row["position"].lower()

    if position == "":
        errors.append("No position given.")
    elif position not in _POSITIONS:
        errors.append("Unrecognized position: {position}.".format(**row))
    else:
        # The short names of the columns required for the position.
        required = row[_POSITIONS[position]["required"]]
        # Which of the required columns are missing or blank.
        missing = required.isna() | (required == "")

        for column in map(_COLUMN_LONG_NAMES.get, required[missing].index):
            errors.append(f"Missing required entry: {column}")

    return errors


def _errors_date_time(row):
    """Check for errors in the entry's date, time, and duration.

    Args:
        row: A timesheet row as a Pandas Series

    Returns:
        List (possibly empty) of error description strings.
    """
    errors = []
    month = False
    day = False
    start = False
    end = False
    duration = False

    # Date and time can be invalid in quite a few ways, and some checks only make sense
    # if some other values are present and valid. This would be a great place for a
    # Maybe monad.

    # Start with basic type casting.
    try:
        month = _MONTH_NAMES.index(row["month"].lower())
    except ValueError:
        errors.append("Invalid month (must be full name): {month}.".format(**row))

    try:
        day = int(row["day"])
    except ValueError:
        errors.append("Invalid day (must be a number): {day}.".format(**row))

    if row["start"]:
        try:
            start = _parse_time(row["start"])
        except ValueError:
            errors.append("Invalid start time: {start}.".format(**row))

    if row["end"]:
        try:
            end = _parse_time(row["end"])
        except ValueError:
            errors.append("Invalid end time: {end}.".format(**row))

    if row["duration"]:
        try:
            duration = float(row["duration"])
        except ValueError:
            errors.append(f"Invalid duration (must be a number): {row['duration']}.")

    # More complicated validity checks
    if month and day:
        try:
            date = dt.date(_CURRENT_YEAR, month, day)

            if date > row["_period_end"]:
                errors.append(
                    "Date ({month} {day}) is past end of pay period "
                    "({_pay_period}).".format(**row)
                )
        except ValueError:
            errors.append(
                "Day is out of range for month: {month} {day}, {year}.".format(
                    year=_CURRENT_YEAR, **row
                )
            )

    if duration and duration <= 0:
        errors.append("Invalid duration (must be postive): {duration}.".format(**row))

    if start and end and start > end:
        # If 12-hour time is used without AM/PM then 1:30 is parsed as 1:30 AM and give
        # a negative duration. Assume they meant PM if this happens. If this correction
        # isn't right, the computed duration probably won't match the given duration so
        # there will still be an error.
        errors.append(
            "Start ({start}) is after end ({end}). Assuming end time is PM.".format(
                **row
            )
        )
        end += dt.timedelta(hours=12)

    if start and end and duration:
        computed_duration = (end - start).seconds / 3600

        # If the duration is computed with a formula in the sheet then duration and
        # computed_duration can be slightly different. This is probably due to how
        # Pandas handles formulas.
        if abs(duration - computed_duration) > 0.1:
            errors.append(
                "Duration ({duration}) doesn't match times ({start} - {end})".format(
                    **row
                )
            )

    return errors


def _errors_match(row, matches):
    """Check if the provider is matched with the student.

    Args:
        row: A timesheet row as a Pandas Series
        matches: (optional) A list of students matched with the provider

    Returns:
        List (possibly empty) of error description strings.
    """
    # Only check the match if the full name is given. If it isn't the required-fields
    # check will flag the error.
    if row["last"] and row["first"] and (row["last"], row["first"]) not in matches:
        return ["No record of match with {first} {last}.".format(**row)]
    else:
        return []


########################################################################################
# Timesheet processing


def _concatenate_pay_periods(workbook, *, usecols, dtype=None, fillna=None):
    """Concatenate all pay period timesheets into one DataFrame.

    Args:
        workbook: Pandas ExcelFile containing the timesheets
        usecols: List of column names
        dtype: Type, list of types, or dictionary of column names and types
        fillna: Dictionary of original column names and replacement values

    Returns:
        A DataFrame containing all entries from all pay periods, with added columns for
        originating pay period and 1-indexed row.
    """
    period_names = [name for name in workbook.sheet_names if name in _PAY_PERIODS]
    period_sheets = []

    for name in period_names:
        sheet = workbook.parse(
            name, header=_EXCEL_HEADER_ROW, usecols=usecols, dtype=dtype
        )
        # The timesheet has irrelevant entries in rows 0-12. workbook.parse will
        # properly ignore the entries, but will produce empty rows if the data columns
        # are empty. Also, filled rows can be interspersed with filled rows.
        sheet.dropna(how="all", inplace=True)

        if fillna:
            sheet.fillna(fillna, inplace=True)

        sheet.rename(columns=_COLUMN_SHORT_NAMES, inplace=True)

        # Record original row number and pay period for error messages. The +2 is
        # necessary because Excel rows are 1-indexed while Pandas DataFrames are
        # 0-indexed and the first row of the DataFrame is one after the header.
        sheet["_row_in_sheet"] = sheet.index + _EXCEL_HEADER_ROW + 2
        sheet["_pay_period"] = name
        sheet["_period_end"] = dt.date(_CURRENT_YEAR, *_PAY_PERIODS[name]["last"])
        sheet["_period_end"] = pd.to_datetime(sheet["_period_end"])

        period_sheets.append(sheet)

    return pd.concat(period_sheets, ignore_index=True)


def errors(timesheet, matches=None):
    """All errors in a timesheet.

    Args:
        timesheet: A concatenated, but not processed, timesheet

    Returns:
        DataFrame with columns '_pay_period', '_row_in_sheet', and 'error'
    """
    return pd.concat(
        # concat requires an iterable, so the Series returned by agg must be converted
        # to something like a list.
        timesheet.agg(_errors_in_row, axis=1, matches=matches).tolist(),
        ignore_index=True,
    )


def load(filename, *, usecols=None, dtype=str, fillna=None):
    """Load the timesheets in an Excel workbook into a DataFrame.

    Args:
        filename:
        usecols:
        dtype:
        fillna:"""
    if usecols is None:
        usecols = _EXCEL_USE_COLUMNS
    if fillna is None:
        fillna = _EXCEL_COLUMN_FILLNA

    workbook = pd.ExcelFile(filename)
    return _concatenate_pay_periods(
        workbook, usecols=usecols, dtype=dtype, fillna=fillna
    )
