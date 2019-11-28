"""
Utilities for CAL timesheets
"""
import calendar  # For month names and abbreviations
import datetime  # For date checking and durations

import pandas as pd

# Prefixes in lower case to make comparison easier. The [1:] slice is needed because the
# list starts with ''. The list is cast to a tuple because str.startswith requires a
# tuple.
MONTH_PREFIXES = tuple(map(str.lower, calendar.month_abbr[1:]))

# calendar.month_name is a calendar.py-specific type which only supports __get__ and
# __len__, not searching.
MONTH_NAMES = tuple(calendar.month_name)

# Default arguments for Pandas ExcelFile.parse.
EXCELFILE_HEADER_ROW = 1
EXCELFILE_USE_COLUMNS = [
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
EXCELFILE_COLUMN_TYPES = {
    "Duration": float,
}
EXCELFILE_COLUMN_FILLNA = {
    "Position": "",
    "Last Name": "",
    "First Name": "",
    "Class Tutored": "",
    "Month": "",
    "Notes": "",
}

# Valid provider positions and associated information.
# TODO: Put in external JSON file?
POSITIONS = {
    "Tutor": {
        "required": [
            "Last Name",
            "First Name",
            "Class Tutored",
            "Month",
            "Day",
            "Start Time",
            "End Time",
            "Duration",
        ],
    },
    "Learning Strategist": {
        "required": [
            "Last Name",
            "First Name",
            "Month",
            "Day",
            "Start Time",
            "End Time",
            "Duration",
        ]
    },
    "Training": {"required": ["Month", "Day", "Start Time", "End Time", "Duration"]},
    "Matching Meeting": {
        "required": ["Month", "Day", "Start Time", "End Time", "Duration"]
    },
    "Coordinator": {"required": ["Month", "Day", "Duration"]},
    "Alternative Text": {
        "required": ["Month", "Day", "Year", "Start Time", "End Time", "Duration"]
    },
    "Exam Reader": {
        "required": [
            "Last Name",
            "First Name",
            "Class Tutored",
            "Month",
            "Day",
            "Start Time",
            "End Time",
            "Duration",
        ]
    },
    "Lab Assistant": {
        "required": [
            "Last Name",
            "First Name",
            "Class Tutored",
            "Month",
            "Day",
            "Start Time",
            "End Time",
            "Duration",
        ]
    },
    "Other": {"required": ["Notes"]},
}


def concatenate_pay_periods(
    workbook, header=EXCELFILE_HEADER_ROW, usecols=None, dtype=str, fillna=None,
):
    """Concatenate all pay period timesheets into one DataFrame.

    Args:
        workbook: A Pandas ExcelFile
        header: 0-indexed row number of column headings
        usecols: List of column names to use
        dtype: Type, list of types, or dictionary of column names and types

    Returns:
        A DataFrame containing all entries from all pay periods, with added columns for
        originating pay period and 1-indexed row.
    """
    if usecols is None:
        usecols = EXCELFILE_USE_COLUMNS
    if fillna is None:
        fillna = EXCELFILE_COLUMN_FILLNA

    # Any sheet with a name starting with a month is a pay period.
    pay_period_names = [
        name for name in workbook.sheet_names if name.lower().startswith(MONTH_PREFIXES)
    ]
    pay_period_sheets = []

    for name in pay_period_names:
        sheet = workbook.parse(name, header=header, usecols=usecols)
        # The timesheet has irrelevant entries in rows 0-12. workbook.parse will
        # properly ignore the entries, but will produce empty rows if the data columns
        # are empty. Also, filled rows can be interspersed with filled rows.
        sheet.dropna(how="all", inplace=True)
        sheet.fillna(fillna, inplace=True)

        # Record original row number and pay period for error messages. The +1 is
        # necessary because Excel rows are 1-indexed while Pandas DataFrames are
        # 0-indexed.
        sheet["_row_in_sheet"] = sheet.index + header + 1
        sheet["_pay_period"] = name

        pay_period_sheets.append(sheet)

    return pd.concat(pay_period_sheets, ignore_index=True)


def _errors_in_row(row, matches=None):
    """Find all errors in a timesheet row.

    Args:
        row: A timesheet row as a Pandas Series
        matches: (optional) List of (last, first) names the provider is matched with

    Returns:
        A DataFrame with columns (pay_period, row_in_sheet, error)
    """
    errors = []

    for checker in [_errors_position, _errors_date]:
        errors.extend(checker(row))

    # Checking the match is a special case as it requires a list of matches.
    if matches is not None:
        errors.extend(_errors_match(row, matches))

    # The DataFrame records the original pay period and row.
    errors_df = pd.DataFrame(errors, columns=["error"])
    errors_df.insert(0, "pay_period", row["_pay_period"])
    errors_df.insert(1, "row_in_sheet", row["_row_in_sheet"])

    return errors_df


def _errors_position(row):
    """Check for errors in Position column.

    Args:
        row: A timesheet row as a Pandas Series

    Returns:
        List (possibly empty) of error description strings.
    """
    position = row["Position"]

    if position == "":
        return ["No position given."]
    elif position not in POSITIONS:
        return [f"Unrecognized position: {position}."]
    else:
        return []


def _errors_date(row):
    """Check for errors in the entry's date.

    Args:
        row: A timesheet row as a Pandas Series

    Returns:
        List (possibly empty) of error description strings.
    """
    errors = []

    month = row["Month"]
    month_num = 0
    day = row["Day"]
    day_num = 0

    if month == "":
        errors.append("No month given.")
    else:
        try:
            # MONTH_NAMES[0] is "", but month is not "" in this branch, so
            # MONTH_NAMES.index isn't 0.
            month_num = MONTH_NAMES.index(month)
        except ValueError:
            errors.append(f"Invalid month: {month}.")

    if day == "":
        errors.append("No day given.")
    else:
        try:
            day_num = int(day)
        except ValueError:
            errors.append(f"Invalid day (must be a number): {day}.")

    if month_num and day_num:
        # February 29 is only valid in some years, so the check must use the current
        # year.
        year = datetime.date.today().year

        try:
            datetime.date(year, month_num, day_num)
        except ValueError:
            errors.append(f"Day is out of range for month: {month} {day_num}, {year}.")

    return errors


def _errors_match(row, matches):
    """Check if the provider is matched with the student.

    Args:
        row: A timesheet row as a Pandas Series

    Returns:
        List (possibly empty) of error description strings.
    """
    first = row["First Name"]
    last = row["Last Name"]

    # Only check the match if the full name is given. If it isn't the required-fields
    # check will flag the error.
    if last and first and (last, first) not in matches:
        return [f"No record of match with {first} {last}."]
    else:
        return []
