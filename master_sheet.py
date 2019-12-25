import calendar
import datetime
import pathlib

import pandas as pd

import timesheets as ts
import timesheets2 as ts2

_MONTH_NAMES = list(map(str.lower, calendar.month_name))


def parse_date(series):
    month = _MONTH_NAMES.index(series["month"].lower())
    day = int(series["day"])
    return datetime.date(2019, month, day)


def master_sheet():
    all_timesheets = []

    for path in pathlib.Path("./data/").glob("*PAYROLL*.xlsx"):
        filename = path.parts[-1]
        _, _, last, first, _ = filename.split("_")

        print(f"Processing {first} {last} ({filename})")

        timesheet = ts.load(path)
        timesheet = timesheet[
            (timesheet["position"] == "Tutor")
            | (timesheet["position"] == "Learning Strategist")
        ]

        if len(timesheet) > 0:
            timesheet["student"] = (
                timesheet["first"].apply(str.strip).apply(str.capitalize)
                + " "
                + timesheet["last"].apply(str.strip).apply(str.capitalize)
            )
            timesheet["date"] = timesheet[["Month", "day"]].apply(parse_date, axis=1)
            timesheet["time"] = timesheet["start"].apply(ts2._parse_time)
            timesheet.drop(
                columns=[
                    "_row_in_sheet",
                    "_pay_period",
                    "_period_End Time",
                    "first",
                    "last",
                ],
                inplace=True,
            )
            timesheet["provider"] = f"{first} {last}"

            all_timesheets.append(timesheet)

    return pd.concat(all_timesheets, ignore_index=True)


def tutor_by_date(master):
    with pd.ExcelWriter("./data/tutor_by_date.xlsx") as writer:
        for tutor, entries in master.groupby("provider"):
            entries.sort_values(["date", "time"]).rename(
                columns={
                    "provider": "Provider",
                    "student": "Student",
                    "course": "Class Tutored",
                    "month": "Month",
                    "day": "Day",
                    "start": "Start Time",
                    "end": "End Time",
                    "duration": "Duration",
                    "notes": "Notes",
                }
            ).to_excel(
                writer,
                sheet_name=tutor,
                index=False,
                columns=[
                    "Student",
                    "Class Tutored",
                    "Month",
                    "Day",
                    "Start Time",
                    "End Time",
                    "Duration",
                    "Notes",
                ],
            )


def tutor_by_student(master):
    with pd.ExcelWriter("./data/tutor_by_student.xlsx") as writer:
        for tutor, entries in master.groupby("provider"):
            entries.sort_values(["student", "date", "time"]).rename(
                columns={
                    "provider": "Provider",
                    "student": "Student",
                    "course": "Class Tutored",
                    "month": "Month",
                    "day": "Day",
                    "start": "Start Time",
                    "end": "End Time",
                    "duration": "Duration",
                    "notes": "Notes",
                }
            ).to_excel(
                writer,
                sheet_name=tutor,
                index=False,
                columns=[
                    "Student",
                    "Class Tutored",
                    "Month",
                    "Day",
                    "Start Time",
                    "End Time",
                    "Duration",
                    "Notes",
                ],
            )


def student_by_date(master):
    with pd.ExcelWriter("./data/student_by_date.xlsx") as writer:
        for student, entries in master.groupby("student"):
            entries.sort_values(["date", "time"]).rename(
                columns={
                    "provider": "Provider",
                    "student": "Student",
                    "course": "Class Tutored",
                    "month": "Month",
                    "day": "Day",
                    "start": "Start Time",
                    "end": "End Time",
                    "duration": "Duration",
                    "notes": "Notes",
                }
            ).to_excel(
                writer,
                sheet_name=student,
                index=False,
                columns=[
                    "Provider",
                    "Class Tutored",
                    "Month",
                    "Day",
                    "Start Time",
                    "End Time",
                    "Duration",
                    "Notes",
                ],
            )


def student_by_tutor(master):
    with pd.ExcelWriter("./data/student_by_tutor.xlsx") as writer:
        for student, entries in master.groupby("student"):
            entries.sort_values(["provider", "date", "time"]).rename(
                columns={
                    "provider": "Provider",
                    "student": "Student",
                    "course": "Class Tutored",
                    "month": "Month",
                    "day": "Day",
                    "start": "Start Time",
                    "end": "End Time",
                    "duration": "Duration",
                    "notes": "Notes",
                }
            ).to_excel(
                writer,
                sheet_name=student,
                index=False,
                columns=[
                    "Provider",
                    "Class Tutored",
                    "Month",
                    "Day",
                    "Start Time",
                    "End Time",
                    "Duration",
                    "Notes",
                ],
            )


if __name__ == "__main__":
    master = master_sheet()
    master.to_pickle("./data/master.pkl")
    tutor_by_student(master)
    tutor_by_date(master)
    student_by_tutor(master)
    student_by_date(master)
