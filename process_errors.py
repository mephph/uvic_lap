import pathlib

import timesheets


def format_error(e):
    return "\t\tLine {_row_in_sheet}: {error}".format(**e)


if __name__ == "__main__":
    with open("./data/errors.txt", "w") as errors_file:
        for path in pathlib.Path("./data/").glob("*PAYROLL*.xlsx"):
            filename = path.parts[-1]
            print(f"Processing {path}")
            errors_file.write(f"{filename}\n")

            timesheet = timesheets.load(path)
            errors = timesheets.errors(timesheet)
            print(f"Found {len(errors)} errors")

            for period, group in errors.groupby("_pay_period"):
                errors_file.write(f"\t{period}\n")
                errors_text = group.apply(format_error, axis=1)
                errors_file.write(errors_text.to_csv(header=False, index=False))
                errors_file.write("\n")
