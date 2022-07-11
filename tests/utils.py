import csv
import io
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
test_db = os.path.join(dir_path, "data", "test_db.sqlite")


def strip_columns_from_csv(content: str, ignore_columns: list = None) -> str:
    if ignore_columns:
        content = str_dedent(content)
        rows = list(csv.DictReader(content.splitlines()))
        for row in rows:
            for col in ignore_columns:
                row.pop(col, None)
        if rows:
            csv_content = io.StringIO()
            first_row = rows[0]
            writer = csv.DictWriter(csv_content, fieldnames=list(first_row.keys()))
            writer.writeheader()
            writer.writerows(rows)
            return csv_content.getvalue().replace("\r\n", "\n").strip("\n")

    return content


def str_dedent(s: str) -> str:
    return "\n".join([x.lstrip() for x in s.split("\n")]).lstrip("\n").rstrip("\n")


def expr_to_str(expr):
    """Utility to print sql for a expression"""
    return str(expr.compile(compile_kwargs={"literal_binds": True}))
