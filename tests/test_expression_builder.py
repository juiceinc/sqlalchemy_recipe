"""Test building sqlalchemy expressions."""

import time

from freezegun import freeze_time
from lark.exceptions import GrammarError
from sqlalchemy_recipe.dbinfo import get_dbinfo

from sqlalchemy_recipe.expression.builder import SQLAlchemyBuilder
from sqlalchemy_recipe.expression.grammar import is_valid_column
from sqlalchemy_recipe.expression.validator import VisitError

from .test_expression_base import ExpressionTestCase
from .utils import expr_to_str

utc_offset = -1 * time.localtime().tm_gmtoff / 3600.0 + time.localtime().tm_isdst


class BuilderTestCase(ExpressionTestCase):
    """Add tools for building test cases from strings"""

    maxDiff = None

    def setup_builder(self, tablename):
        reflected_table = self.dbinfo.reflect(tablename)
        return SQLAlchemyBuilder(self.dbinfo, reflected_table=reflected_table)

    def setUp(self):
        super().setUp()
        self.builder = self.setup_builder("datatypes")

    def examples(self, input_rows):
        """Take input where each line looks like
        field     -> expected_sql
        #field    -> expected_sql (commented out)
        """
        for row in input_rows.split("\n"):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            if "->" in row:
                field, expected_sql = row.split("->")
            else:
                field = row
                expected_sql = "None"
            expected_sql = expected_sql.strip()
            yield field, expected_sql

    def bad_examples(self, input_rows):
        """Take input where each input is separated by three equals

        field ->
        expected_error
        ===
        field ->
        expected_error
        ===
        #field ->
        expected_error  (commented out)

        """
        for row in input_rows.split("==="):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            if "->" in row:
                field, expected_error = row.split("->")
            else:
                field = row
                expected_error = "None"

            field = field.strip()
            expected_error = expected_error.strip() + "\n"
            yield field, expected_error

    def check_bad_examples(self, bad_examples, builder=None, **parse_kwargs):
        if builder is None:
            builder = self.builder

        for field, expected_error in self.bad_examples(bad_examples):
            with self.assertRaises(Exception) as e:
                builder.parse(field, debug=True, **parse_kwargs)
                if str(e.exception).strip() != expected_error.strip():
                    print("===" * 10)
                    print(e.exception)
                    print("vs")
                    print(expected_error)
                    print("===" * 10)
                self.assertEqual(str(e.exception).strip(), expected_error.strip())

    def check_good_examples(self, good_examples, builder=None, **parse_kwargs):
        if builder is None:
            builder = self.builder

        for field, expected_sql in self.examples(good_examples):
            resp = builder.parse(field, debug=True, **parse_kwargs)
            self.assertEqual(expr_to_str(resp.expression), expected_sql)

    def check_good_examples_datatype(self, good_examples, builder=None, **parse_kwargs):
        if builder is None:
            builder = self.builder

        for field, expected_datatype in self.examples(good_examples):
            resp = builder.parse(field, debug=True, **parse_kwargs)
            self.assertEqual(resp.datatype, expected_datatype)


class SQLAlchemyBuilderTestCase(BuilderTestCase):
    def test_drivername(self):
        self.assertEqual(self.dbinfo.engine.url.drivername, "sqlite")

    def test_enforce_aggregation(self):
        """Enforce aggregation will wrap the function in a sum if no aggregation was seen"""

        good_examples = """
        [score]                         -> sum(datatypes.score)
        [ScORE]                         -> sum(datatypes.score)
        [ScORE] + [ScORE]               -> sum(datatypes.score + datatypes.score)
        max([ScORE] + [ScORE])          -> max(datatypes.score + datatypes.score)
        max([score]) - min([score])     -> max(datatypes.score) - min(datatypes.score)
        """
        self.check_good_examples(good_examples, enforce_aggregation=True)

    def test_data_type(self):
        good_examples = """
        [score]                           -> num
        [ScORE]                           -> num
        [ScORE] + [ScORE]                 -> num
        max([ScORE] + [ScORE])            -> num
        max([score]) - min([score])       -> num
        [department]                      -> str
        [department] > "foo"              -> bool
        day([test_date])                  -> date
        month([test_datetime])            -> date
        [department] > "foo" anD [score] < 22    -> bool
        min([department])                 -> str
        min([test_date])                  -> date
        count(*)                          -> num
        count([department] > "foo")       -> num
        substr([department], 5)           -> str
        substr([department], 5, 5)        -> str
        """
        self.check_good_examples_datatype(good_examples)

    def test_disallow_literals(self):
        examples = """
        "22"          -> error
        2.0           -> error
        2.0 + 1.0     -> error
        "220" + "foo" -> error
        5             -> error
        """

        for field, _ in self.examples(examples):
            with self.assertRaises(GrammarError):
                self.builder.parse(field)

    def test_selectable_census(self):
        """Test a selectable that is a orm class"""
        self.setup_builder("census")
        type_examples = """
        [age]                             -> num
        [state]                           -> str
        [pop2000] + [pop2008]             -> num
        [state] + [sex]                   -> str
        [state] = "2"                     -> bool
        max([pop2000]) > 100              -> bool
        """
        b = self.setup_builder("census")
        self.check_good_examples_datatype(type_examples, builder=b)

        sql_examples = """
        [age]                             -> census.age
        [state]                           -> census.state
        min([pop2000] + [pop2008])        -> min(census.pop2000 + census.pop2008)
        [state] + [sex]                   -> census.state || census.sex
        """
        self.check_good_examples(sql_examples, builder=b)


class SQLAlchemyBuilderConvertDatesTestaAse(BuilderTestCase):
    def test_enforce_convert_dates(self):
        """Enforce aggregation will wrap the function in a sum if no aggregation was seen"""

        good_examples = """
        [test_date]                         -> date_trunc('year', datatypes.test_date)
        [test_date]                         -> date_trunc('year', datatypes.test_date)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc('year', datatypes.test_date), '2020-01-01')
        """
        self.check_good_examples(
            good_examples, enforce_aggregation=True, convert_dates_with="year_conv"
        )

        good_examples = """
        [test_date]                         -> date_trunc('month', datatypes.test_date)
        [test_date]                         -> date_trunc('month', datatypes.test_date)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc('month', datatypes.test_date), '2020-01-01')
        """
        self.check_good_examples(
            good_examples, enforce_aggregation=True, convert_dates_with="month_conv"
        )

        # If the date conversion doesn't exist, don't convert
        good_examples = """
        [test_date]                         -> datatypes.test_date
        [test_date]                         -> datatypes.test_date
        coalesce([test_date], date("2020-01-01"))   -> coalesce(datatypes.test_date, '2020-01-01')
        """
        self.check_good_examples(
            good_examples, enforce_aggregation=True, convert_dates_with="a_potato"
        )


class TestDataTypesTable(BuilderTestCase):
    def test_fields_and_addition(self):
        """These examples should all succeed"""

        good_examples = """
        [score]                         -> datatypes.score
        [ScORE]                         -> datatypes.score
        [ScORE] + [ScORE]               -> datatypes.score + datatypes.score
        [score] + 2.0                   -> datatypes.score + 2.0
        substr([department], 5)         -> substr(datatypes.department, 5)
        substr([department], 5, 2)      -> substr(datatypes.department, 5, 2)
        #([score] + 2.0) / [score]                   -> datatypes.score + 2.0
        [username] + [department]       -> datatypes.username || datatypes.department
        "foo" + [department]            -> 'foo' || datatypes.department
        1.0 + [score]                   -> 1.0 + datatypes.score
        1.0 + [score] + [score]         -> 1.0 + datatypes.score + datatypes.score
        -0.1 * [score] + 600            -> -0.1 * datatypes.score + 600
        -0.1 * [score] + 600.0          -> -0.1 * datatypes.score + 600.0
        [score] = [score]               -> datatypes.score = datatypes.score
        [score] >= 2.0                  -> datatypes.score >= 2.0
        2.0 <= [score]                  -> datatypes.score >= 2.0
        NOT [score] >= 2.0              -> datatypes.score < 2.0
        NOT 2.0 <= [score]              -> datatypes.score < 2.0
        [score] > 3 AND true            -> datatypes.score > 3
        [valid_score] AND [score] > 3   -> datatypes.valid_score AND datatypes.score > 3
        # This is a bad case
        # what happens is TRUE AND score > 3 gets simplified to score > 3
        [valid_score] = TRUE AND [score] > 3 -> datatypes.valid_score = (datatypes.score > 3)
        # Parentheses make this work
        ([valid_score] = TRUE) AND [score] > 3 -> datatypes.valid_score = true AND datatypes.score > 3
        [score] = Null                  -> datatypes.score IS NULL
        [score] IS NULL                 -> datatypes.score IS NULL
        [score] != Null                 -> datatypes.score IS NOT NULL
        [score] <> Null                 -> datatypes.score IS NOT NULL
        [score] IS NOT nULL             -> datatypes.score IS NOT NULL
        [department] like "foo"         -> datatypes.department LIKE '%foo%'
        [department] ilike "foo%"       -> lower(datatypes.department) LIKE lower('foo%')
        "F" + [department] ILIKE "f__"  -> lower('F' || datatypes.department) LIKE lower('f__')
        string([score])                 -> CAST(datatypes.score AS VARCHAR)
        coalesce([score], 0.14)         -> coalesce(datatypes.score, 0.14)
        int([department])               -> CAST(datatypes.department AS INTEGER)
        coalesce([department], "moo")   -> coalesce(datatypes.department, 'moo')
        coalesce([test_date], date("2020-01-01"))   -> coalesce(datatypes.test_date, '2020-01-01')
        """
        self.check_good_examples(good_examples)

    def test_division_and_math(self):
        """These examples should all succeed"""

        good_examples = """
        [score] / 2                      -> CAST(datatypes.score AS FLOAT) / 2
        [score] / 2.0                    -> CAST(datatypes.score AS FLOAT) / 2.0
        sum([score]) / count(*)            -> CASE WHEN (count(*) = 0) THEN NULL ELSE CAST(sum(datatypes.score) AS FLOAT) / CAST(count(*) AS FLOAT) END
        [score] / 1                      -> datatypes.score
        sum([score] / 1)                 -> sum(datatypes.score)
        sum([score] / [score])           -> sum(CASE WHEN (datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score AS FLOAT) / CAST(datatypes.score AS FLOAT) END)
        [score] / 2                        -> CAST(datatypes.score AS FLOAT) / 2
        sum([score] / [score])               -> sum(CASE WHEN (datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score AS FLOAT) / CAST(datatypes.score AS FLOAT) END)
        [score] / (2/1)                  -> CAST(datatypes.score AS FLOAT) / 2
        [score] / (0.5/0.25)             -> CAST(datatypes.score AS FLOAT) / 2.0
        [score] / (0.5 /    0.25)        -> CAST(datatypes.score AS FLOAT) / 2.0
        [score] * (2*3)                  -> datatypes.score * 6
        [score] * (2*[score])              -> datatypes.score * 2 * datatypes.score
        [score] * (2 / [score])            -> datatypes.score * CASE WHEN (datatypes.score = 0) THEN NULL ELSE 2 / CAST(datatypes.score AS FLOAT) END
        [score] / (10-7)                 -> CAST(datatypes.score AS FLOAT) / 3
        [score] / (10-9)                 -> datatypes.score
        ([score] + [score]) / ([score] - [score]) -> CASE WHEN (datatypes.score - datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score + datatypes.score AS FLOAT) / CAST(datatypes.score - datatypes.score AS FLOAT) END
        # Order of operations has: score + (3 + (5 / 5))
        [score] + (3 + 5 / (10 - 5))       -> datatypes.score + 4.0
        # Order of operations has: score + (3 + 0.5 - 5)
        [score] + (3 + 5 / 10 - 5)         -> datatypes.score + -1.5
        """
        self.check_good_examples(good_examples)

    def test_arrays(self):
        good_examples = """
        [score] NOT in (1,2,3)            -> (datatypes.score NOT IN (1, 2, 3))
        [score] In (1,2,   3.0)           -> datatypes.score IN (1, 2, 3)
        [score] In (1)                    -> datatypes.score IN (1)
        NOT [score] In (1)                -> (datatypes.score NOT IN (1))
        NOT NOT [score] In (1)            -> datatypes.score IN (1)
        [department] In ("A", "B")        -> datatypes.department IN ('A', 'B')
        [department] In ("A", "B",)       -> datatypes.department IN ('A', 'B')
        [department] iN  (  "A",    "B" ) -> datatypes.department IN ('A', 'B')
        [department] In ("A",)            -> datatypes.department IN ('A')
        [department] In ("A")             -> datatypes.department IN ('A')
        [department] + [username] In ("A", "B")        -> datatypes.department || datatypes.username IN ('A', 'B')
        """
        self.check_good_examples(good_examples)

    def test_boolean(self):
        good_examples = """
        [score] > 3                                           -> datatypes.score > 3
        [department] > "b"                                    -> datatypes.department > 'b'
        string([score]) like "9_"                             -> CAST(datatypes.score AS VARCHAR) LIKE '9_'
        [score] > 3 AND [score] < 5                           -> datatypes.score > 3 AND datatypes.score < 5
        [score] > 3 AND [score] < 5 AND [score] = 4           -> datatypes.score > 3 AND datatypes.score < 5 AND datatypes.score = 4
        [score] > 3 AND True                                  -> datatypes.score > 3
        [score] > 3 AND False                                 -> false
        NOT [score] > 3 AND [score] < 5                       -> NOT (datatypes.score > 3 AND datatypes.score < 5)
        NOT ([score] > 3 AND [score] < 5)                     -> NOT (datatypes.score > 3 AND datatypes.score < 5)
        (NOT [score] > 3) AND [score] < 5                     -> datatypes.score <= 3 AND datatypes.score < 5
        # The following is a unexpected result but not sure how to fix it
        NOT [score] > 3 AND NOT [score] < 5                   ->  NOT (datatypes.score > 3 AND datatypes.score >= 5)
        [score] > 3 OR [score] < 5                            -> datatypes.score > 3 OR datatypes.score < 5
        [score] > 3 AND [score] < 5 OR [score] = 4            -> datatypes.score > 3 AND datatypes.score < 5 OR datatypes.score = 4
        [score] > 3 AND ([score] < 5 OR [score] = 4)          -> datatypes.score > 3 AND (datatypes.score < 5 OR datatypes.score = 4)
        [score] > 3 AND [score] < 5 OR [score] = 4 AND [score] = 3 -> datatypes.score > 3 AND datatypes.score < 5 OR datatypes.score = 4 AND datatypes.score = 3
        [score] > 3 AND ([score] < 5 OR [score] = 4) AND [score] = 3 -> datatypes.score > 3 AND (datatypes.score < 5 OR datatypes.score = 4) AND datatypes.score = 3
        [score] between 1 and 3                               -> datatypes.score BETWEEN 1 AND 3
        [score] between [score] and [score]                   -> datatypes.score BETWEEN datatypes.score AND datatypes.score
        [username] between "a" and "z"                        -> datatypes.username BETWEEN 'a' AND 'z'
        [username] between [department] and "z"               -> datatypes.username BETWEEN datatypes.department AND 'z'
        count_distinct([score] > 80)                          -> count(DISTINCT (datatypes.score > 80))
        count([score] > 80)                                   -> count(datatypes.score > 80)
        """
        self.check_good_examples(good_examples)

    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
unknown ->
unknown is not a valid column name

unknown
^
===
[scores] ->
scores is not a valid column name

[scores]
 ^
===
[scores] + -1.0 ->
scores is not a valid column name

[scores] + -1.0
 ^
unknown_col and num can not be added together

[scores] + -1.0
 ^
===
2.0 + [scores] ->
scores is not a valid column name

2.0 + [scores]
       ^
num and unknown_col can not be added together

2.0 + [scores]
^
===
[foo_b] ->
foo_b is not a valid column name

[foo_b]
 ^
===
[username] + [score] ->
string and num can not be added together

[username] + [score]
 ^
===
[username]-[score] ->
string and num can not be subtracted

[username]-[score]
 ^
===
[username] * [score] ->
string and num can not be multiplied together

[username] * [score]
 ^
===
[score] * [username] ->
num and string can not be multiplied together

[score] * [username]
 ^
===
[score]   + [department] ->
num and string can not be added together

[score]   + [department]
 ^
===
[score] = [department] ->
Can't compare num to str

[score] = [department]
 ^
===
[score] = "5" ->
Can't compare num to str

[score] = "5"
 ^
===
[department] = 3.24 ->
Can't compare str to num

[department] = 3.24
 ^
===
[department] In ("A", 2) ->
An array may not contain both strings and numbers

[department] In ("A", 2)
                 ^
===
[username] NOT IN (2, "B") ->
An array may not contain both strings and numbers

[username] NOT IN (2, "B")
                   ^
===
1 in (1,2,3) ->
Must be a column or expression

1 in (1,2,3)
^
===
NOT [department] ->
NOT requires a boolean value

NOT [department]
^
===
[score] / 0 ->
When dividing, the denominator can not be zero
===
[score] / (10-10) ->
When dividing, the denominator can not be zero
===
avg([department]) ->
A str can not be aggregated using avg.

avg([department])
^
===
avg([test_date]) ->
A date can not be aggregated using avg.

avg([test_date])
^
"""
        self.check_bad_examples(bad_examples)


class DataTypesTableDatesTestCase(BuilderTestCase):
    @freeze_time("2020-01-14 09:21:34", tz_offset=utc_offset)
    def test_dates(self):
        good_examples = """
        [test_date]           -> datatypes.test_date
        [test_date] > date("2020-01-01")     -> datatypes.test_date > '2020-01-01'
        [test_date] > date("today")          -> datatypes.test_date > '2020-01-14'
        date("today") < [test_date]          -> datatypes.test_date > '2020-01-14'
        [test_date] > date("1 day ago")      -> datatypes.test_date > '2020-01-13'
        [test_date] > date("1 day")          -> datatypes.test_date > '2020-01-13'
        [test_date] > date("1 days ago")     -> datatypes.test_date > '2020-01-13'
        [test_date] between date("2020-01-01") and date("2020-01-30")      -> datatypes.test_date BETWEEN '2020-01-01' AND '2020-01-30'
        [test_date] IS last year              -> datatypes.test_date BETWEEN '2019-01-01' AND '2019-12-31'
        [test_datetime] > date("1 days ago")  -> datatypes.test_datetime > '2020-01-13 09:21:34'
        [test_datetime] between date("2020-01-01") and date("2020-01-30")      -> datatypes.test_datetime BETWEEN '2020-01-01 00:00:00' AND '2020-01-30 23:59:59.999999'
        [test_datetime] IS last year          -> datatypes.test_datetime BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 23:59:59.999999'
        [test_datetime] IS next year          -> datatypes.test_datetime BETWEEN '2021-01-01 00:00:00' AND '2021-12-31 23:59:59.999999'
        # The date() wrapper function is optional
        [test_date] > "1 days ago"            -> datatypes.test_date > '2020-01-13'
        [test_datetime] > "1 days ago"        -> datatypes.test_datetime > '2020-01-13 09:21:34'
        [test_date] between "30 days ago" and "now" -> datatypes.test_date BETWEEN '2019-12-15' AND '2020-01-14'
        [test_date] between date("30 days ago") and date("now") -> datatypes.test_date BETWEEN '2019-12-15' AND '2020-01-14'
        [test_datetime] between date("30 days ago") and date("now") -> datatypes.test_datetime BETWEEN '2019-12-15 09:21:34' AND '2020-01-14 09:21:34'
        """
        self.check_good_examples(good_examples)

    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
[test_date] > date("1 day from now") ->

Can't convert '1 day from now' to a date.
===
[test_date] between date("2020-01-01") and 7 ->
When using between, the column (date) and between values (date, num) must be the same data type.

[test_date] between date("2020-01-01") and 7
 ^
===
[test_date] between "potato" and date("2020-01-01") ->
Can't convert 'potato' to a date.
"""
        self.check_bad_examples(bad_examples)


class TestDataTypesTableDatesInBigquery(BuilderTestCase):
    """Test generating "bigquery" flavored expressions."""

    def setUp(self):
        super().setUp()
        self.builder.drivername = "bigquery"
        self.builder.transformer.drivername = "bigquery"

    def test_dates_without_freetime(self):
        """bigquery generates different sql for date conversions"""
        good_examples = """
        month([test_date]) > date("2020-12-30")          -> date_trunc(datatypes.test_date, month) > '2020-12-30'
        month([test_datetime]) > date("2020-12-30")      -> datetime(timestamp_trunc(datatypes.test_datetime, month)) > '2020-12-30'
        date("2020-12-30") < month([test_datetime])      -> datetime(timestamp_trunc(datatypes.test_datetime, month)) > '2020-12-30'
        day([test_date]) > date("2020-12-30")            -> date_trunc(datatypes.test_date, day) > '2020-12-30'
        week([test_date]) > date("2020-12-30")           -> date_trunc(datatypes.test_date, week(monday)) > '2020-12-30'
        quarter([test_date]) > date("2020-12-30")        -> date_trunc(datatypes.test_date, quarter) > '2020-12-30'
        year([test_date]) > date("2020-12-30")           -> date_trunc(datatypes.test_date, year) > '2020-12-30'
        date([test_datetime])                            -> datetime(timestamp_trunc(datatypes.test_datetime, day))
        """
        self.check_good_examples(good_examples)

    # def test_percentiles(self):
    #     good_examples = f"""
    #     percentile1([score])                 -> sum(datatypes.score)
    #     """
    #     self.check_good_examples(good_examples)


class TestAggregations(BuilderTestCase):
    def test_allow_aggregation(self):
        # Can't tests with date conversions and freeze time :/
        good_examples = """
        sum([score])                                         -> sum(datatypes.score)
        sum([score])                                         -> sum(datatypes.score)
        sum([score]*2.0)                                     -> sum(datatypes.score * 2.0)
        avg([score])                                         -> avg(datatypes.score)
        min([test_date])                                     -> min(datatypes.test_date)
        max([test_datetime])                                 -> max(datatypes.test_datetime)
        max([score]) - min([score])                          -> max(datatypes.score) - min(datatypes.score)
        count_distinct([score])                              -> count(DISTINCT datatypes.score)
        count_distinct([department])                         -> count(DISTINCT datatypes.department)
        count_distinct([department])                         -> count(DISTINCT datatypes.department)
        count_distinct([department] = "MO" AND [score] > 20) -> count(DISTINCT (datatypes.department = 'MO' AND datatypes.score > 20))
        count_distinct(if([department] = "MO" AND [score] > 20, [department])) -> count(DISTINCT CASE WHEN (datatypes.department = 'MO' AND datatypes.score > 20) THEN datatypes.department END)
        count(IF([department] = "MO" AND [score] > 20, [department])) -> count(CASE WHEN (datatypes.department = 'MO' AND datatypes.score > 20) THEN datatypes.department END)
        count(*)                     -> count(*)
        """
        self.check_good_examples(good_examples)

    def test_forbid_aggregation(self):
        """These examples should all fail"""
        bad_examples = """
sum([score]) ->
Aggregations are not allowed in this field.

sum([score])
^
===
sum([score]) ->
Aggregations are not allowed in this field.

sum([score])
^
===
sum([department]) ->
A str can not be aggregated using sum.

sum([department])
^
===
2.1235 + sum([department]) ->
A str can not be aggregated using sum.

2.1235 + sum([department])
         ^
===
sum([score]) + sum([department]) ->
Aggregations are not allowed in this field.

sum([score]) + sum([department])
^
A str can not be aggregated using sum.

sum([score]) + sum([department])
               ^
===
sum([score]) + sum([department]) ->
Aggregations are not allowed in this field.

sum([score]) + sum([department])
^
A str can not be aggregated using sum.

sum([score]) + sum([department])
               ^
"""
        self.check_bad_examples(bad_examples, forbid_aggregation=True)

    def test_bad_aggregations(self):
        """These examples should all fail"""

        bad_examples = """
sum([department]) ->
A str can not be aggregated using sum.

sum([department])
^
===
2.1235 + sum([department]) ->
A str can not be aggregated using sum.

2.1235 + sum([department])
         ^
===
sum([score]) + sum([department]) ->
A str can not be aggregated using sum.

sum([score]) + sum([department])
               ^
===
percentile1([score]) ->
Percentile is not supported on sqlite

percentile1([score])
^
===
percentile13([score]) ->
Percentile values of 13 are not supported.

percentile13([score])
^
Percentile is not supported on sqlite

percentile13([score])
^
"""
        self.check_bad_examples(bad_examples)

    # def test_percentiles(self):
    #     # TODO: build these tests
    #     # Can't test with sqlalchemy
    #     good_examples = f"""
    #     #percentile1([score])                 -> sum(datatypes.score)
    #     """
    #     self.check_good_examples(good_examples)
    #     for field, expected_sql in self.examples(good_examples):
    #         expr, _ = self.builder.parse(field, debug=True)
    #         self.assertEqual(expr_to_str(expr), expected_sql)


class TestIf(BuilderTestCase):
    def test_if(self):
        good_examples = f"""
        # Number if statements
        if([valid_score], [score], -1)                                                             -> CASE WHEN datatypes.valid_score THEN datatypes.score ELSE -1 END
        if([score] > 2, [score], -1)                                                               -> CASE WHEN (datatypes.score > 2) THEN datatypes.score ELSE -1 END
        if([score] > 2, [score])                                                                   -> CASE WHEN (datatypes.score > 2) THEN datatypes.score END
        if([score] > 2, [score]) + if([score] > 4, 1)                                              -> CASE WHEN (datatypes.score > 2) THEN datatypes.score END + CASE WHEN (datatypes.score > 4) THEN 1 END
        if([score] > 2, [score] + if([score] > 4, 1))                                              -> CASE WHEN (datatypes.score > 2) THEN datatypes.score + CASE WHEN (datatypes.score > 4) THEN 1 END END
        if([score] > 2, [score], [score] > 4, [score]*2.0, -5)                                     -> CASE WHEN (datatypes.score > 2) THEN datatypes.score WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 ELSE -5 END
        if([score] > 2, null, [score] > 4, [score]*2.0, -5)                                        -> CASE WHEN (datatypes.score > 2) THEN NULL WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 ELSE -5 END
        if([score] > 2, null, [score] > 4, [score]*2.0, NULL)                                      -> CASE WHEN (datatypes.score > 2) THEN NULL WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 END
        if([score] > 2, [SCORE]/2.24, [score] > 4, [score]*2.0, [score] > 6.0, [score]*3.5, NULL)  -> CASE WHEN (datatypes.score > 2) THEN CAST(datatypes.score AS FLOAT) / 2.24 WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 WHEN (datatypes.score > 6.0) THEN datatypes.score * 3.5 END
        if([score] > 2 OR [score] = 1, [score]*3.5)                                                -> CASE WHEN (datatypes.score > 2 OR datatypes.score = 1) THEN datatypes.score * 3.5 END
        # String if statements
        if([department] = "Radiology", "XDR-Radiology")                                            -> CASE WHEN (datatypes.department = 'Radiology') THEN 'XDR-Radiology' END
        if([score] > 2, "XDR-Radiology")                                                           -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' END
        if([score] > 2, "XDR-Radiology", "OTHERS")                                                 -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' END
        if([score] > 2, "XDR-Radiology", "OTHERS"+[department])                                    -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' || datatypes.department END
        if([score] > 2, "XDR-Radiology", "OTHERS") + [department]                                  -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' END || datatypes.department
        # This is actually an error, but we allow it for now
        if([score] > 2, NULL, "OTHERS") + [department]                                             -> CASE WHEN (datatypes.score > 2) THEN NULL ELSE 'OTHERS' END || datatypes.department
        if([score] > 2, [department], [score] > 4, [username], "OTHERS")                           -> CASE WHEN (datatypes.score > 2) THEN datatypes.department WHEN (datatypes.score > 4) THEN datatypes.username ELSE 'OTHERS' END
        # Date if statements
        if([score] > 2, [test_date])                                                               -> CASE WHEN (datatypes.score > 2) THEN datatypes.test_date END
        month(if([score] > 2, [test_date]))                                                        -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_date END)
        if([test_date] > date("2020-01-01"), [test_date])                                          -> CASE WHEN (datatypes.test_date > '2020-01-01') THEN datatypes.test_date END
        # Datetime if statements
        if([score] > 2, [test_datetime])                                                           -> CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END
        month(if([score] > 2, [test_datetime]))                                                    -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if([test_datetime] > date("2020-01-01"), [test_datetime])                                  -> CASE WHEN (datatypes.test_datetime > '2020-01-01 00:00:00') THEN datatypes.test_datetime END
        month(if([score] > 2, [test_datetime]))                                                    -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if([score]<2,"babies",[score]<13,"children",[score]<20,"teens","oldsters")                 -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if(([score])<2,"babies",([score])<13,"children",([score])<20,"teens","oldsters")           -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if([department] = "1", [score], [department]="2", [score]*2)                               -> CASE WHEN (datatypes.department = '1') THEN datatypes.score WHEN (datatypes.department = '2') THEN datatypes.score * 2 END
        """
        self.check_good_examples(good_examples)

    def test_failing_if(self):
        """These examples should all fail"""

        bad_examples = """
if([department], [score]) ->
This should be a boolean column or expression

if([department], [score])
    ^
===
if([department] = 2, [score]) ->
Can't compare str to num

if([department] = 2, [score])
    ^
===
if([department] = "1", [score], [department], [score]*2) ->
This should be a boolean column or expression

if([department] = "1", [score], [department], [score]*2)
                                 ^
===
if([department] = "1", [score], [valid_score], [score]*2, [department], 12.5) ->
This should be a boolean column or expression

if([department] = "1", [score], [valid_score], [score]*2, [department], 12.5)
                                                           ^
===
if([department], [score], [valid_score], [score]*2) ->
This should be a boolean column or expression

if([department], [score], [valid_score], [score]*2)
    ^
===
if([department] = "foo", [score], [valid_score], [department]) ->
The values in this if statement must be the same type, not num and str

if([department] = "foo", [score], [valid_score], [department])
                                                  ^
===
if([department] = "foo", [department], [valid_score], [score]) ->
The values in this if statement must be the same type, not str and num

if([department] = "foo", [department], [valid_score], [score])
                                                       ^
"""
        self.check_bad_examples(bad_examples)

    def test_validator_errors(self):
        """These examples should all fail"""
        expr = "if([department], [score])"
        with self.assertRaises(Exception) as e:
            self.builder.parse(expr, debug=True)
            self.assertEqual(len(e.validator_errors), 1)
            self.assertIsInstance(e.validator_errors[0], VisitError)
            self.assertEqual(e.validator_errors[0].pos, 20)


class TestSQLAlchemySerialize(BuilderTestCase):
    """Test we can serialize and deserialize parsed results using
    sqlalchemy.ext.serialize. This is important because parsing is
    costly."""

    def test_ser_deser(self):
        # Can't tests with date conversions and freeze time :/
        good_examples = """
        sum([score])                 -> sum(datatypes.score)
        sum([score])                 -> sum(datatypes.score)
        month(if([score] > 2, [test_datetime]))                                          -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if([test_datetime] > date("2020-01-01"), [test_datetime])                        -> CASE WHEN (datatypes.test_datetime > '2020-01-01 00:00:00') THEN datatypes.test_datetime END
        month(if([score] > 2, [test_datetime]))                                          -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if([score]<2,"babies",[score]<13,"children",[score]<20,"teens","oldsters")       -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if(([score])<2,"babies",([score])<13,"children",([score])<20,"teens","oldsters") -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        """
        from sqlalchemy.ext.serializer import dumps, loads

        for field, expected_sql in self.examples(good_examples):
            resp = self.builder.parse(field, forbid_aggregation=False, debug=True)
            ser = dumps(resp.expression)
            expr = loads(
                ser, self.builder.dbinfo.metadata, engine=self.builder.dbinfo.engine
            )
            self.assertEqual(expr_to_str(expr), expected_sql)
