"""Test the lark grammar used to define field expressions."""

from ast import Expr
import time

# from freezegun import freeze_time
from sqlalchemy_recipe.expression.grammar import (
    is_valid_column,
    make_columns_for_table,
    make_datatype_rule,
    make_grammar,
    make_raw_column_rules,
)
from tests.utils import str_dedent

from .test_expression_base import ExpressionTestCase

utc_offset = -1 * time.localtime().tm_gmtoff / 3600.0 + time.localtime().tm_isdst


class GrammarTestCase(ExpressionTestCase):
    def assertSelectableGrammar(self, selectable, grammar_text: str):
        actual_grammar = make_grammar(selectable)

        if grammar_text not in str_dedent(actual_grammar):
            print(
                f"\n\n\nActual:\n{str_dedent(actual_grammar)}\n\n\n\nExpected:\n{str_dedent(grammar_text)}"
            )
        self.assertTrue(grammar_text in str_dedent(actual_grammar))

    def test_make_raw_column_rules(self):
        """We get the right rules for each table"""
        expected_column_rules = [
            """
            date_0: "[" + /birth_date/i + "]"
            datetime_0: "[" + /dt/i + "]"
            num_0: "[" + /age/i + "]"
            str_0: "[" + /first/i + "]"
            str_1: "[" + /last/i + "]"
            """,
            """
            date_0: "[" + /dt/i + "]"
            num_0: "[" + /count/i + "]"
            """,
            """
            date_0: "[" + /test_date/i + "]"
            num_0: "[" + /score/i + "]"
            str_0: "[" + /username/i + "]"
            str_1: "[" + /department/i + "]"
            str_2: "[" + /testid/i + "]"
            """,
            """
            bool_0: "[" + /valid_score/i + "]"
            date_0: "[" + /test_date/i + "]"
            datetime_0: "[" + /test_datetime/i + "]"
            num_0: "[" + /score/i + "]"
            str_0: "[" + /username/i + "]"
            str_1: "[" + /department/i + "]"
            str_2: "[" + /testid/i + "]"
            """,
            """
            date_0: "[" + /test_date/i + "]"
            num_0: "[" + /score/i + "]"
            str_0: "[" + /username/i + "]"
            str_1: "[" + /department/i + "]"
            str_2: "[" + /testid/i + "]"
            """,
            """
            num_0: "[" + /score/i + "]"
            str_0: "[" + /username/i + "]"
            str_1: "[" + /tag/i + "]"
            str_2: "[" + /department/i + "]"
            str_3: "[" + /testid/i + "]"
            """,
            """
            num_0: "[" + /student_id/i + "]"
            num_1: "[" + /age/i + "]"
            num_2: "[" + /age_id/i + "]"
            num_3: "[" + /score/i + "]"
            str_0: "[" + /student/i + "]"
            """,
            """
            num_0: "[" + /age/i + "]"
            num_1: "[" + /pop2000/i + "]"
            num_2: "[" + /pop2008/i + "]"
            str_0: "[" + /state/i + "]"
            str_1: "[" + /sex/i + "]"
            """,
            """
            str_0: "[" + /id/i + "]"
            str_1: "[" + /name/i + "]"
            str_10: "[" + /census_region/i + "]"
            str_11: "[" + /census_region_name/i + "]"
            str_12: "[" + /census_division/i + "]"
            str_13: "[" + /census_division_name/i + "]"
            str_2: "[" + /abbreviation/i + "]"
            str_3: "[" + /sort/i + "]"
            str_4: "[" + /status/i + "]"
            str_5: "[" + /occupied/i + "]"
            str_6: "[" + /notes/i + "]"
            str_7: "[" + /fips_state/i + "]"
            str_8: "[" + /assoc_press/i + "]"
            str_9: "[" + /standard_federal_region/i + "]"
            unusable_0: "[" + /circuit_court/i + "]"
            """,
        ]
        for table, expected_rules in zip(self.tables, expected_column_rules):
            expected_rules = str_dedent(expected_rules)
            columns = make_columns_for_table(table)
            column_rules = str_dedent(make_raw_column_rules(columns))
            if column_rules != expected_rules:
                print(f"\n\nActual:\n{column_rules}\n\nExpected:\n{expected_rules}")
            self.assertEqual(column_rules, expected_rules)
        self.assertEqual(len(self.tables), len(expected_column_rules))

    def test_make_datatype_rule_string(self):
        """We correctly make a rule for the string datatype that gathers all
        the matching string rules"""
        expected_rules = [
            'string.1: str_0 | str_1 | foo | "(" + string + ")"',
            'string.1: foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | str_2 | foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | str_2 | foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | str_2 | foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | str_2 | str_3 | foo | "(" + string + ")"',
            'string.1: str_0 | foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | foo | "(" + string + ")"',
            'string.1: str_0 | str_1 | str_10 | str_11 | str_12 | str_13 | str_2 | str_3 | str_4 | str_5 | str_6 | str_7 | str_8 | str_9 | foo | "(" + string + ")"',
        ]
        for table, expected in zip(self.tables, expected_rules):
            columns = make_columns_for_table(table)
            actual = make_datatype_rule("string.1", columns, "str", ["foo"])
            self.assertEqual(actual, expected)
        self.assertEqual(len(self.tables), len(expected_rules))


class TestIsValidColumn(ExpressionTestCase):
    def test_is_valid_column(self):
        good_values = [
            "this",
            "that",
            "THIS",
            "THAT",
            "this_that_and_other",
            "_other",
            "THIS_that_",
        ]
        for v in good_values:
            self.assertTrue(is_valid_column(v))

        bad_values = [
            " this",
            "that ",
            " THIS",
            "TH AT  ",
            "for_slackbot}_organization_name",
        ]
        for v in bad_values:
            self.assertFalse(is_valid_column(v))
