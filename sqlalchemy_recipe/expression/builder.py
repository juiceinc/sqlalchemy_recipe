"""Validate and parse expressions into SQLAlchemy expressions"""

import functools
from dataclasses import dataclass
from datetime import date, datetime

from lark import GrammarError
from sqlalchemy import func
from sqlalchemy.sql.expression import ClauseElement

from sqlalchemy_recipe.dbinfo import DBInfo, ReflectedTable
from sqlalchemy_recipe.expression.datatype import DataType
from sqlalchemy_recipe.expression.grammar import make_columns_for_table
from sqlalchemy_recipe.expression.transformer import TransformToSQLAlchemyExpression
from sqlalchemy_recipe.expression.validator import SQLALchemyValidator


@dataclass
class BuilderResponse:
    datatype: DataType
    expression: ClauseElement
    is_aggr: bool


class SQLAlchemyBuilder:
    def __init__(self, dbinfo: DBInfo, reflected_table: ReflectedTable):
        """Parse an expression by building a custom grammar that
        uses the columns in a table.

        Args:
            dbinfo (DBInfo): _description_
            table (Table): _description_
            grammar (str): _description_
        """
        self.dbinfo = dbinfo
        self.reflected_table = reflected_table
        self.drivername = self.dbinfo.engine.url.drivername
        self.transformer = TransformToSQLAlchemyExpression(
            reflected_table, self.dbinfo.engine.url.drivername
        )

    @functools.lru_cache(maxsize=None)
    def parse(
        self,
        text: str,
        forbid_aggregation: bool = False,
        enforce_aggregation: bool = False,
        convert_dates_with: str = None,
        convert_datetimes_with: str = None,
        default_aggregation=func.sum,
        debug: bool = False,
    ) -> BuilderResponse:
        """Return a parse tree for text

        Args:
            text (str): A field expression
            forbid_aggregation (bool, optional):
              The expression may not contain aggregations. Defaults to False.
            enforce_aggregation (bool, optional):
              Wrap the expression in an aggregation if one is not provided. Defaults to False.
            debug (bool, optional): Show some debug info. Defaults to False.
            convert_dates_with (str, optional): A converter to use for date fields
            convert_datetimes_with (str, optional): A converter to use for datetime fields

        Raises:
            GrammarError: A description of any errors and where they occur

        Returns:
            BuilderResponse
        """
        tree = self.reflected_table.parser.parse(text, start="col")
        validator = SQLALchemyValidator(
            text, forbid_aggregation, self.dbinfo.engine.url.drivername
        )
        # Populate errors and check aggregation
        validator.visit(tree)
        datatype = validator.last_datatype

        if validator.errors:
            if debug:
                print("".join(map(str, validator.errors)))
                print("Tree:\n" + tree.pretty())
            # Validator.errors is a list of VisitError
            ge = GrammarError("".join(map(str, validator.errors)))
            ge.validator_errors = validator.errors
            raise ge

        if debug:
            print("Tree:\n" + tree.pretty())

        self.transformer.text = text
        self.transformer.convert_dates_with = convert_dates_with
        self.transformer.convert_datetimes_with = convert_datetimes_with

        # Build an expression from the bottom of the tree up.
        expr = self.transformer.transform(tree)

        # Expressions that return literal values can't be labeled
        # Possibly we could wrap them in text() but this may be unsafe
        # instead we will disallow them.
        if isinstance(expr, (str, float, int, date, datetime)):
            raise GrammarError("Must return an expression, not a constant value")

        if (
            enforce_aggregation
            and not validator.found_aggregation
            and datatype == "num"
        ):
            return BuilderResponse(
                expression=default_aggregation(expr),
                datatype=datatype,
                is_aggr=validator.found_aggregation,
            )
        else:
            return BuilderResponse(
                expression=expr, datatype=datatype, is_aggr=validator.found_aggregation
            )
