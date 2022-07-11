"""Validate and parse expressions into SQLAlchemy expressions"""

from dataclasses import dataclass
import functools
from sqlalchemy import Table, func
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy_recipe.dbinfo import DBInfo, ReflectedTable
from lark import GrammarError, Lark
from datetime import datetime, date
from sqlalchemy_recipe.expression.datatype import DataType

from sqlalchemy_recipe.expression.grammar import make_columns_for_table
from .transformer import TransformToSQLAlchemyExpression
from .validator import SQLALchemyValidator

BUILDER_CACHE = {}


@dataclass
class BuilderResponse:
    datatype: DataType
    expression: ClauseElement


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

        # The data type of the last parsed expression
        self.last_datatype = None

    @functools.lru_cache(maxsize=None)
    def parse(
        self,
        text: str,
        forbid_aggregation: bool = False,
        enforce_aggregation: bool = False,
        debug: bool = False,
        convert_dates_with=None,
        convert_datetimes_with=None,
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
            A tuple of
                ColumnElement: A SQLALchemy expression
                DataType: The datatype of the expression (bool, date, datetime, num, str)
        """
        tree = self.reflected_table.parser.parse(text, start="col")
        validator = SQLALchemyValidator(
            text, forbid_aggregation, self.dbinfo.engine.url.drivername
        )
        validator.visit(tree)
        self.last_datatype = validator.last_datatype

        if validator.errors:
            if debug:
                print("".join(validator.errors))
                print("Tree:\n" + tree.pretty())
            raise GrammarError("".join(validator.errors))
        else:
            if debug:
                print("Tree:\n" + tree.pretty())
            self.transformer.text = text
            self.transformer.convert_dates_with = convert_dates_with
            self.transformer.convert_datetimes_with = convert_datetimes_with
            expr = self.transformer.transform(tree)

            # Expressions that return literal values can't be labeled
            # Possibly we could wrap them in text() but this may be unsafe
            # instead we will disallow them.
            if isinstance(expr, (str, float, int, date, datetime)):
                raise GrammarError("Must return an expression, not a constant value")

            if (
                enforce_aggregation
                and not validator.found_aggregation
                and self.last_datatype == "num"
            ):
                return BuilderResponse(
                    expression=func.sum(expr), datatype=self.last_datatype
                )
            else:
                return BuilderResponse(expression=expr, datatype=self.last_datatype)
