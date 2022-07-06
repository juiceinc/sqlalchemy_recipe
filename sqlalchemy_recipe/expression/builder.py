BUILDER_CACHE = {}


class SQLAlchemyBuilder(object):
    @classmethod
    def get_builder(cls, selectable):
        if selectable not in BUILDER_CACHE:
            BUILDER_CACHE[selectable] = cls(selectable)
        return BUILDER_CACHE[selectable]

    @classmethod
    def clear_builder_cache(cls):
        global BUILDER_CACHE
        BUILDER_CACHE = {}

    def __init__(self, engine, selectable):
        """Parse a recipe field by building a custom grammar that
        uses the colums in a selectable.

        Args:
            selectable (Table): A SQLAlchemy selectable
        """
        self.engine = engine
        self.selectable = selectable
        # Database driver
        try:
            self.drivername = engine.url.drivername
        except Exception:
            self.drivername = "unknown"

        self.columns = make_columns_for_selectable(selectable)
        self.grammar = make_grammar(self.columns)
        self.parser = Lark(
            self.grammar,
            parser="earley",
            ambiguity="resolve",
            start="col",
            propagate_positions=True,
            # predict_all=True,
        )
        self.transformer = TransformToSQLAlchemyExpression(
            self.selectable, self.columns, self.drivername
        )

        # The data type of the last parsed expression
        self.last_datatype = None

    @functools.lru_cache(maxsize=None)
    def parse(
        self,
        text,
        forbid_aggregation=False,
        enforce_aggregation=False,
        debug=False,
        convert_dates_with=None,
        convert_datetimes_with=None,
    ):
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
        tree = self.parser.parse(text, start="col")
        validator = SQLALchemyValidator(text, forbid_aggregation, self.drivername)
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
                return (func.sum(expr), self.last_datatype)
            else:
                return (expr, self.last_datatype)
