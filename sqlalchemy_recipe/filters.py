ALLOWED_OPERATORS = {
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "is",
    "isnot",
    "like",
    "ilike",
    "quickselect",
    "in",
    "notin",
    "between",
}


def is_nested_condition(v) -> bool:
    return isinstance(v, dict) and "operator" in v and "value" in v


def contains_complex_values(v: List) -> bool:
    """Check if any of the values in a list requires special handling to filter on"""
    return None in v or any(map(is_nested_condition, v))


def _build_scalar_filter(value, operator=None, target_role=None):
    """Build a Filter given a single value.

    Args:

        value (a string, number, boolean or None):
        operator (`str`)
            A valid scalar operator. The default operator
            is `eq`
        target_role (`str`)
            An optional role to build the filter against

    Returns:

        A Filter object
    """
    # Developer's note: Valid operators should appear in ALLOWED_OPERATORS
    # This is used by the AutomaticFilter extension.
    if operator is None:
        operator = "eq"
    if target_role and target_role in self.roles:
        filter_column = self.roles.get(target_role)
        datatype = determine_datatype(self, target_role)
    else:
        filter_column = self.columns[0]
        datatype = determine_datatype(self)

    # Ensure that the filter_column and value have compatible data types

    # Support passing ILIKE in Paginate extensions
    if datatype == "date":
        value = convert_date(value)
    elif datatype == "datetime":
        value = convert_datetime(value)

    if isinstance(value, str) and datatype != "str":
        filter_column = cast(filter_column, String)

    if operator == "eq":
        # Default operator is 'eq' so if no operator is provided, handle
        # like an 'eq'
        if value is None:
            return filter_column.is_(value)
        else:
            return filter_column == value
    if operator == "ne":
        return filter_column != value
    elif operator == "lt":
        return filter_column < value
    elif operator == "lte":
        return filter_column <= value
    elif operator == "gt":
        return filter_column > value
    elif operator == "gte":
        return filter_column >= value
    elif operator == "is":
        return filter_column.is_(value)
    elif operator == "isnot":
        return filter_column.isnot(value)
    elif operator == "like":
        value = str(value)
        return filter_column.like(value)
    elif operator == "ilike":
        value = str(value)
        return filter_column.ilike(value)
    elif operator == "quickselect":
        for qs in self.quickselects:
            if qs.get("name") == value:
                return qs.get("condition")
        raise ValueError(
            "quickselect {} was not found in " "ingredient {}".format(value, self.id)
        )
    else:
        raise ValueError("Unknown operator {}".format(operator))


def _build_vector_filter(self, value, operator=None, target_role=None):
    """Build a Filter given a list of values.

    Args:

        value (a list of string, number, boolean or None):
        operator (:obj:`str`)
            A valid vector operator. The default operator is
            `in`.
        target_role (`str`)
            An optional role to build the filter against

    Returns:

        A Filter object
    """

    # Developer's note: Valid operators should appear in ALLOWED_OPERATORS
    # This is used by the AutomaticFilter extension.
    if operator is None:
        operator = "in"
    if target_role and target_role in self.roles:
        filter_column = self.roles.get(target_role)
        datatype = determine_datatype(self, target_role)
    else:
        filter_column = self.columns[0]
        datatype = determine_datatype(self)

    if datatype == "date":
        value = list(map(convert_date, value))
    elif datatype == "datetime":
        value = list(map(convert_datetime, value))

    if operator == "and":
        conditions = [self.build_filter(x["value"], x["operator"]) for x in value]
        return and_(*conditions)

    if operator in ("in", "notin"):
        if contains_complex_values(value):
            # A list may contain additional operators or nones
            # Convert from:
            # department__in: [None, "A", 'B"]
            #
            # to the SQL
            #
            # department in ("A", "B") OR department is null
            simple_values = sorted(
                [v for v in value if v is not None and not is_nested_condition(v)]
            )
            nested_conditions = [v for v in value if is_nested_condition(v)]
            conditions = []
            if None in value:
                conditions.append(filter_column.is_(None))
            if simple_values:
                conditions.append(filter_column.in_(simple_values))
            conditions.extend(
                self.build_filter(cond["value"], operator=cond["operator"])
                for cond in nested_conditions
            )
            cond = or_(*conditions)

        else:
            # Sort to generate deterministic query sql for caching
            cond = filter_column.in_(sorted(value))

        return not_(cond) if operator == "notin" else cond

    elif operator == "between":
        if len(value) != 2:
            ValueError(
                "When using between, you can only supply a " "lower and upper bounds."
            )
        lower_bound, upper_bound = value
        return between(filter_column, lower_bound, upper_bound)
    elif operator == "quickselect":
        qs_conditions = []
        for v in value:
            qs_found = False
            for qs in self.quickselects:
                if qs.get("name") == v:
                    qs_found = True
                    qs_conditions.append(qs.get("condition"))
                    break
            if not qs_found:
                raise ValueError(
                    "quickselect {} was not found in "
                    "ingredient {}".format(value, self.id)
                )
        return or_(*qs_conditions)
    else:
        raise ValueError("Unknown operator {}".format(operator))


def build_filter(self, value, operator=None, target_role=None):
    """
    Builds a filter based on a supplied value and optional operator. If
    no operator is supplied an ``in`` filter will be used for a list and a
    ``eq`` filter if we get a scalar value.

    ``build_filter`` is used by the AutomaticFilter extension.

    Args:

        value:
            A value or list of values to operate against
        operator (:obj:`str`)
            An operator that determines the type of comparison
            to do against value.

            The default operator is 'in' if value is a list and
            'eq' if value is a string, number, boolean or None.
        target_role (`str`)
            An optional role to build the filter against

    Returns:

        A SQLAlchemy boolean expression

    """
    value_is_scalar = not isinstance(value, (list, tuple))

    if value_is_scalar:
        return self._build_scalar_filter(
            value, operator=operator, target_role=target_role
        )
    else:
        return self._build_vector_filter(
            value, operator=operator, target_role=target_role
        )
