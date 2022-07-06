from functools import total_ordering
import this
from uuid import uuid4
from sqlalchemy import Float, String, and_, between, case, cast, func, or_, text, not_
from recipe.exceptions import BadIngredient
from recipe.utils import AttrDict, filter_to_string
from recipe.utils.datatype import datatype_from_column_expression

"""
we take a dictionary like this

patients:
   count(patients)
dept_full:
   department + "/" + institution


this becomes

{
    id: patients,
    expression: count(patients)
    datatype: int
    kind: measure
}
{
    id: dept_full
    expression: department + "/" + institution
    datatype: str
    kind: dimension
}


r = (
    recipe("patients", "dept_full")
    .order_by("-dept_full")
    .filters("[patients]>5")
    .limit(5)
    .execute_with(engine, table)

select
    dept + "/" + institution as dept_full,
    count(patients) as patients,
from mytable
group by dept_full desc
having count(patients) > 5

"""


@total_ordering
class Ingredient(object):
    """Ingredients combine to make a SQLAlchemy query.

    Returns:
        An Ingredient object.

    """

    def __init__(self, config, **kwargs):
        self.id = config.pop("id", uuid4())
        self.expression = config.pop("expression")
        self.datatype = config.pop("datatype")

    def _order(self):
        """Ingredients are sorted by subclass then by id."""
        if isinstance(self, Dimension):
            return (0, self.id)
        elif isinstance(self, Metric):
            return (1, self.id)
        elif isinstance(self, Filter):
            return (2, self.id)
        elif isinstance(self, Having):
            return (3, self.id)
        else:
            return (4, self.id)

    def __lt__(self, other):
        """Make ingredients sortable."""
        return self._order() < other._order()

    def __eq__(self, other):
        """Make ingredients sortable."""
        return self._order() == other._order()

    def __ne__(self, other):
        """Make ingredients sortable."""
        return not (self._order() == other._order())

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.columns:
            return self.columns[0]
        else:
            return None


class Filter(Ingredient):
    """A simple filter created from a single expression."""

    def __init__(self, expression, **kwargs):
        super(Filter, self).__init__(**kwargs)
        self.filters = [expression]
        self.datatype = "bool"

    def _stringify(self):
        return filter_to_string(self)

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.filters:
            return self.filters[0]
        else:
            return None


class Having(Ingredient):
    """A Having that limits results based on an aggregate boolean clause"""

    def __init__(self, expression, **kwargs):
        super(Having, self).__init__(**kwargs)
        self.havings = [expression]
        self.datatype = "bool"

    def _stringify(self):
        return " ".join(str(expr) for expr in self.havings)

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.havings:
            return self.havings[0]
        else:
            return None


class Dimension(Ingredient):
    """A Dimension is an Ingredient that adds columns and groups by those
    columns. Columns should be non-aggregate SQLAlchemy expressions.

    The required expression supplies the dimension's "value" role. Additional
    expressions can be provided in keyword arguments with keys
    that look like "{role}_expression". The role is suffixed to the
    end of the SQL column name.

    For instance, the following

    .. code:: python

        Dimension(Hospitals.name,
                  latitude_expression=Hospitals.lat
                  longitude_expression=Hospitals.lng,
                  id='hospital')

    would add columns named "hospital", "hospital_latitude", and
    "hospital_longitude" to the recipes results. All three of these expressions
    would be used as group bys.

    Two special roles that can be added are "id" and "order_by". If a keyword argument
    "id_expression" is passed, this expression will appear first in the list of
    columns and group_bys. This "id" will be used if you call `build_filter` on the
    dimension.

    If the keyword argument "order_by_expression" is passed, this expression will
    appear last in the list of columns and group_bys.

    The following additional keyword parameters are also supported:

    Args:

        lookup (:obj:`dict`):
            A dictionary that is used to map values to new values.

            Note: Lookup adds a ``formatter`` callable as the first
            item in the list of formatters.
        lookup_default (:obj:`object`)
            A default to show if the value can't be found in the
            lookup dictionary.

    Returns:

        A Filter object
    :param lookup: dict A dictionary to translate values into
    :param lookup_default: A default to show if the value can't be found in the
      lookup dictionary.
    """

    def __init__(self, expression, **kwargs):
        super(Dimension, self).__init__(**kwargs)
        if self.datatype is None:
            self.datatype = datatype_from_column_expression(expression)

        # We must always have a value role
        self.roles = {"value": expression}

        for k, v in kwargs.items():
            role = None
            if k.endswith("_expression"):
                # Remove _expression to get the role
                role = k[:-11]
            if role:
                if role == "raw":
                    raise BadIngredient("raw is a reserved role in dimensions")
                self.roles[role] = v

        if not self.datatype_by_role:
            for k, expr in self.roles.items():
                self.datatype_by_role[k] = datatype_from_column_expression(expr)

        self.columns = []
        self._group_by = []
        self.role_keys = []
        if "id" in self.roles:
            self.columns.append(self.roles["id"])
            self._group_by.append(self.roles["id"])
            self.role_keys.append("id")
        if "value" in self.roles:
            self.columns.append(self.roles["value"])
            self._group_by.append(self.roles["value"])
            self.role_keys.append("value")

        # Add all the other columns in sorted order of role
        # with order_by coming last
        # For instance, if the following are passed
        # expression, id_expression, order_by_expresion, zed_expression the order of
        # columns would be "id", "value", "zed", "order_by"
        # When using group_bys for ordering we put them in reverse order.
        ordered_roles = [
            k for k in sorted(self.roles.keys()) if k not in ("id", "value")
        ]
        # Move order_by to the end
        if "order_by" in ordered_roles:
            ordered_roles.remove("order_by")
            ordered_roles.append("order_by")

        for k in ordered_roles:
            self.columns.append(self.roles[k])
            self._group_by.append(self.roles[k])
            self.role_keys.append(k)

        if "lookup" in kwargs:
            self.lookup = kwargs.get("lookup")
            if not isinstance(self.lookup, dict):
                raise BadIngredient("lookup must be a dictionary")
            # Inject a formatter that performs the lookup
            if "lookup_default" in kwargs:
                self.lookup_default = kwargs.get("lookup_default")
                self.formatters.insert(
                    0, lambda value: self.lookup.get(value, self.lookup_default)
                )
            else:
                self.formatters.insert(0, lambda value: self.lookup.get(value, value))

    @property
    def group_by(self):
        # Ensure the labels are generated
        if not self._labels:
            list(self.query_columns)

        if self.group_by_strategy == "labels":
            return [lbl for _, lbl in zip(self._group_by, self._labels)]
        else:
            return self._group_by

    @group_by.setter
    def group_by(self, value):
        self._group_by = value

    @property
    def cauldron_extras(self):
        """Yield extra tuples containing a field name and a callable that takes
        a row
        """
        # This will format the value field
        for extra in super(Dimension, self).cauldron_extras:
            yield extra

        yield self.id + "_id", lambda row: getattr(row, self.id_prop)

    def make_column_suffixes(self):
        """Make sure we have the right column suffixes. These will be appended
        to `id` when generating the query.
        """
        if self.formatters:
            value_suffix = "_raw"
        else:
            value_suffix = ""

        return tuple(
            value_suffix if role == "value" else "_" + role for role in self.role_keys
        )

    @property
    def id_prop(self):
        """The label of this dimensions id in the query columns"""
        if "id" in self.role_keys:
            return self.id + "_id"
        else:
            # Use the value dimension
            if self.formatters:
                return self.id + "_raw"
            else:
                return self.id


class IdValueDimension(Dimension):
    """
    DEPRECATED: A convenience class for creating a Dimension
    with a separate ``id_expression``.  The following are identical.

    .. code:: python

        d = Dimension(Student.student_name, id_expression=Student.student_id)

        d = IdValueDimension(Student.student_id, Student.student_name)

    The former approach is recommended.

    Args:

        id_expression (:obj:`ColumnElement`)
            A column expression that is used to identify the id
            for a Dimension
        value_expression (:obj:`ColumnElement`)
            A column expression that is used to identify the value
            for a Dimension

    """

    def __init__(self, id_expression, value_expression, **kwargs):
        kwargs["id_expression"] = id_expression
        super(IdValueDimension, self).__init__(value_expression, **kwargs)


class LookupDimension(Dimension):
    """DEPRECATED Returns the expression value looked up in a lookup dictionary"""

    def __init__(self, expression, lookup, **kwargs):
        """A Dimension that replaces values using a lookup table.

        :param expression: The dimension field
        :type value: object
        :param lookup: A dictionary of key/value pairs. If the keys will
           be replaced by values in the value of this Dimension
        :type operator: dict
        :param default: The value to use if a dimension value isn't
           found in the lookup table.  The default behavior is to
           show the original value if the value isn't found in the
           lookup table.
        :type default: object
        """
        if "default" in kwargs:
            kwargs["lookup_default"] = kwargs.pop("default")
        kwargs["lookup"] = lookup

        super(LookupDimension, self).__init__(expression, **kwargs)


class Metric(Ingredient):
    """A simple metric created from a single expression"""

    def __init__(self, expression, **kwargs):
        super(Metric, self).__init__(**kwargs)
        self.columns = [expression]
        if self.datatype is None:
            self.datatype = datatype_from_column_expression(expression)

        # We must always have a value role
        self.roles = {"value": expression}

    def build_filter(self, value, operator=None):
        """Building filters with Metric returns Having objects."""
        f = super().build_filter(value, operator=operator)
        return Having(f.filters[0])


class DivideMetric(Metric):
    """A metric that divides a numerator by a denominator handling several
    possible error conditions

    The default strategy is to add an small value to the denominator
    Passing ifzero allows you to give a different value if the denominator is
    zero.
    """

    def __init__(self, numerator, denominator, **kwargs):
        ifzero = kwargs.pop("ifzero", "epsilon")
        epsilon = kwargs.pop("epsilon", 0.000000001)
        if ifzero == "epsilon":
            # Add an epsilon value to denominator to avoid divide by zero
            # errors
            expression = cast(numerator, Float) / (
                func.coalesce(cast(denominator, Float), 0.0) + epsilon
            )
        else:
            # If the denominator is zero, return the ifzero value otherwise do
            # the division
            expression = case(
                ((cast(denominator, Float) == 0.0, ifzero),),
                else_=cast(numerator, Float) / cast(denominator, Float),
            )
        super(DivideMetric, self).__init__(expression, **kwargs)


class WtdAvgMetric(DivideMetric):
    """A metric that generates the weighted average of a metric by a weight."""

    def __init__(self, expression, weight_expression, **kwargs):
        numerator = func.sum(expression * weight_expression)
        denominator = func.sum(weight_expression)
        super(WtdAvgMetric, self).__init__(numerator, denominator, **kwargs)


class InvalidIngredient(Ingredient):
    pass
