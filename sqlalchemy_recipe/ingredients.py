from copy import copy
from dataclasses import dataclass
from functools import total_ordering
import this
from typing import List, Union
from typing_extensions import Self
from uuid import uuid4
from numpy import isin
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

from sqlalchemy_recipe.expression.builder import BuilderResponse

# each ingredient is a single expression that may have an id and role
# id+role acts as the label for the ingredient
# br = BuilderResponse(id="foo", role=None)
# br2 = BuilderResponse(id="foo", role="id")
# br3 = BuilderResponse(id="foo", role="order_by")

# an ingredient is a collection of builder responses that get added
# an ingredient may also generate subqueries

# three uses for subqueries
# 1) value / total of value
#    sum(sales) / {{ sum(sales) }}
# 2) value / unfiltered total of value
# 3) other_table(student_id).student_age


_POP_DEFAULT = object()


@total_ordering
@dataclass
class BuilderIngredient(object):
    builder_response: BuilderResponse
    id: str
    role: str
    sort_ascending: bool = True
    order_by: bool = False
    anonymizer: Union[str, callable] = None

    def __eq__(self, other: Self) -> bool:
        return (self.id, self.role) == (other.id, other.role)

    def __lt__(self, other: Self) -> bool:
        return (self.id, self.role) < (other.id, other.role)


class BuilderIngredientDict:
    """Holds ingredients used by a recipe."""

    def __init__(self, *args, **kwargs):
        self._ingredients = {}
        self.update(*args, **kwargs)

    # Dict Interface

    def get(self, key, default=_POP_DEFAULT):
        """key is either a (id, role) tuple or an id"""
        key = self._clean_key(key)
        return self._ingredients.get(key, default)

    def items(self):
        """Return an iterator over the ingredient names and values."""
        return self._ingredients.items()

    def values(self) -> List[BuilderIngredient]:
        """Return an iterator over the ingredients."""
        return self._ingredients.values()

    def keys(self):
        """Return an iterator over the ingredient keys."""
        return self._ingredients.keys()

    def __copy__(self):
        ingredients = copy(self._ingredients)
        return type(self)(ingredients)

    def __iter__(self):
        return iter(self._ingredients)

    def __getitem__(self, key):
        return self.get(self._clean_key(key))

    def __setitem__(self, key, ingredient: BuilderIngredient):
        """Set the id and anonymize property of the ingredient whenever we
        get or set items"""
        # Maintainer's note: try to make all mutation of self._ingredients go
        # through this method, so we can reliably copy & annotate the
        # ingredients that go into the Shelf.
        if not isinstance(ingredient, BuilderIngredient):
            raise TypeError(
                "Can only set BuilderIngredient as items on BuilderIngredientDict. "
                "Got: {!r}".format(ingredient)
            )
        ingredient_copy = copy(ingredient)
        ingrkey = (ingredient.id, ingredient.role)
        self._ingredients[ingrkey] = ingredient_copy

    def __contains__(self, key):
        key = self._clean_key(key)
        return key in self._ingredients

    def __len__(self):
        return len(self._ingredients)

    def clear(self):
        self._ingredients.clear()

    def update(self, d=None, **kwargs):
        items = []
        if d is not None:
            items = list(d.items())
        for k, v in items + list(kwargs.items()):
            k = self._clean_key(k)
            self[k] = v

    def pop(self, key, default=_POP_DEFAULT):
        """Pop an ingredient off of this shelf."""
        key = self._clean_key(key)
        if default is _POP_DEFAULT:
            return self._ingredients.pop(key)
        else:
            return self._ingredients.pop(key, default)

    # End dict interface

    def _clean_key(self, key):
        assert isinstance(key, (str, tuple))
        if not isinstance(key, tuple):
            return (key, None)
        assert len(key) == 2
        return key

    def ingredients(self):
        """Return the ingredients in this shelf in a deterministic order"""
        return sorted(list(self.values()))

    def order_by(self, *args: List[str]):
        """Mark these ingredients as order bys"""
        ascending = True
        for key in args:
            if key.startswith("-"):
                ascending = False
                key = key[1:]
            ingr = self.find((key, "order_by"), (key, None))
            ingr.sort_ascending = ascending
            ingr.order_by = True

    # @property
    # def dimension_ids(self):
    #     """Return the Dimensions on this shelf in the order in which
    #     they were used."""
    #     return self._sorted_ingredients(
    #         [d.id for d in self.values() if isinstance(d, Dimension)]
    #     )

    # @property
    # def metric_ids(self):
    #     """Return the Metrics on this shelf in the order in which
    #     they were used."""
    #     return self._sorted_ingredients(
    #         [d.id for d in self.values() if isinstance(d, Metric)]
    #     )

    # @property
    # def filter_ids(self):
    #     """Return the Filters on this shelf in the order in which
    #     they were used."""
    #     return self._sorted_ingredients(
    #         [d.id for d in self.values() if isinstance(d, Filter)]
    #     )

    def __repr__(self):
        """A string representation of the ingredients used in a recipe
        ordered by Dimensions, Metrics, Filters, then Havings
        """
        lines = [ingredient.describe() for ingredient in sorted(self.values())]
        return "\n".join(lines)

    def find(self, *keys: List[tuple]):
        """
        Find an ingredient by searching through keys in order
        """
        for key in keys:
            if key in self:
                return self[key]
        raise Exception(f"Can't find ingredient in shelf {keys}")

    def make_statement(self, group_by_labels=True, order_by_labels=True):
        """Build a query using the ingredients in this shelf"""
        columns = []
        group_bys = []
        order_bys = []

        for ingr in self.values():
            if ingr.builder_response.is_aggr:
                continue
            lbl = f"{ingr.id}__{ingr.role}" if ingr.role is not None else ingr.id
            columns.append(ingr.builder_response.expression.label(lbl))
            if group_by_labels:
                group_bys.append(lbl)
            else:
                group_bys.append(ingr.builder_response.expression)
            if ingr.order_by:
                if order_by_labels:
                    order_bys.append(lbl)
                else:
                    order_bys.append(ingr.builder_response.expression)

    def brew_query_parts(self, order_by_keys=[]):
        """Make columns, group_bys, filters, havings"""
        columns, group_bys, filters, havings = [], [], set(), set()
        order_by_keys = list(order_by_keys)
        all_filters = set()

        for ingredient in self.ingredients():
            if ingredient.error:
                error_type = ingredient.error.get("type")
                if error_type == "invalid_column":
                    extra = ingredient.error.get("extra", {})
                    column_name = extra.get("column_name")
                    ingredient_name = extra.get("ingredient_name")
                    error_msg = 'Invalid column "{0}" in ingredient "{1}"'.format(
                        column_name, ingredient_name
                    )
                    raise InvalidColumnError(error_msg, column_name=column_name)
                raise BadIngredient(str(ingredient.error))
            if ingredient.query_columns:
                columns.extend(ingredient.query_columns)
            if ingredient.group_by:
                group_bys.extend(ingredient.group_by)
            if ingredient.filters:
                # Ensure we don't add duplicate filters
                for new_f in ingredient.filters:
                    from recipe.utils import filter_to_string

                    new_f_str = filter_to_string(new_f)
                    if new_f_str not in all_filters:
                        filters.add(new_f)
                        all_filters.add(new_f_str)
            if ingredient.havings:
                havings.update(ingredient.havings)

            # If there is an order_by key on one of the ingredients, make sure
            # the recipe orders by this ingredient
            if "order_by" in ingredient.roles:
                if (
                    ingredient.id not in order_by_keys
                    and "-" + ingredient.id not in order_by_keys
                ):
                    if ingredient.ordering == "desc":
                        order_by_keys.append("-" + ingredient.id)
                    else:
                        order_by_keys.append(ingredient.id)

        order_bys = OrderedDict()
        for key in order_by_keys:
            try:
                ingr = self.find(key, (Dimension, Metric))
                for c in ingr.order_by_columns:
                    # Avoid duplicate order by columns
                    if str(c) not in [str(o) for o in order_bys]:
                        order_bys[c] = None
            except BadRecipe as e:
                # Ignore order_by if the dimension/metric is not used.
                # TODO: Add structlog warning
                pass

        return {
            "columns": columns,
            "group_bys": group_bys,
            "filters": filters,
            "havings": havings,
            "order_bys": list(order_bys.keys()),
        }

    def enchant(self, data, cache_context=None):
        """Add any calculated values to each row of a resultset generating a
        new namedtuple

        :param data: a list of row results
        :param cache_context: optional extra context for caching
        :return: a list with ingredient.cauldron_extras added for all
                 ingredients
        """
        enchantedlist = []
        if data:
            sample_item = data[0]

            # Extra fields to add to each row
            # With extra callables
            extra_fields, extra_callables = [], []

            for ingredient in self.ingredients():
                if not isinstance(ingredient, (Dimension, Metric)):
                    continue
                if cache_context:
                    ingredient.cache_context += str(cache_context)
                for extra_field, extra_callable in ingredient.cauldron_extras:
                    extra_fields.append(extra_field)
                    extra_callables.append(extra_callable)

            # Mixin the extra fields
            keyed_tuple = lightweight_named_tuple(
                "result", sample_item._fields + tuple(extra_fields)
            )

            # Iterate over the results and build a new namedtuple for each row
            for row in data:
                values = row + tuple(fn(row) for fn in extra_callables)
                enchantedlist.append(keyed_tuple(values))

        return enchantedlist
