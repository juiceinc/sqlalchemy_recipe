import logging
from typing_extensions import Self
from functools import wraps
from typing import List
from uuid import uuid4

import tablib

logger = logging.getLogger(__name__)


def builder(*args):
    """Decorator for recipe builder arguments.

    Promotes builder pattern by returning a recipe.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *_args, **_kwargs):
            func(self, *_args, **_kwargs)
            return self

        return wrapper

    return decorator


class RecipeRow:
    pass


class Recipe:
    """
    recipe(table="foo", dbinfo=dbinfo)
        .dimensions("cow", "mouse")
        .metrics("count(distinct(cow)) / {{count(distinct(cow))}}")
        .order_by("count(distinct_cow))")
        .filters(dict of filter syntax or boolean expressions)

    shelf = {
        cntcow: count(distinct(cow)
        cntcowpct: @cntcow / {{@cntcow}}
        cow: cow
        mouse: mouse
    }

    recipe(table="foo", dbinfo=dbinfo)
        .ingredients("cow", "mouse", "cntcowpct")
        .order_by(cntcowpct)
        .filters({
            cow:["holstein", "brown betty"]
        })

    select cow,
           mouse,
           count(distinct(cow)) / 9520
    from mytable
    where xxx
    group by cow, mouse
    order by count(distinct(cow))

    extra features:
    * anonymize (post recipe)
    * pagination
    * cache
    * configuration is applied how...

    """

    def __init__(self):
        self._id = str(uuid4())
        self._dimensions = []
        self._measures = []
        self._order_by = []

    @builder
    def dimensions(self, *args):
        """Args can be expressions or dimension objects"""
        self._dimensions = args

    @builder
    def measures(self, *args):
        self._measures = args

    @builder
    def order_by(self, *args) -> Self:
        self._order_by = args

    @builder
    def order_by(self, *args) -> Self:
        self._order_bys = args

    def select_from(self, selectable) -> Self:
        self._select_from = selectable
        return self

    @builder
    def limit(self, limit) -> Self:
        """Limit the number of rows returned from the database.

        :param limit: The number of rows to return in the recipe. 0 will
                      return all rows.
        :type limit: int
        """
        self._limit = limit

    @builder
    def offset(self, offset) -> Self:
        """Offset a number of rows before returning rows from the database.

        :param offset: The number of rows to offset in the recipe. 0 will
                       return from the first available row
        :type offset: int
        """
        self._offset = offset

    def query(self):
        """
        Generates a query using the ingredients supplied by the recipe.

        :return: A SQLAlchemy query
        """
        if self._query is not None:
            return self._query

        if hasattr(self, "optimize_redshift"):
            self.optimize_redshift(self._is_redshift())

        if len(self._cauldron.ingredients()) == 0:
            raise BadRecipe("No ingredients have been added to this recipe")

        # Step 1: Gather up global filters and user filters and
        # apply them as if they had been added to recipe().filters(...)

        for extension in self.recipe_extensions:
            extension.add_ingredients()

        # Step 2: Build the query (now that it has all the filters
        # and apply any blend recipes

        # Get the parts of the query from the cauldron
        # {
        #             "columns": columns,
        #             "group_bys": group_bys,
        #             "filters": filters,
        #             "havings": havings,
        #             "order_bys": list(order_bys)
        #         }
        recipe_parts = self._cauldron.brew_query_parts(self._order_bys)

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_recipe_parts(recipe_parts)

        # Start building the query
        query = self._session.query(*recipe_parts["columns"])
        if self._select_from is not None:
            query = query.select_from(self._select_from)
        recipe_parts["query"] = (
            query.group_by(*recipe_parts["group_bys"])
            .order_by(*recipe_parts["order_bys"])
            .filter(*recipe_parts["filters"])
        )

        if recipe_parts["havings"]:
            for having in recipe_parts["havings"]:
                recipe_parts["query"] = recipe_parts["query"].having(having)

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_prequery_parts(recipe_parts)

        if (
            self._select_from is None
            and len(recipe_parts["query"].selectable.froms) != 1
        ):
            raise BadRecipe(
                "Recipes must use ingredients that all come from "
                "the same table. \nDetails on this recipe:\n{"
                "}".format(str(self._cauldron))
            )

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_postquery_parts(recipe_parts)

        if "recipe" not in recipe_parts:
            recipe_parts["cache_region"] = self._cache_region
            recipe_parts["cache_prefix"] = self._cache_prefix
        recipe_parts = run_hooks(recipe_parts, "modify_query", self.dynamic_extensions)

        # Apply limit on the outermost query
        # This happens after building the comparison recipe
        if self._limit and self._limit > 0:
            recipe_parts["query"] = recipe_parts["query"].limit(self._limit)

        if self._offset and self._offset > 0:
            recipe_parts["query"] = recipe_parts["query"].offset(self._offset)

        # Patch the query if there's a comparison query
        # cache results

        self._query = recipe_parts["query"]
        return self._query

    def total_count(self) -> int:
        """Return the total count of items with no pagination applied"""
        pass

    def to_sql(self) -> str:
        """A string representation of the SQL this recipe will generate."""
        return ""

    def all(self) -> List[RecipeRow]:
        return []

    def one(self) -> RecipeRow:
        """Return the first element on the result"""
        pass

    def first(self):
        """Return the first element on the result"""
        return self.one()

    @property
    def dataset(self) -> tablib.Dataset:
        rows = self.all()
        if rows:
            first_row = rows[0]
            return tablib.Dataset(*rows, headers=first_row._fields)
        else:
            return tablib.Dataset([], headers=[])
