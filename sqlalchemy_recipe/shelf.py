from copy import copy

from collections import OrderedDict


_POP_DEFAULT = object()


class Shelf:
    """Holds ingredients used by a recipe."""

    def __init__(self, *args, **kwargs):
        self._ingredients = {}
        self.update(*args, **kwargs)

    # Dict Interface

    def get(self, k, d=None):
        ingredient = self._ingredients.get(k, d)
        if isinstance(ingredient, Ingredient):
            ingredient.id = k
        return ingredient

    def items(self):
        """Return an iterator over the ingredient names and values."""
        return self._ingredients.items()

    def values(self):
        """Return an iterator over the ingredients."""
        return self._ingredients.values()

    def keys(self):
        """Return an iterator over the ingredient keys."""
        return self._ingredients.keys()

    def __copy__(self):
        meta = copy(self.Meta)
        ingredients = copy(self._ingredients)
        new_shelf = type(self)(ingredients)
        new_shelf.Meta = meta
        return new_shelf

    def __iter__(self):
        return iter(self._ingredients)

    def __getitem__(self, key):
        """Set the id and anonymize property of the ingredient whenever we
        get or set items"""
        return self._ingredients[key]

    def __setitem__(self, key, ingredient):
        """Set the id and anonymize property of the ingredient whenever we
        get or set items"""
        # Maintainer's note: try to make all mutation of self._ingredients go
        # through this method, so we can reliably copy & annotate the
        # ingredients that go into the Shelf.
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                "Can only set Ingredients as items on Shelf. "
                "Got: {!r}".format(ingredient)
            )
        ingredient_copy = copy(ingredient)
        self._ingredients[key] = ingredient_copy

    def __contains__(self, key):
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
            self[k] = v

    def pop(self, k, d=_POP_DEFAULT):
        """Pop an ingredient off of this shelf."""
        if d is _POP_DEFAULT:
            return self._ingredients.pop(k)
        else:
            return self._ingredients.pop(k, d)

    # End dict interface

    def ingredients(self):
        """Return the ingredients in this shelf in a deterministic order"""
        return sorted(list(self.values()))

    @property
    def dimension_ids(self):
        """Return the Dimensions on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Dimension)]
        )

    @property
    def metric_ids(self):
        """Return the Metrics on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Metric)]
        )

    @property
    def filter_ids(self):
        """Return the Filters on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Filter)]
        )

    def _sorted_ingredients(self, ingredients):
        def sort_key(id):
            if id in self.Meta.ingredient_order:
                return self.Meta.ingredient_order.index(id)
            else:
                return 9999

        return tuple(sorted(ingredients, key=sort_key))

    def __repr__(self):
        """A string representation of the ingredients used in a recipe
        ordered by Dimensions, Metrics, Filters, then Havings
        """
        lines = []
        # sort the ingredients by type
        for ingredient in sorted(self.values()):
            lines.append(ingredient.describe())
        return "\n".join(lines)

    def use(self, ingredient):
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                "Can only set Ingredients as items on Shelf. "
                "Got: {!r}".format(ingredient)
            )

        # Track the order in which ingredients are added.
        self.Meta.ingredient_order.append(ingredient.id)
        self[ingredient.id] = ingredient

    def find(self, id: str, role=None):
        """
        Find an Ingredient, optionally using the shelf.

        :param obj: A string or Ingredient
        :param filter_to_class: The Ingredient subclass that obj must be an
         instance of
        :param constructor: An optional callable for building Ingredients
         from obj
        :return: An Ingredient of subclass `filter_to_class`
        """
        if callable(constructor):
            obj = constructor(obj, shelf=self)

        if isinstance(obj, str):
            set_descending = obj.startswith("-")
            if set_descending:
                obj = obj[1:]

            if obj not in self:
                raise BadRecipe("{} doesn't exist on the shelf".format(obj))

            ingredient = self[obj]
            if isinstance(ingredient, InvalidIngredient):
                # allow InvalidIngredient, it will be handled at a later time
                return ingredient

            if not isinstance(ingredient, filter_to_class):
                raise BadRecipe("{} is not a {}".format(obj, filter_to_class))

            if set_descending:
                ingredient.ordering = "desc"

            return ingredient
        elif isinstance(obj, filter_to_class):
            return obj
        else:
            raise BadRecipe("{} is not a {}".format(obj, filter_to_class))

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
