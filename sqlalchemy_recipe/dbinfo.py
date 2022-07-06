import functools
from threading import Lock

import attr
import cachetools
from sqlalchemy_recipe.expression.grammar import make_columns_for_table, make_grammar
import structlog
from cachetools import TTLCache, cached
from sqlalchemy import MetaData, event, exc, select, engine_from_config, Table

SLOG = structlog.get_logger(__name__)


#: A global cache of DBInfo, keyed by the connection string.
#: Cached for 10 minutes, but accesses will refresh the TTL.
_DBINFO_CACHE = TTLCache(maxsize=1024, ttl=600)
_DBINFO_CACHE_LOCK = Lock()


# Decorate with an engine identifier
def make_engine_event_handler(event_name, engine_name):
    def event_handler(*args, event_name=event_name, engine_name=engine_name):
        log = SLOG.bind(engine_name=engine_name)
        log.info(event_name)

    return event_handler


def genericize_datatypes(inspector, tablename, column_dict):
    column_dict["type"] = column_dict["type"].as_generic()


def refreshing_cached(cache, key=cachetools.keys.hashkey, lock=None):
    """Same as `cachetools.cached`,
    but it also refreshes the TTL and checks for expiry on read operations.
    """

    def decorator(func):
        func = cached(cache, key, lock)(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if lock is not None:
                with lock:
                    cache[key(*args, **kwargs)] = result
                    # cachetools also only auto-expires on *mutating*
                    # operations, so we need to be explicit:
                    cache.expire()
            return result

        return wrapper

    return decorator


@attr.s
class DBInfo(object):
    """An object for keeping track of SQLAlchemy objects related to a
    single database.
    """

    engine = attr.ib()
    cache_queries: bool = attr.ib()
    metadata = attr.ib(default=None)
    tables = attr.ib(default=dict)
    grammars = attr.ib(default=dict)
    metadata_write_lock = attr.ib(default=None)
    is_postgres = attr.ib(default=False)

    def __attrs_post_init__(self):
        """Test if an engine is postgres compatible, setup metadata."""
        self.metadata = MetaData(bind=self.engine)
        event.listen(self.metadata, "column_reflect", genericize_datatypes)

        self.metadata_write_lock = Lock()

        self.tables = {}
        self.grammars = {}
        pg_identifiers = ["redshift", "postg", "pg"]
        self.is_postgres = any((pg_id in self.engine.name for pg_id in pg_identifiers))

    def reflect(self, table: str) -> Table:
        # Build a table definition with generic datatypes
        if table not in self.tables:
            self.tables[table] = Table(table, self.metadata, autoload_with=self.engine)

        if table not in self.grammars:
            cols = make_columns_for_table(self.tables[table])
            self.grammars[table] = make_grammar(cols)

        return self.tables[table]

    def execute(self, recipe):
        with self.engine.connect() as connection:
            result = connection.execute(recipe)
            for row in result:
                pass

    @property
    def drivername(self):
        return self.engine.url.drivername


def make_dbinfo_key(*args, **kwargs):
    """Generate a unique key for this dbinfo configuration"""
    engine_config = args[0]
    prefix = kwargs.get("prefix", "sqlalchemy.")
    cache_queries = kwargs.get("cache_queries", False)
    return f"{engine_config}:{prefix}:{cache_queries}"


@refreshing_cached(cache=_DBINFO_CACHE, key=make_dbinfo_key, lock=_DBINFO_CACHE_LOCK)
def get_dbinfo(
    engine_config: dict,
    prefix: str = "sqlalchemy.",
    cache_queries: bool = False,
    debug: bool = False,
):
    """Get a (potentially cached) DBInfo object based on a connection string.

    Args:
        engine_config (dict): A configuration dictionary
        prefix (str): A prefix find engine related keys in the config dict
        cache_queries (bool): Should the queries be cached
        debug (bool): Add debugging to engine events

    Returns:
        DBInfo: A cached DBInfo object
    """

    # Note: Connections should be configured with pool_pre_ping=True
    engine = engine_from_config(configuration=engine_config, prefix=prefix)

    # Optionally, listen to events
    if debug:
        for event_name in (
            "checkout",
            "checkin",
            "close",
            "close_detached",
            "first_connect",
            "detach",
            "invalidate",
            "reset",
            "soft_invalidate",
            "engine_connect",
        ):
            event.listen(
                engine,
                event_name,
                make_engine_event_handler(
                    event_name=event_name, engine_name=engine.url
                ),
            )

    return DBInfo(engine=engine, cache_queries=cache_queries)
