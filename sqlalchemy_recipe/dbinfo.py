import functools
from threading import Lock
from typing import Tuple

import attr
import cachetools
import structlog
from cachetools import TTLCache, cached
from dogpile.cache import CacheRegion, make_region
from dogpile.cache.api import NoValue
from sqlalchemy import MetaData, Table, engine_from_config, event, exc, select
from sqlalchemy.engine import Engine

from sqlalchemy_recipe.expression.grammar import make_grammar

from .caching import _key_from_statement, mangle_key

SLOG = structlog.get_logger(__name__)


#: A global cache of DBInfo, keyed by engine config.
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


def make_table_key(*args, **kwargs):
    """Generate a unique key for this table information"""
    return args[0]


@attr.s
class DBInfo(object):
    """An object for keeping track of SQLAlchemy objects related to a
    single database.
    """

    engine: Engine = attr.ib()
    dogpile_region: CacheRegion = attr.ib()
    cache_queries: bool = attr.ib(default=False)
    table_cache_maxsize = attr.ib(default=1024)
    table_cache_ttl = attr.ib(default=600)
    metadata = attr.ib(default=None)
    metadata_write_lock = attr.ib(default=None)
    is_postgres = attr.ib(default=False)

    def __attrs_post_init__(self):
        """Test if an engine is postgres compatible, setup metadata."""
        self.metadata = MetaData(bind=self.engine)
        event.listen(self.metadata, "column_reflect", genericize_datatypes)

        self.metadata_write_lock = Lock()

        self.is_postgres = any(
            (pg_id in self.engine.name for pg_id in ("redshift", "postg", "pg"))
        )
        self.TABLE_CACHE = TTLCache(
            maxsize=self.table_cache_maxsize, ttl=self.table_cache_ttl
        )
        self.TABLE_LOCK = Lock()

        @refreshing_cached(
            cache=self.TABLE_CACHE, key=make_table_key, lock=self.TABLE_LOCK
        )
        def reflect(table: str) -> Tuple:
            # Build a table definition with generic datatypes
            table = Table(table, self.metadata, autoload_with=self.engine)
            grammar = make_grammar(table)
            return table, grammar

        self.reflect = reflect

    def invalidate(self, table: str):
        """Remove cached information for a table"""
        self.TABLE_CACHE.pop(table, None)

    def execute(self, statement):
        from_cache = False
        key = mangle_key(_key_from_statement(statement))
        if self.dogpile_region:
            val = self.dogpile_region.get(key)
            if isinstance(val, NoValue):
                with self.engine.connect() as connection:
                    val = connection.execute(statement).all()
                self.dogpile_region.set(key, val)
            else:
                from_cache = True
            return val
        else:
            with self.engine.connect() as connection:
                return connection.execute(statement).all()

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
def get_dbinfo(config: dict, debug: bool = False):
    """Get a (potentially cached) DBInfo object based on a connection string.

    Args:
        config (dict): A configuration dictionary for a sqlalchemy engine and dogpile
        cache_queries (bool): Should the queries be cached
        debug (bool): Add debugging to engine events

    Engine configuration can be anything from engine_from_config. Engine configuration
    uses a prefix of "sqlalchemy."
    https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.engine_from_config

    Caching configuration accepts the following options:

        caching.cache_queries (bool): Is query caching enabled? If true, then a
            caching.dogpile_region must be configured.
        caching.dogpile_region.*: Dogpile configuration used to configure a cache_region
            from config. See https://dogpilecache.sqlalchemy.org/en/latest/api.html#dogpile.cache.region.CacheRegion.configure_from_config
        caching.table_cache_maxsize (int): The number of objects to hold in table cache
            for this database
        caching.table_cache_ttl (int): The duration in seconds to hold table cache
            items for if they are unused. If read, the ttl will be refreshed.

    Returns:
        DBInfo: A cached DBInfo object
    """

    # Note: Connections should be configured with pool_pre_ping=True
    engine = engine_from_config(configuration=config, prefix="sqlalchemy.")

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

    dogpile_region = None
    cache_queries = config.get("cache.cache_queries", False)
    if cache_queries:
        # A dogpile_region must be configured.
        dogpile_region = make_region()
        dogpile_region.configure_from_config(config, "cache.dogpile.")

    return DBInfo(
        engine=engine, cache_queries=cache_queries, dogpile_region=dogpile_region
    )
