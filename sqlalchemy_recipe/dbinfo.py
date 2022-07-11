from calendar import c
from dataclasses import dataclass
import faulthandler
from hashlib import sha1
from threading import Lock
from typing import List

import attr
import structlog
from lark import Lark
from cachetools import TTLCache
from dogpile.cache import CacheRegion, make_region
from dogpile.cache.api import NoValue
from sqlalchemy import MetaData, Table, column, engine_from_config, event, exc, select
from sqlalchemy.engine import Engine

from sqlalchemy_recipe.caching import _key_from_statement, mangle_key, refreshing_cached
from sqlalchemy_recipe.core import RecipeRow
from sqlalchemy_recipe.expression.grammar import make_columns_for_table, make_grammar
from sqlalchemy_recipe.utils.acquire import acquire_with_timeout
from sqlalchemy.exc import NoSuchTableError, OperationalError

SLOG = structlog.get_logger(__name__)


#: A global cache of DBInfo, keyed by engine config.
#: Cached for 10 minutes, but accesses will refresh the TTL.
_DBINFO_CACHE = TTLCache(maxsize=1024, ttl=600)
_DBINFO_CACHE_LOCK = Lock()


# A global cache of table info
_REFLECTED_TABLE_CACHE = TTLCache(maxsize=1024, ttl=600)
_REFLECTED_TABLE_LOCK = Lock()


def make_engine_event_handler(event_name, engine_name):
    def event_handler(*args, event_name=event_name, engine_name=engine_name):
        log = SLOG.bind(engine_name=engine_name)
        log.info(event_name)

    return event_handler


def genericize_datatypes(inspector, tablename, column_dict):
    column_dict["type"] = column_dict["type"].as_generic()


def make_table_key(dbinfo, table, *args, **kwargs):
    """Generate a unique key for a table by hashing the dbinfo and table name"""
    return sha1(f"{dbinfo.key}:{table}".encode("utf-8")).hexdigest()


def make_dbinfo_key(config, *args, **kwargs):
    """Generate a unique key for this dbinfo configuration by hashing
    the configuration dict"""
    return sha1(str(config).encode("utf-8")).hexdigest()


@dataclass
class ExecutionResult:
    from_cache: bool
    rows: List[RecipeRow]


@dataclass
class ReflectedTable:
    """A database table that can have expressions built against it."""

    table: Table
    columns: dict
    grammar: str
    # A lark parser using the grammar
    parser: Lark


@attr.s
class DBInfo(object):
    """An object for keeping track of SQLAlchemy objects related to a
    single database.

    DBInfo will cache table
    """

    key: str = attr.ib()
    engine: Engine = attr.ib()
    dogpile_region: CacheRegion = attr.ib()
    cache_queries: bool = attr.ib(default=False)
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

    @refreshing_cached(
        cache=_REFLECTED_TABLE_CACHE, key=make_table_key, lock=_REFLECTED_TABLE_LOCK
    )
    def reflect(self, table: str) -> ReflectedTable:
        """Safely load a table, returning the table and a specific grammar for
        the table."""
        # sourcery skip: raise-specific-error
        # Build a table definition with generic datatypes

        try:
            # SQLAlchemy metadata is not thread-safe for writes
            try:
                with acquire_with_timeout(self.metadata_write_lock, 10):
                    table = Table(
                        table, self.metadata, autoload=True, autoload_with=self.engine
                    )
            except TimeoutError:
                faulthandler.dump_traceback()
                raise
            if len(table.columns) == 0:
                # Ok, so the lock acquisition above should totally avoid this problem,
                # but just in case: if we get a Table with no columns, *don't* try
                # to proceed.
                raise Exception(f"The table {table!r} has no columns")

        except NoSuchTableError:
            raise

        columns = make_columns_for_table(table)
        grammar = make_grammar(table, columns=columns)
        parser = Lark(
            grammar,
            parser="earley",
            ambiguity="resolve",
            start="col",
            propagate_positions=True,
        )
        return ReflectedTable(
            table=table, columns=columns, grammar=grammar, parser=parser
        )

    def invalidate(self, table: str):
        """Remove cached information for a table"""
        _REFLECTED_TABLE_CACHE.pop(make_table_key(self, table), None)

    def execute(self, statement) -> ExecutionResult:
        from_cache = False
        key = mangle_key(_key_from_statement(statement))
        if self.dogpile_region:
            rows = self.dogpile_region.get(key)
            if isinstance(rows, NoValue):
                with self.engine.connect() as connection:
                    rows = connection.execute(statement).all()
                self.dogpile_region.set(key, rows)
            else:
                from_cache = True
            return ExecutionResult(from_cache=from_cache, rows=rows)
        else:
            with self.engine.connect() as connection:
                return ExecutionResult(
                    from_cache=from_cache,
                    rows=connection.execute(statement=statement).all(),
                )


@refreshing_cached(cache=_DBINFO_CACHE, key=make_dbinfo_key, lock=_DBINFO_CACHE_LOCK)
def get_dbinfo(config: dict, debug: bool = False):
    """Get a (potentially cached) DBInfo object based on a connection string.

    Args:
        config (dict): A configuration dictionary for a sqlalchemy engine and dogpile
        cache_queries (bool): Should the queries be cached
        debug (bool): Add debugging to engine events

    Engine configuration can be anything from engine_from_config. Engine configuration
    uses a fixed prefix of "sqlalchemy."
    https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.engine_from_config

    Caching configuration accepts the following options:

        cache.cache_queries (bool): Is query caching enabled? If true, then a
            caching.dogpile_region must be configured.
        cache.dogpile_region.*: Dogpile configuration used to configure a cache_region
            from config. See https://dogpilecache.sqlalchemy.org/en/latest/api.html#dogpile.cache.region.CacheRegion.configure_from_config

    Returns:
        DBInfo: A cached DBInfo object
    """

    key = make_dbinfo_key(config)

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
        key=key,
        engine=engine,
        cache_queries=cache_queries,
        dogpile_region=dogpile_region,
    )
