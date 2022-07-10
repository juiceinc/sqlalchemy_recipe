"""caching_query.py

Represent functions and classes which allow the usage of
Dogpile caching with SQLAlchemy.

 * CachingQuery - a Query subclass that caches and
   retrieves results in/from dogpile.cache.

The rest of what's here are standard SQLAlchemy and dogpile.cache constructs.
"""

import structlog
import unicodedata
from dogpile.cache.util import sha1_mangle_key
from dogpile.cache.api import NO_VALUE, CacheBackend
from dogpile.util.readwrite_lock import LockError as DogpileLockError

# from recipe.utils import clean_unicode, prettyprintable_sql
from redis.exceptions import ConnectionError
from redis.exceptions import LockError as RedisLockError
from sqlalchemy.orm.query import Query

SLOG = structlog.get_logger(__name__)


class DictionaryBackend(CacheBackend):
    """A simple backend for testing."""

    def __init__(self, arguments):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key, NO_VALUE)

    def set(self, key, value):
        self.cache[key] = value

    def delete(self, key):
        self.cache.pop(key)


def clean_unicode(value):
    """Convert value into ASCII bytes by brute force."""
    if not isinstance(value, str):
        value = str(value)
    try:
        return value.encode("ascii")
    except UnicodeEncodeError:
        value = unicodedata.normalize("NFKD", value)
        return value.encode("ascii", "ignore")


def unicode_sha1_mangle_key(key):
    return sha1_mangle_key(clean_unicode(key))


def mangle_key(key):
    # prefix, key = key.split(":", 1)
    base = "sqlalchemy_recipe:dogpile"
    # if prefix:
    #     base += f"{prefix}"
    # else:
    #     raise ValueError(key)
    return f"{base}:{sha1_mangle_key(key)}"


def _key_from_statement(statement):
    """Given a Statement, create a cache key.

    There are many approaches to this; here we use the simplest,
    which is to create an md5 hash of the text of the SQL statement,
    combined with stringified versions of all the bound parameters
    within it.  There's a bit of a performance hit with
    compiling out "query.statement" here; other approaches include
    setting up an explicit cache key with a particular Query,
    then combining that with the bound parameter values.
    """
    compiled = statement.compile()
    params = compiled.params

    # here we return the key as a long string.  our "key mangler"
    # set up with the region will boil it down to an md5.

    key = " ".join(
        [clean_unicode(compiled).decode("utf-8")]
        + [clean_unicode(params[k]).decode("utf-8") for k in sorted(params)]
    )
    return mangle_key(key)
