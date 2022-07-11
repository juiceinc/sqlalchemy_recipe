from sqlalchemy_recipe.core import *
from sqlalchemy import create_engine, text, engine_from_config, MetaData, select
from unittest import TestCase

from sqlalchemy_recipe.dbinfo import get_dbinfo, _REFLECTED_TABLE_CACHE

from tests.utils import test_db


def make_dumper(engine):
    def dump(sql, *multiparams, **params):
        print(sql.compile(dialect=engine.dialect))

    return dump


class RecipeTestCase(TestCase):
    def setUp(self) -> None:
        from dogpile.cache import register_backend

        register_backend("dictionary", "sqlalchemy_recipe.caching", "DictionaryBackend")

        config = {
            "sqlalchemy.url": f"sqlite:///{test_db}",
            "sqlalchemy.pool_pre_ping": True,
            "cache.cache_queries": True,
            "cache.dogpile.backend": "dictionary",
            "cache.dogpile.expiration_time": 2,
        }
        self.dbinfo = get_dbinfo(config)
        return super().setUp()

    def testit(self):
        reflected_table = self.dbinfo.reflect("census")
        self.assertEqual(len(reflected_table.table.c), 5)
        with self.dbinfo.engine.connect() as conn:
            stmt = select(reflected_table.table).limit(5)
            result = conn.execute(stmt)
            print(result.all())
            self.assertEqual(len(_REFLECTED_TABLE_CACHE), 1)
        self.assertEqual(1, 1)

    def testcache(self):
        from dogpile.cache import make_region

        region = make_region("myregion", "dictionary")

        region.configure("dictionary", expiration_time=300)

        data = "somevalue"
        region.set("somekey", data)
        newdata = region.get("somekey")
        self.assertEqual(data, newdata)
        missing = region.get("missingval")
        from dogpile.cache.api import NoValue

        self.assertIsInstance(missing, NoValue)

    def testcache_query(self):
        """We can query with cache using dbinfo"""
        reflected_table = self.dbinfo.reflect("census")
        for i in range(5):
            stmt = select(reflected_table.table).limit(1)
            rez = self.dbinfo.execute(stmt)
            print(rez)
        self.assertEqual(1, 1)
