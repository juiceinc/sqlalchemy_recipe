from sqlalchemy_recipe.core import *
from sqlalchemy import create_engine, text, engine_from_config, MetaData
from unittest import TestCase
import os

from sqlalchemy_recipe.dbinfo import get_dbinfo

dir_path = os.path.dirname(os.path.realpath(__file__))
test_db = os.path.join(dir_path, "data", "test_db.sqlite")


def make_dumper(engine):
    def dump(sql, *multiparams, **params):
        print(sql.compile(dialect=engine.dialect))

    return dump


class RecipeTestCase(TestCase):
    def setUp(self) -> None:
        config = {
            "sqlalchemy.url": f"sqlite:///{test_db}",
            "sqlalchemy.pool_pre_ping": True,
        }
        self.dbinfo = get_dbinfo(config)
        return super().setUp()

    def testit(self):
        tbl = self.dbinfo.reflect("census")
        print(tbl, type(tbl), tbl.c)
        print(self.dbinfo.grammars["census"])
        with self.dbinfo.engine.connect() as conn:
            result = conn.execute(text("select * from census limit 10"))
            print(result.all())
        self.assertEqual(1, 2)
