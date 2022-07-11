"""A base class for expressions"""

import time
from unittest import TestCase

from dogpile.cache import register_backend

# from freezegun import freeze_time
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Table,
)

from sqlalchemy_recipe.dbinfo import get_dbinfo
from tests.utils import test_db

utc_offset = -1 * time.localtime().tm_gmtoff / 3600.0 + time.localtime().tm_isdst


register_backend("dictionary", "sqlalchemy_recipe.caching", "DictionaryBackend")


configs = [
    {
        "sqlalchemy.url": f"sqlite:///{test_db}",
        "sqlalchemy.pool_pre_ping": True,
        "cache.cache_queries": True,
        "cache.dogpile.backend": "dictionary",
        "cache.dogpile.expiration_time": 2,
    },
    {"sqlalchemy.url": f"sqlite:///{test_db}"},
    {"sqlalchemy.url": f"sqlite:///{test_db}"},
]


class ExpressionTestCase(TestCase):
    """Grammar is constructed correctly."""

    def setUp(self):

        config = {
            "sqlalchemy.url": f"sqlite:///{test_db}",
            "sqlalchemy.pool_pre_ping": True,
            "cache.cache_queries": True,
            "cache.dogpile.backend": "dictionary",
            "cache.dogpile.expiration_time": 2,
        }
        self.dbinfo = get_dbinfo(config)

        basic_table = Table(
            "foo",
            self.dbinfo.metadata,
            Column("first", String),
            Column("last", String),
            Column("age", Integer),
            Column("birth_date", Date),
            Column("dt", DateTime),
            extend_existing=True,
        )

        datetester_table = Table(
            "datetester",
            self.dbinfo.metadata,
            Column("dt", Date),
            Column("count", Integer),
            extend_existing=True,
        )

        scores_table = Table(
            "scores",
            self.dbinfo.metadata,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            extend_existing=True,
        )

        datatypes_table = Table(
            "datatypes",
            self.dbinfo.metadata,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            Column("test_datetime", DateTime),
            Column("valid_score", Boolean),
            extend_existing=True,
        )

        scores_with_nulls_table = Table(
            "scores_with_nulls",
            self.dbinfo.metadata,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            extend_existing=True,
        )

        tagscores_table = Table(
            "tagscores",
            self.dbinfo.metadata,
            Column("username", String),
            Column("tag", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            extend_existing=True,
        )

        id_tests_table = Table(
            "id_tests",
            self.dbinfo.metadata,
            Column("student", String),
            Column("student_id", Integer),
            Column("age", Integer),
            Column("age_id", Integer),
            Column("score", Float),
            extend_existing=True,
        )

        census_table = Table(
            "census",
            self.dbinfo.metadata,
            Column("state", String),
            Column("sex", String),
            Column("age", Integer),
            Column("pop2000", Integer),
            Column("pop2008", Integer),
            extend_existing=True,
        )

        state_fact_table = Table(
            "state_fact",
            self.dbinfo.metadata,
            Column("id", String),
            Column("name", String),
            Column("abbreviation", String),
            Column("sort", String),
            Column("status", String),
            Column("occupied", String),
            Column("notes", String),
            Column("fips_state", String),
            Column("assoc_press", String),
            Column("standard_federal_region", String),
            Column("census_region", String),
            Column("census_region_name", String),
            Column("census_division", String),
            Column("census_division_name", String),
            # JSON is an unusable datatype for expressions
            Column("circuit_court", JSON),
            extend_existing=True,
        )

        self.tables = [
            basic_table,
            datetester_table,
            scores_table,
            datatypes_table,
            scores_with_nulls_table,
            tagscores_table,
            id_tests_table,
            census_table,
            state_fact_table,
        ]

        return super().setUp()
