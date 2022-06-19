#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()

if sys.argv[-1] == "test":
    try:
        __import__("py")
    except ImportError:
        print("py.test required.")
        sys.exit(1)

    errors = os.system("py.test tests/")
    sys.exit(bool(errors))

install = [
    "faker",
    "SQLAlchemy>=1.4",
    "tablib",
    "pyyaml",
    "dateparser",
    "attrs",
    "lark",
]

setup(
    name="sqlalchemy_recipe",
    version="0.1",
    description="Build SQL queries from configuration",
    long_description=(open("README.rst").read()),
    author="Chris Gemignani",
    author_email="chris.gemignani@juiceanalytics.com",
    url="https://github.com/juiceinc/sqlalchemy_recipe",
    packages=find_packages(include=["sqlalchemy_recipe*"]),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    tests_require=["pytest", "pytest-cov"],
    install_requires=install,
)
