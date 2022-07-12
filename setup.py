#!/usr/bin/env python3

from sys import version
from setuptools import setup, find_packages

from msks import __version__

setup(
    name='msks',
    description="Reproducible task execution with Conda",
    version=__version__,
    packages=find_packages(),
    install_requires=[
        "PyYAML>=5.4",
        "gitpython>=3.1",
        "filelock>=3.0",
        "bidict>=0.21",
        "attributee>=0.1.1",
        "argcomplete>=1.12",
        "watchgod>=0.7",
        "rich>=9.11",
        "filtration>=2.2.0"
    ],
    entry_points = {
        'msks': ['msks=msks:main'],
    }

)
