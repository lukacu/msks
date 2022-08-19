#!/usr/bin/env python3

from setuptools import setup, find_packages

from msks import __version__

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
    name='msks',
    description="Reproducible task execution with Conda",
    version=__version__,
    packages=find_packages(),
    install_requires=install_requires,
    entry_points = {
        'msks': ['msks=msks:main'],
    }

)
