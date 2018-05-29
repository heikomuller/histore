#! /usr/bin/env python

from setuptools import setup

setup(
    name='histore',
    version='0.1.0',
    description='Snapshot management for structured datasets',
    keywords='data versioning ',
    url='https://github.com/heikomuller/histore',
    license='New BSD',
    packages=['histore'],
    package_data={'': ['LICENSE']},
    install_requires=[
        'pyyaml'
    ]
)
