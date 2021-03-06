# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Required packages for install, test, docs, and tests."""

import os
import re

from setuptools import setup, find_packages


install_requires = [
    'future',
    'appdirs>=1.4.4',
    'pandas>=1.0.0',
    'jsonschema>=3.2.0',
    'python-dateutil',
    'pyyaml',
    'psutil',
    'SQLAlchemy>=1.3.18',
    'Click>=7.0'
]


tests_require = [
    'coverage>=4.0',
    'pytest',
    'pytest-cov'
]


dev_require = ['flake8', 'python-language-server'] + tests_require


extras_require = {
    'docs': [
        'Sphinx',
        'sphinx-rtd-theme',
        'sphinxcontrib-apidoc'
    ],
    'tests': tests_require,
    'dev': dev_require
}


# Get the version string from the version.py file in the histore package.
# Based on:
# https://stackoverflow.com/questions/458550
with open(os.path.join('histore', 'version.py'), 'rt') as f:
    filecontent = f.read()
match = re.search(r"^__version__\s*=\s*['\"]([^'\"]*)['\"]", filecontent, re.M)
if match is not None:
    version = match.group(1)
else:
    raise RuntimeError('unable to find version string in %s.' % (filecontent,))


# Get long project description text from the README.rst file
with open('README.rst', 'rt') as f:
    readme = f.read()


setup(
    name='histore',
    version=version,
    description='Library for maintaining evolving tabular data sets',
    long_description=readme,
    long_description_content_type='text/x-rst',
    keywords='data versioning',
    url='https://github.com/heikomuller/histore',
    author='Heiko Mueller',
    author_email='heiko.muller@gmail.com',
    license='New BSD',
    license_file='LICENSE',
    packages=find_packages(exclude=('tests',)),
    include_package_data=True,
    extras_require=extras_require,
    tests_require=tests_require,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'histore = histore.cli.base:cli',
        ]
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python'
    ]
)
