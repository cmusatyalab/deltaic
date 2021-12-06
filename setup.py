#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014 Carnegie Mellon University
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of version 2 of the GNU General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

from setuptools import setup

setup(
    name="deltaic",
    packages=[
        "deltaic",
        "deltaic.sources",
    ],
    scripts=[
        "bin/deltaic",
        "bin/dt-askpass",
    ],
    description="Efficient backup system supporting multiple data sources",
    license="GNU General Public License, version 2",
    install_requires=[
        "argparse",
        "boto >= 2.23.0",
        "github3.py < 1.0.0",  # >= 0.9.0
        "google-api-python-client",
        "oauth2client",
        "pybloom-live",
        "python-dateutil",
        "PyYAML",
        "xattr",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Topic :: System :: Archiving :: Backup",
    ],
    zip_safe=True,
)
