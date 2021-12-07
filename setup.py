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
    description="Efficient backup system supporting multiple data sources",
    license="GNU General Public License, version 2",
    install_requires=[
        "argparse",
        "boto >= 2.23.0",
        "click",
        "click-plugins",
        "github3.py < 1.0.0",  # >= 0.9.0
        "google-api-python-client",
        "oauth2client",
        "pybloom-live",
        "python-dateutil",
        "PyYAML",
        "xattr",
    ],
    entry_points={
        "console_scripts": [
            "deltaic = deltaic.command:deltaic_cli",
            "dt-askpass = deltaic.command:askpass",
        ],
        "deltaic.lowlevel": [
            "coda = deltaic.sources.coda:coda",
            "github = deltaic.sources.github:github",
            "googledrive = deltaic.archivers.googledrive:googledrive",
            "rbd = deltaic.sources.rbd:rbd",
            "rgw = deltaic.sources.rgw:rgw",
            "rsync = deltaic.sources.rsync:rsync",
        ],
        "deltaic.sources": [
            "coda = deltaic.sources.coda:CodaSource",
            "github = deltaic.sources.github:GitHubSource",
            "rbd = deltaic.sources.rbd:RBDSource",
            "rgw = deltaic.sources.rgw:RGWSource",
            "rsync = deltaic.sources.rsync:RsyncSource",
        ],
        "deltaic.archivers": [
            "aws = deltaic.archivers.aws:AWSArchiver",
            "googledrive = deltaic.archivers.googledrive:DriveArchiver",
        ],
    },
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
