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
    name='deltaic',
    packages=[
        'deltaic',
        'deltaic.sources',
    ],
    scripts=[
        'bin/deltaic',
        'bin/dt-askpass',
    ],
    description='Efficient backup system supporting multiple data sources',
    license='GNU General Public License, version 2',
    install_requires=[
        'argparse',
        'boto',
        'github3.py >= 0.9.0',
        'pybloom', # >= 2.0
        'python-dateutil',
        'PyYAML',
        'xattr',
    ],
    dependency_links=[
        'git+https://github.com/jaybaird/python-bloomfilter@v2.0#egg=pybloom',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Archiving :: Backup',
    ],
    zip_safe=True,
)
