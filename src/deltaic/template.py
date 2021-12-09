#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014-2021 Carnegie Mellon University
#
# SPDX-License-Identifier: GPL-2.0-only
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

import os
import sys
from getpass import getuser

import click

TEMPLATES = {
    "crontab": """
MAILTO = %(email)s

0 23 * * * %(prog)s prune
55 23 * * * %(prog)s df -c
0 0 * * * %(prog)s run >/dev/null && echo "OK"

# To run offsite archives, enable these jobs and configure their schedule
#0 0 1 1,4,7,10 * %(prog)s archive run
#0 4 * * * %(prog)s archive prune
#0 3 30 6,12 * %(prog)s archive resync
""",
    "sudoers": """
# Allow Deltaic to query, create, delete, mount, and unmount snapshot volumes
%(user)s ALL=NOPASSWD: /sbin/lvs, /sbin/lvcreate, /sbin/lvremove, /sbin/lvchange, /bin/mount, /bin/umount
# Allow running sudo from cron
Defaults:%(user)s !requiretty
""",
}


@click.command()
@click.option("--email", default="root", help="email address to send status reports")
@click.argument("file", type=click.Choice(list(TEMPLATES)), required=True)
def mkconf(email, file):
    """generate a config file template"""
    print(
        TEMPLATES[file].strip()
        % {
            "user": getuser(),
            "prog": os.path.abspath(sys.argv[0]),
            "email": email,
        }
    )
