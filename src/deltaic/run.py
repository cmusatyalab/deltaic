#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014-2022 Carnegie Mellon University
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

import sys

import click

from .command import pass_config
from .sources import Source
from .storage import PhysicalSnapshot
from .util import lockfile


def back_up_units(config):
    tasks = []
    for source_type in Source.get_sources().values():
        source = source_type(config)
        task = source.get_backup_task()
        task.start()
        tasks.append(task)
    success = True
    for task in tasks:
        if not task.wait():
            success = False
    return success


@click.command()
@click.option(
    " /-S",
    "--snapshot/--no-snapshot",
    default=True,
    show_default=True,
    help="snapshot backup volume",
)
@pass_config
def run(config, snapshot):
    """run a backup"""
    settings = config["settings"]
    with lockfile(settings, "backup"):
        success = back_up_units(config)
        if snapshot:
            PhysicalSnapshot.create(settings, verbose=True)
        if not success:
            sys.exit(1)
