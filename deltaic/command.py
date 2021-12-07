#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014-2021 Carnegie Mellon University
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

import shutil
import sys
from pathlib import Path

import click


def _get_default_config_path():
    return Path(click.get_app_dir("deltaic")).with_suffix(".conf")


default_config_path = _get_default_config_path()


pass_config = click.make_pass_decorator(dict)


def get_cmdline_for_subcommand(subcommand):
    cmd = [shutil.which(sys.argv[0])]

    # Append global command-line arguments that must be passed to all
    # sub-invocations.
    ctx = click.get_current_context()
    root_ctx = ctx.find_root()
    config_file = root_ctx.params["config_file"]
    if not config_file.samefile(default_config_path):
        cmd.extend(["-c", str(config_file)])

    cmd.extend(subcommand)
    return cmd
