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
from pathlib import Path

import click
import yaml
from click_plugins import with_plugins
from pkg_resources import iter_entry_points

from .archivers import archive
from .command import default_config_path
from .prune import prune
from .run import run
from .storage import df, ls, mount, umount
from .template import mkconf

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@with_plugins(iter_entry_points("deltaic.lowlevel"))
@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-c",
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=default_config_path,
    show_default=True,
    help="path to config file",
)
@click.pass_context
def deltaic_cli(ctx, config_file):
    if os.geteuid() == 0:
        raise OSError("Do not run as root!")

    with config_file.open() as fh:
        config = yaml.safe_load(fh)

    ctx.obj = config


deltaic_cli.add_command(archive)
deltaic_cli.add_command(prune)
deltaic_cli.add_command(run)
deltaic_cli.add_command(df)
deltaic_cli.add_command(ls)
deltaic_cli.add_command(mount)
deltaic_cli.add_command(umount)
deltaic_cli.add_command(mkconf)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("prompt", required=True)
def askpass(prompt):
    """Returns username or password based on askpass prompt string."""
    if "Username" in prompt:
        print(os.environ["DT_ASKPASS_USER"])
    elif "Password" in prompt:
        print(os.environ["DT_ASKPASS_PASS"])
    else:
        raise click.UsageError(f"Unrecognized prompt: {prompt}")
