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

import argparse
import os
import sys


def _get_default_config_path():
    config_dir = os.environ.get("XDG_CONFIG_HOME")
    if config_dir is None:
        config_dir = os.path.join(os.environ["HOME"], ".config")
    return os.path.join(config_dir, "deltaic.conf")


_default_config_path = _get_default_config_path()


def make_subcommand_group(name, help):
    subparser = subparsers.add_parser(name, help=help)
    group = subparser.add_subparsers(metavar="COMMAND")
    group.parser = subparser
    return group


def get_cmdline_for_subcommand(subcommand):
    cmd = [sys.executable, "-u", sys.argv[0]]
    # Append global command-line arguments that must be passed to all
    # sub-invocations.  Reparse the command line so the entire call chain
    # doesn't have to pass the Namespace around.
    args = parser.parse_args()
    if args.config_file != parser.get_default("config_file"):
        cmd.extend(["-c", args.config_file])
    cmd.extend(subcommand)
    return cmd


def sort_subparsers():
    # Ensure top-level help is in a reasonable order
    subparsers._get_subactions().sort(key=lambda k: k.dest)


parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    metavar="PATH",
    default=_default_config_path,
    help="path to config file (default: %s)" % _default_config_path,
)

subparsers = parser.add_subparsers(metavar="COMMAND")
