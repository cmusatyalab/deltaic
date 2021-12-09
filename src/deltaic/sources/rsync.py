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
import re
import subprocess
import sys

import click

from ..command import pass_config
from ..util import make_dir_path, random_do_work
from . import Source, Unit


def remote_command(host, command, user="root"):
    args = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        f"{user}@{host}",
        command,
    ]
    print(" ".join(args))
    subprocess.check_call(args)


def run_rsync(cmd):
    print(" ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    # Filter out spurious log output from
    # https://bugzilla.samba.org/show_bug.cgi?id=10496
    spurious = re.compile(r"[.h][dfL]\.{8}x ")
    for line in proc.stdout:
        line = line.decode(sys.stdout.encoding)
        if not spurious.match(line):
            print(line.strip())

    return proc.wait()


def run_rsync_with_fallback(cmd):
    cmd = list(cmd)
    ret = run_rsync(cmd)
    if ret in (2, 12):
        # Drop features that require protocol >= 30
        cmd.remove("--acls")
        cmd.remove("--xattrs")
        ret = run_rsync(cmd)
    if ret not in (0, 24):
        raise OSError(f"rsync failed with code {ret}")


def backup_host(
    host, root_dir, mounts, exclude=(), scrub=False, rsync=None, user="root"
):
    if rsync is None:
        rsync = "rsync"
    make_dir_path(root_dir)
    args = [
        rsync,
        "-aHRxi",
        "--acls",
        "--xattrs",
        "--fake-super",
        "--delete",
        "--delete-excluded",
        "--numeric-ids",
        "--stats",
        "--partial",
        "--rsh=ssh -o BatchMode=yes -o StrictHostKeyChecking=no",
    ]
    args.extend(["--exclude=" + r for r in exclude])
    args.extend([f"{user}@{host}:{mount.rstrip('/') or '/'}" for mount in mounts])
    args.append(root_dir.rstrip("/"))
    if scrub:
        args.append("--checksum")
    run_rsync_with_fallback(args)


def restore_host(
    source, dest_host, dest_dir, coda=False, user=None, extra_args=None, rsync=None
):
    if rsync is None:
        rsync = "rsync"
    if user is None:
        user = "root"
    source = source.rstrip("/")
    if os.path.isdir(source):
        source += "/"
    dest = f"{user}@{dest_host}:{dest_dir.rstrip('/')}/"

    if not coda:
        # Complain if remote superuser activities (-ogD) fail
        args = [
            rsync,
            "-aHi",
            "--acls",
            "--xattrs",
            "--fake-super",
            "-M--super",
            "--numeric-ids",
            source,
            dest,
        ]
    else:
        # Coda.  No -AXDg, and force -o even though we're likely not
        # superuser.
        args = [
            rsync,
            "-rlptoHi",
            "--fake-super",
            "-M--super",
            "--numeric-ids",
            source,
            dest,
        ]
    if extra_args:
        args.extend(extra_args)
    run_rsync_with_fallback(args)


def get_relroot(hostname, info):
    alias = info.get("alias", hostname.split(".")[0])
    return os.path.join("rsync", alias)


@click.group()
def rsync():
    """low-level rsync support for host filesystems"""


@rsync.command()
@click.option("-c", "--scrub", is_flag=True, help="check backup data against original")
@click.argument("host")
@pass_config
def backup(config, scrub, host):
    """back up host filesystem"""
    settings = config["settings"]
    info = config["rsync"][host]
    root_dir = os.path.join(settings["root"], get_relroot(host, info))
    exclude = settings.get("rsync-exclude", [])
    exclude.extend(info.get("exclude", []))
    rsync = settings.get("rsync-local-binary")
    user = info.get("user", "root")

    if "pre" in info:
        remote_command(host, info["pre"], user=user)
    backup_host(
        host,
        root_dir,
        info["mounts"],
        exclude,
        scrub=scrub,
        rsync=rsync,
        user=user,
    )
    if "post" in info:
        remote_command(host, info["post"], user=user)


@rsync.command()
@click.option(
    "--coda", is_flag=True, help="use options suitable for restoring a Coda volume"
)
@click.option(
    "-u",
    "--user",
    default="root",
    show_default=True,
    help="username on destination host",
)
@click.argument("source")
@click.argument("host")
@click.argument("destdir")
@click.argument("args", nargs=-1)
@pass_config
def restore(config, coda, user, source, host, destdir, args):
    """restore file or directory tree to host filesystem

    ARGS specifies additional arguments for rsync.
    """
    settings = config["settings"]
    rsync = settings.get("rsync-local-binary")
    restore_host(
        source,
        host,
        destdir,
        coda=coda,
        user=user,
        extra_args=args,
        rsync=rsync,
    )


class RsyncUnit(Unit):
    def __init__(self, settings, hostname, info):
        Unit.__init__(self)
        self.root = get_relroot(hostname, info)
        self.backup_args = ["rsync", "backup", hostname]
        if random_do_work(settings, "rsync-scrub-probability", 0.0166):
            self.backup_args.append("-c")


class RsyncSource(Source):
    LABEL = "rsync"

    def get_units(self):
        ret = []
        for hostname, info in sorted(
            self._manifest.items(), key=lambda e: e[1].get("alias", e[0])
        ):
            ret.append(RsyncUnit(self._settings, hostname, info))
        return ret
