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

import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Optional

import click

from ..command import pass_config
from ..platform import lutime
from ..util import (
    BloomSet,
    XAttrs,
    gc_directory_tree,
    make_dir_path,
    random_do_work,
    update_file,
)
from . import Source, Unit

ATTR_INCREMENTAL = "user.coda.incremental-ok"
ATTR_STAT = "user.rsync.%stat"
DUMP_ATTEMPTS = 10


class DumpError(Exception):
    pass


def volutil_cmd(host, subcommand, args=(), volutil=None):
    if volutil is None:
        volutil = "volutil"
    print(">", volutil, subcommand, " ".join(args))
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        f"root@{host}",
        volutil,
        subcommand,
    ] + list(args)


def get_err_stream(verbose):
    return None if verbose else subprocess.DEVNULL


def get_volume_ids(host, volume, verbose=False, volutil=None):
    try:
        info = subprocess.run(
            volutil_cmd(host, "info", [volume], volutil=volutil),
            stdout=subprocess.PIPE,
            stderr=get_err_stream(verbose),
            encoding=sys.stdout.encoding,
            check=True,
        ).stdout
    except subprocess.CalledProcessError:
        raise OSError(f"Couldn't get volume info for {volume}")

    match = re.search("^id = ([0-9a-f]+)", info, re.MULTILINE)
    if match is None:
        raise ValueError(f"Couldn't find volume ID for {volume}")
    volume_id = match.group(1)

    match = re.search(", backupId = ([0-9a-f]+)", info)
    if match is None:
        raise ValueError(f"Couldn't find backup ID for {volume}")
    backup_id = match.group(1)

    return volume_id, backup_id


def refresh_backup_volume(host, volume, verbose=False, volutil=None):
    volume_id, _ = get_volume_ids(host, volume, verbose, volutil=volutil)
    subprocess.run(
        volutil_cmd(host, "lock", [volume_id], volutil=volutil),
        stdout=get_err_stream(verbose),
        stderr=get_err_stream(verbose),
        check=True,
    )
    subprocess.run(
        volutil_cmd(host, "backup", [volume_id], volutil=volutil),
        stdout=get_err_stream(verbose),
        stderr=get_err_stream(verbose),
        check=True,
    )


def build_path(root_dir, path):
    absolute_path = Path(root_dir) / path

    if ".." in absolute_path.parts:
        raise ValueError(f"Attempted directory traversal: {path}")

    return absolute_path


class TarMemberFile:
    # Wrapper around tarfile.ExFileObject that raises DumpError when
    # the tar stream is truncated mid-file.  This prevents us from
    # replacing a complete backup file with a truncated one.

    def __init__(self, tar, entry):
        self._file = tar.extractfile(entry)
        self._remaining = entry.size

    def read(self, size=None):
        buf = self._file.read(size)
        self._remaining -= len(buf)
        if self._remaining > 0 and (buf == b"" or size is None):
            raise DumpError("Premature EOF on tar stream")
        return buf

    def close(self):
        self._file.close()


def update_dir_from_tar(tar, root_dir):
    directories = []
    valid_paths = BloomSet()
    for entry in tar:
        # Convert entry type to stat constant
        if entry.type == tarfile.DIRTYPE:
            entry_stat_type = stat.S_IFDIR
        elif entry.type in (tarfile.REGTYPE, tarfile.LNKTYPE):
            # Coda apparently doesn't allow hard links to symlinks
            entry_stat_type = stat.S_IFREG
        elif entry.type == tarfile.SYMTYPE:
            entry_stat_type = stat.S_IFLNK
        else:
            raise ValueError(f"Unexpected file type {entry.type}")

        # Check for existing file
        path = build_path(root_dir, entry.name)
        try:
            st: Optional[os.stat_result] = os.lstat(path)
        except OSError:
            st = None

        # If entry has changed types, remove the old object
        if st is not None and stat.S_IFMT(st.st_mode) != entry_stat_type:
            if stat.S_ISDIR(st.st_mode):
                shutil.rmtree(path)
            else:
                os.unlink(path)
            st = None

        # Create parent directory if not present.  Parents are not
        # necessarily dumped before children.
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create new object
        if entry.isdir():
            if not os.path.exists(path):
                print("d", path)
                os.mkdir(path)
            # Go back and set mtime after directory has been populated
            directories.append(entry)
        elif entry.isfile():
            # update_file() will break hard links if it modifies the file.
            # This is what we want because links may have also been broken
            # at the source.  codadump2tar always dumps hard links, so we
            # will rebuild any links that should still exist.
            if update_file(path, TarMemberFile(tar, entry)):
                print("f", path)
        elif entry.issym():
            if entry.linkname and (st is None or os.readlink(path) != entry.linkname):
                print("s", path)
                if st is not None:
                    os.unlink(path)
                os.symlink(entry.linkname, path)
        elif entry.islnk():
            target_path = build_path(root_dir, entry.linkname)
            target_st = os.lstat(target_path)
            if (
                st is None
                or st.st_dev != target_st.st_dev
                or st.st_ino != target_st.st_ino
            ):
                print("l", path)
                if st is not None:
                    os.unlink(path)
                os.link(target_path, path)

        # Update metadata
        attrs = XAttrs(path)
        # owner and mode.  Hardlinks were updated with the primary, and we
        # can't set xattrs on symlinks.
        if entry.isfile() or entry.isdir():
            # rsync --fake-super compatible:
            # octal_mode_with_type major,minor uid:gid
            mode = entry_stat_type | entry.mode
            attrs.update(
                ATTR_STAT,
                f"{mode:o} 0,0 {entry.uid}:{entry.gid}",
            )
        # mtime.  Directories will be updated later, and hardlinks were
        # updated with the primary.
        if (
            (entry.isfile() or entry.issym())
            and path.exists()
            and os.lstat(path).st_mtime != entry.mtime
        ):
            lutime(path, entry.mtime)

        # Protect from garbage collection
        valid_paths.add(str(path))

    # Deferred update of directory mtimes
    for entry in directories:
        path = build_path(root_dir, entry.name)
        if os.stat(path).st_mtime != entry.mtime:
            os.utime(path, (entry.mtime, entry.mtime))

    return valid_paths


def update_dir(
    host, backup_id, root_dir, incremental=False, volutil=None, codadump2tar=None
):
    if codadump2tar is None:
        codadump2tar = "codadump2tar"
    args = ["-i", "-1"] if incremental else []
    args.extend([backup_id, "|", codadump2tar, "-rn", "."])

    proc = subprocess.Popen(
        volutil_cmd(host, "dump", args, volutil=volutil),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    try:
        tar = tarfile.open(fileobj=proc.stdout, mode="r|")
        valid_paths = update_dir_from_tar(tar, root_dir)
    except tarfile.ReadError as e:
        raise DumpError(str(e))

    if proc.wait():
        raise DumpError(f"Coda dump returned {proc.returncode}")
    return valid_paths


def sync_backup_volume(
    host,
    volume,
    root_dir,
    incremental=False,
    verbose=False,
    volutil=None,
    codadump2tar=None,
):
    make_dir_path(root_dir)

    # Force a full backup unless the root_dir is marked incremental-ok.
    # This ensures that the first backup is a full.  Remove incremental-ok
    # attr when a full backup is requested, to ensure that full-backup
    # failures are reproducible in the next backup run.
    root_xattrs = XAttrs(root_dir)
    if ATTR_INCREMENTAL not in root_xattrs:
        incremental = False
    if not incremental:
        root_xattrs.delete(ATTR_INCREMENTAL)

    _, backup_id = get_volume_ids(host, volume, verbose, volutil=volutil)

    # Retry dump a few times to paper over rpc2 timeouts
    for tries_remaining in range(DUMP_ATTEMPTS - 1, -1, -1):
        try:
            valid_paths = update_dir(
                host,
                backup_id,
                root_dir,
                incremental=incremental,
                volutil=volutil,
                codadump2tar=codadump2tar,
            )
            break
        except DumpError:
            if not tries_remaining:
                raise

    if not incremental:

        def report(path, is_dir):
            print("-", path)

        gc_directory_tree(root_dir, valid_paths, report)

    subprocess.run(
        volutil_cmd(host, "ancient", [backup_id], volutil=volutil),
        stderr=get_err_stream(verbose),
        check=True,
    )
    root_xattrs.update(ATTR_INCREMENTAL, "true")


def get_relroot(hostname, volume):
    return os.path.join("coda", hostname.split(".")[0], volume)


@click.group()
def coda():
    """low-level Coda support"""


@coda.command()
@click.option("-i", "--incremental", is_flag=True, help="request incremental backup")
@click.option(
    " /-R",
    "--refresh/--skip-refresh",
    default=True,
    show_default=True,
    help="refresh backup volume",
)
@click.option("-v", "--verbose", is_flag=True, help="show volutil output")
@click.argument("host")
@click.argument("volume")
@pass_config
def backup(config, incremental, refresh, verbose, host, volume):
    """back up Coda volume"""
    settings = config["settings"]
    root_dir = os.path.join(settings["root"], get_relroot(host, volume))
    volutil = settings.get("coda-volutil-path", "volutil")
    codadump2tar = settings.get("coda-codadump2tar-path", "codadump2tar")
    if refresh:
        refresh_backup_volume(host, volume, verbose=verbose, volutil=volutil)
    sync_backup_volume(
        host,
        volume,
        root_dir,
        incremental=incremental,
        verbose=verbose,
        volutil=volutil,
        codadump2tar=codadump2tar,
    )


class CodaUnit(Unit):
    def __init__(self, settings, server, volume):
        Unit.__init__(self)
        self.root = get_relroot(server, volume)
        self.backup_args = ["coda", "backup", server, volume]
        if random_do_work(settings, "coda-full-probability", 0.143):
            self.backup_args.append("-i")


class CodaSource(Source):
    LABEL = "coda"

    def get_units(self):
        ret = []
        for group in self._manifest:
            for volume in group["volumes"]:
                for server in group["servers"]:
                    ret.append(CodaUnit(self._settings, server, volume))
        return ret
