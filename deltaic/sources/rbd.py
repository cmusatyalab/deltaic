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

import contextlib
import json
import os
import struct
import subprocess
import sys
import uuid

import click

from ..command import pass_config
from ..platform import punch
from ..util import XAttrs, make_dir_path, random_do_work
from . import Source, Unit

BLOCKSIZE = 256 << 10
DIFF_MAGIC = b"rbd diff v1\n"
PENDING_EXT = ".pending"
ATTR_SNAPSHOT = "user.rbd.snapshot"
ATTR_PENDING_SNAPSHOT = "user.rbd.pending-snapshot"
ATTR_SNAPID = "user.rbd.snapid"


def rbd_exec(pool, cmd, *args):
    cmdline = ["rbd", cmd] + list(args) + ["-p", pool]
    print(" ".join(cmdline))
    subprocess.check_call(cmdline)


def rbd_query(pool, *args):
    cmd = ["rbd"] + list(args) + ["-p", pool, "--format=json"]
    try:
        out = subprocess.check_output(cmd).decode(sys.stdout.encoding)
    except subprocess.CalledProcessError as e:
        raise OSError("rbd query returned %d" % e.returncode)
    return json.loads(out)


def rbd_pool_info(pool):
    return rbd_query(pool, "ls", "-l")


def rbd_list_snapshots(pool, image):
    return rbd_query(pool, "snap", "ls", "-l", image)


def get_image_for_snapshot(pool, snapshot):
    for cur in rbd_pool_info(pool):
        if cur.get("snapshot") == snapshot:
            return cur["image"]
    raise KeyError("Couldn't locate snapshot %s" % snapshot)


def get_snapid_for_snapshot(pool, image, snapshot):
    for cur in rbd_list_snapshots(pool, image):
        if cur["name"] == snapshot:
            return cur["id"]
    raise KeyError("Couldn't locate snapshot %s" % snapshot)


def create_snapshot(pool, image):
    snapshot = "backup-%s" % uuid.uuid1()
    rbd_exec(pool, "snap", "create", "--image", image, "--snap", snapshot)
    return snapshot


def delete_snapshot(pool, image, snapshot):
    rbd_exec(pool, "snap", "rm", "--image", image, "--snap", snapshot)


def try_unlink(path):
    with contextlib.suppress(OSError):
        os.unlink(path)


class LazyWriteFile:
    def __init__(self, path, create=False):
        self._path = path
        if create and not os.path.exists(path):
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
            self._fh = os.fdopen(fd, "r+b")
        else:
            self._fh = open(path, "rb")

    def __reopen_rw__(self):
        if self._fh.closed or "+" in self._fh.mode:
            return
        print("re-opening", self._path, "rw")
        offset = self._fh.tell()
        self._fh = open(self._path, "r+b")
        self._fh.seek(offset)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    def read(self, size=-1):
        return self._fh.read(size)

    def readline(self):
        return self._fh.readline()

    def readlines(self):
        return self._fh.readlines()

    def seek(self, offset, whence=0):
        self._fh.seek(offset, whence)

    def tell(self):
        return self._fh.tell()

    def write(self, buf):
        self.__reopen_rw__()
        self._fh.write(buf)

    def writelines(self, seq):
        self.__reopen_rw__()
        self._fh.writelines(self, seq)

    def truncate(self, len):
        saved_offset = self._fh.tell()
        self._fh.seek(0, 2)
        filesize = self._fh.tell()
        self._fh.seek(saved_offset)

        if filesize != len:
            self.__reopen_rw__()
            self._fh.truncate(len)

    def close(self):
        self._fh.close()

    @property
    def mode(self):
        return self._fh.mode

    def punch(self, offset, length):
        # deltaic.platform.punch will call this.
        # Just repunch the file rather than checking for zeroes.
        self.__reopen_rw__()
        punch(self._fh, offset, length)


class ScrubbingFile(LazyWriteFile):
    """A writable file-like object that compares its input to the file data
    at the specified path.  If we find a mismatch, fix it in-place."""

    def __init__(self, path):
        super().__init__(path)

    def write(self, buf):
        start = 0
        while start < len(buf):
            disk_buf = self._fh.read(min(BLOCKSIZE, len(buf) - start))
            count = len(disk_buf)
            input_buf = buf[start : start + count]
            if disk_buf != input_buf:
                self._fh.seek(-count, 1)
                print("Fixing data mismatch at %d" % (self._fh.tell()), file=sys.stderr)
                super(LazyWriteFile, self).write(input_buf)
            start += count

    def punch(self, offset, length):
        # deltaic.platform.punch will call this.
        if "+" not in self._fh.mode:
            self._fh.seek(offset)
            buf = self._fh.read(length)
            bufsize = len(buf)
            if bufsize == length and bufsize == buf.count(b"\0"):
                return
        super(LazyWriteFile, self).punch(offset, length)


def export_diff(pool, image, snapshot, basis=None, fh=subprocess.PIPE):
    cmd = [
        "rbd",
        "export-diff",
        "--no-progress",
        "-p",
        pool,
        image,
        "--snap",
        snapshot,
        "-",
    ]
    if basis is not None:
        cmd.extend(["--from-snap", basis])
    print(" ".join(cmd))
    return subprocess.Popen(cmd, stdout=fh)


def read_items(fh, fmt):
    size = struct.calcsize(fmt)
    buf = fh.read(size)
    items = struct.unpack(fmt, buf)
    if len(items) == 1:
        return items[0]
    else:
        return items


def unpack_diff(ifh, ofh, verbose=True):
    total_size = 0
    total_changed = 0
    # Read header
    buf = ifh.read(len(DIFF_MAGIC))
    if buf != DIFF_MAGIC:
        raise OSError("Missing diff magic string")
    # Read each record
    while True:
        type = read_items(ifh, "c")
        if type in (b"f", b"t"):
            # Source/dest snapshot name => ignore
            size = read_items(ifh, "<I")
            ifh.read(size)
        elif type == b"s":
            # Image size
            total_size = read_items(ifh, "<Q")
            ofh.truncate(total_size)
        elif type == b"w":
            # Data
            offset, length = read_items(ifh, "<QQ")
            total_changed += length
            ofh.seek(offset)
            while length > 0:
                buf = ifh.read(min(length, BLOCKSIZE))
                ofh.write(buf)
                length -= len(buf)
        elif type == b"z":
            # Zero data
            offset, length = read_items(ifh, "<QQ")
            total_changed += length
            punch(ofh, offset, length)
        elif type == b"e":
            if ifh.read(1) != b"":
                raise OSError("Expected EOF, didn't find it")
            break
        else:
            raise ValueError("Unknown record type: %s" % type)
    if verbose:
        print("%d bytes written, %d total" % (total_changed, total_size))


def fetch_snapshot(pool, image, snapshot, path):
    if os.path.exists(path):
        os.unlink(path)
    proc = export_diff(pool, image, snapshot)
    try:
        with LazyWriteFile(path, create=True) as ofh:
            unpack_diff(proc.stdout, ofh)
        if proc.wait():
            raise OSError("Export returned %d" % proc.returncode)
        XAttrs(path).update(ATTR_SNAPSHOT, snapshot)
    except Exception:
        os.unlink(path)
        raise


def scrub_snapshot(pool, image, snapshot, path):
    proc = export_diff(pool, image, snapshot)
    with ScrubbingFile(path) as ofh:
        unpack_diff(proc.stdout, ofh, verbose=False)
    if proc.wait():
        raise OSError("Export returned %d" % proc.returncode)


def fetch_image(pool, image, path):
    snapshot = create_snapshot(pool, image)
    try:
        fetch_snapshot(pool, image, snapshot, path)
    except Exception:
        delete_snapshot(pool, image, snapshot)
        raise


def make_patch(pool, image, path):
    attrs = XAttrs(path)
    old_snapshot = attrs[ATTR_SNAPSHOT]
    pending_path = path + PENDING_EXT
    if ATTR_PENDING_SNAPSHOT in attrs:
        raise OSError("Pending snapshot already exists")
    # Delete any leftover file from partial previous run
    try_unlink(pending_path)

    new_snapshot = create_snapshot(pool, image)
    try:
        with open(pending_path, "wb") as fh:
            proc = export_diff(pool, image, new_snapshot, basis=old_snapshot, fh=fh)
        if proc.wait():
            raise OSError("Export returned %d" % proc.returncode)
        attrs.update(ATTR_PENDING_SNAPSHOT, new_snapshot)
    except Exception:
        try_unlink(pending_path)
        delete_snapshot(pool, image, new_snapshot)
        raise


def apply_patch_and_rebase(pool, image, path):
    pending_path = path + PENDING_EXT
    attrs = XAttrs(path)
    old_snapshot = attrs[ATTR_SNAPSHOT]
    new_snapshot = attrs.get(ATTR_PENDING_SNAPSHOT)
    if new_snapshot is None:
        # Nothing to do
        try_unlink(pending_path)
        return
    with open(pending_path, "rb") as ifh, LazyWriteFile(path, create=True) as ofh:
        unpack_diff(ifh, ofh)
    delete_snapshot(pool, image, old_snapshot)
    attrs.update(ATTR_SNAPSHOT, new_snapshot)
    attrs.delete(ATTR_PENDING_SNAPSHOT)
    os.unlink(pending_path)


def backup_image(pool, image, path):
    old_snapshot = XAttrs(path).get(ATTR_SNAPSHOT)
    if old_snapshot is not None:
        # Check for common base image
        try:
            old_image = get_image_for_snapshot(pool, old_snapshot)
        except KeyError:
            old_image = None
        if image != old_image:
            # Base image has changed
            if old_image is not None:
                with contextlib.suppres(subprocess.CalledProcessError):
                    delete_snapshot(pool, old_image, old_snapshot)

            os.unlink(path)
            old_snapshot = None

    if old_snapshot is None:
        # Full backup
        fetch_image(pool, image, path)
    else:
        # Incremental
        # Finish previous incremental, if any
        apply_patch_and_rebase(pool, image, path)
        make_patch(pool, image, path)
        apply_patch_and_rebase(pool, image, path)


def backup_snapshot(pool, snapshot, path):
    image = get_image_for_snapshot(pool, snapshot)
    snapid = get_snapid_for_snapshot(pool, image, snapshot)
    attrs = XAttrs(path)
    if str(snapid) == attrs.get(ATTR_SNAPID):
        return
    # Okay, snapshot has changed.  Delete the current backup and start over.
    # (Inefficient, but we assume this doesn't happen very often.)
    fetch_snapshot(pool, image, snapshot, path)
    attrs.update(ATTR_SNAPID, str(snapid))


def restore_image(path, pool, image):
    if XAttrs(path).get(ATTR_PENDING_SNAPSHOT):
        # Should only happen if unpack_diff() failed during the backup,
        # which likely means we didn't understand something about the diff
        # format.  Properly restoring the backup would require unpacking the
        # pending diff onto it first.  In order to do that, we'd need to
        # copy the backup somewhere else first (probably while preserving
        # sparseness), since we may be restoring from a read-only LVM
        # snapshot.
        raise ValueError("Backup image has partially-applied diff")
    rbd_exec(pool, "import", path, image)


def drop_image_snapshots(pool, path):
    if not os.path.exists(path):
        return
    attrs = XAttrs(path)
    for attr in ATTR_SNAPSHOT, ATTR_PENDING_SNAPSHOT:
        snapshot = attrs.get(attr)
        if snapshot:
            try:
                image = get_image_for_snapshot(pool, snapshot)
                delete_snapshot(pool, image, snapshot)
            except (KeyError, subprocess.CalledProcessError):
                pass
            attrs.delete(attr)
    try_unlink(path + PENDING_EXT)


def get_relroot(pool, friendly_name, snapshot=False):
    return os.path.join(
        "rbd", pool, "snapshots" if snapshot else "images", friendly_name
    )


@click.group()
def rbd():
    """low-level RBD image/snapshot support"""


@rbd.command()
@click.option("-c", "--scrub", is_flag=True, help="check backup data against original")
@click.option("-s", "--snapshot", is_flag=True, help="requested object is a snapshot")
@click.argument("pool")
@click.argument("friendly_name")
@pass_config
def backup(config, scrub, snapshot, pool, friendly_name):
    """back up RBD image or snapshot

    Back up image or snapshot from POOL. FRIENDLY_NAME is the name used in the
    config for the image or snapshot.
    """
    settings = config["settings"]
    object_name = config["rbd"][pool]["snapshots" if snapshot else "images"][
        friendly_name
    ]
    out_path = os.path.join(
        settings["root"],
        get_relroot(pool, friendly_name, snapshot=snapshot),
    )
    make_dir_path(os.path.dirname(out_path))
    if snapshot:
        backup_snapshot(pool, object_name, out_path)
        if scrub:
            image = get_image_for_snapshot(pool, object_name)
            scrub_snapshot(pool, image, object_name, out_path)
    else:
        backup_image(pool, object_name, out_path)
        if scrub:
            snapshot = XAttrs(out_path)[ATTR_SNAPSHOT]
            scrub_snapshot(pool, object_name, snapshot, out_path)


@rbd.command()
@click.argument("path")
@click.argument("pool")
@click.argument("image")
def restore(path, pool, image):
    """restore backup to a new RBD image

    Arguments are PATH to image or snapshot file and the destination POOL and
    IMAGE names.
    """
    restore_image(path, pool, image)


@rbd.command()
@click.argument("pool")
@click.argument("friendly_name")
@pass_config
def drop(config, pool, friendly_name):
    """remove RBD snapshots for image backup"""
    settings = config["settings"]
    out_path = os.path.join(settings["root"], get_relroot(pool, friendly_name))
    drop_image_snapshots(pool, out_path)


class ImageUnit(Unit):
    def __init__(self, settings, pool, friendly_name):
        Unit.__init__(self)
        self.root = get_relroot(pool, friendly_name)
        self.backup_args = ["rbd", "backup", pool, friendly_name]
        if random_do_work(settings, "rbd-scrub-probability", 0.0166):
            self.backup_args.append("-c")


class SnapshotUnit(ImageUnit):
    def __init__(self, settings, pool, friendly_name):
        ImageUnit.__init__(self, settings, pool, friendly_name)
        self.root = get_relroot(pool, friendly_name, snapshot=True)
        self.backup_args.append("-s")


class RBDSource(Source):
    LABEL = "rbd"

    def get_units(self):
        ret = []
        for pool, info in sorted(self._manifest.items()):
            for friendly_name in sorted(info.get("images", {})):
                ret.append(ImageUnit(self._settings, pool, friendly_name))
            for friendly_name in sorted(info.get("snapshots", {})):
                ret.append(SnapshotUnit(self._settings, pool, friendly_name))
        return ret
