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

import json
import os
import struct
import subprocess
import sys
import uuid

from ..command import make_subcommand_group
from ..platform import punch
from ..util import XAttrs, make_dir_path, random_do_work
from . import Target, Source

BLOCKSIZE = 256 << 10
DIFF_MAGIC = 'rbd diff v1\n'
PENDING_EXT = '.pending'
ATTR_SNAPSHOT = 'user.rbd.snapshot'
ATTR_PENDING_SNAPSHOT = 'user.rbd.pending-snapshot'
ATTR_SNAPID = 'user.rbd.snapid'

def rbd_exec(pool, cmd, *args):
    cmdline = ['rbd', cmd] + list(args) + ['-p', pool]
    print ' '.join(cmdline)
    subprocess.check_call(cmdline)


def rbd_query(pool, *args):
    cmd = ['rbd'] + list(args) + ['-p', pool, '--format=json']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    if proc.returncode:
        raise IOError('rbd query returned %d' % proc.returncode)
    return json.loads(out)


def rbd_pool_info(pool):
    return rbd_query(pool, 'ls', '-l')


def rbd_list_snapshots(pool, image):
    return rbd_query(pool, 'snap', 'ls', '-l', image)


def get_image_for_snapshot(pool, snapshot):
    for cur in rbd_pool_info(pool):
        if cur.get('snapshot') == snapshot:
            return cur['image']
    raise KeyError("Couldn't locate snapshot %s" % snapshot)


def get_snapid_for_snapshot(pool, image, snapshot):
    for cur in rbd_list_snapshots(pool, image):
        if cur['name'] == snapshot:
            return cur['id']
    raise KeyError("Couldn't locate snapshot %s" % snapshot)


def create_snapshot(pool, image):
    snapshot = 'backup-%s' % uuid.uuid1()
    rbd_exec(pool, 'snap', 'create', '-i', image, '--snap', snapshot)
    return snapshot


def delete_snapshot(pool, image, snapshot):
    rbd_exec(pool, 'snap', 'rm', '-i', image, '--snap', snapshot)


def create_or_open(path):
    # Open a file R/W, creating it if necessary but not truncating it.
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
    return os.fdopen(fd, 'r+b')


def try_unlink(path):
    try:
        os.unlink(path)
    except OSError:
        pass


class ComparingFile(object):
    """A writable file-like object.  It doesn't actually write anything, but
    instead compares its input to the file data at the specified path.
    If we find a mismatch, we could fix it in-place in the original file,
    but that would allow bugs in this class to produce silent data
    corruption some weeks after an image or snapshot is first backed up.
    Instead, we just complain by raising ValueError."""

    def __init__(self, path):
        self._fh = open(path, 'rb')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    def write(self, buf):
        start = 0
        while start < len(buf):
            disk_buf = self._fh.read(min(BLOCKSIZE, len(buf) - start))
            count = len(disk_buf)
            if disk_buf == '':
                raise ValueError("Unexpected EOF at %d" % self._fh.tell())
            if disk_buf != buf[start:start + count]:
                raise ValueError("Data mismatch at %d" %
                        (self._fh.tell() - count))
            start += count

    def seek(self, offset, whence=0):
        self._fh.seek(offset, whence)

    def tell(self):
        return self._fh.tell()
        
    def truncate(self, len):
        old_off = self._fh.tell()
        try:
            self._fh.seek(0, 2)
            if len != self._fh.tell():
                raise ValueError('Expected file length %d, found %d' %
                        (len, self._fh.tell()))
        finally:
            self._fh.seek(old_off)

    def punch(self, offset, length):
        # deltaic.platform.punch will call this.
        old_off = self._fh.tell()
        try:
            self._fh.seek(offset)
            buf = '\0' * BLOCKSIZE
            while length > 0:
                count = min(BLOCKSIZE, length)
                self.write(buf[:count])
                length -= count
        finally:
            self._fh.seek(old_off)

    def close(self):
        self._fh.close()


def export_diff(pool, image, snapshot, basis=None, fh=subprocess.PIPE):
    cmd = ['rbd', 'export-diff', '--no-progress', '-p', pool, image,
            '--snap', snapshot, '-']
    if basis is not None:
        cmd.extend(['--from-snap', basis])
    print ' '.join(cmd)
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
        raise IOError('Missing diff magic string')
    # Read each record
    while True:
        type = read_items(ifh, 'c')
        if type in ('f', 't'):
            # Source/dest snapshot name => ignore
            size = read_items(ifh, '<I')
            ifh.read(size)
        elif type == 's':
            # Image size
            total_size = read_items(ifh, '<Q')
            ofh.truncate(total_size)
        elif type == 'w':
            # Data
            offset, length = read_items(ifh, '<QQ')
            total_changed += length
            ofh.seek(offset)
            while length > 0:
                buf = ifh.read(min(length, BLOCKSIZE))
                ofh.write(buf)
                length -= len(buf)
        elif type == 'z':
            # Zero data
            offset, length = read_items(ifh, '<QQ')
            total_changed += length
            punch(ofh, offset, length)
        elif type == 'e':
            if ifh.read(1) != '':
                raise IOError("Expected EOF, didn't find it")
            break
        else:
            raise ValueError('Unknown record type: %s' % type)
    if verbose:
        print '%d bytes written, %d total' % (total_changed, total_size)


def fetch_snapshot(pool, image, snapshot, path):
    if os.path.exists(path):
        os.unlink(path)
    proc = export_diff(pool, image, snapshot)
    try:
        with create_or_open(path) as ofh:
            unpack_diff(proc.stdout, ofh)
        if proc.wait():
            raise IOError('Export returned %d' % proc.returncode)
        XAttrs(path).update(ATTR_SNAPSHOT, snapshot)
    except Exception:
        os.unlink(path)
        raise


def scrub_snapshot(pool, image, snapshot, path):
    proc = export_diff(pool, image, snapshot)
    with ComparingFile(path) as ofh:
        unpack_diff(proc.stdout, ofh, verbose=False)
    if proc.wait():
        raise IOError('Export returned %d' % proc.returncode)


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
        raise IOError('Pending snapshot already exists')
    # Delete any leftover file from partial previous run
    try_unlink(pending_path)

    new_snapshot = create_snapshot(pool, image)
    try:
        with open(pending_path, 'wb') as fh:
            proc = export_diff(pool, image, new_snapshot, basis=old_snapshot,
                    fh=fh)
        if proc.wait():
            raise IOError('Export returned %d' % proc.returncode)
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
    with open(pending_path, 'rb') as ifh:
        with create_or_open(path) as ofh:
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
                try:
                    delete_snapshot(pool, old_image, old_snapshot)
                except subprocess.CalledProcessError:
                    pass
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
        raise ValueError('Backup image has partially-applied diff')
    rbd_exec(pool, 'import', path, image)


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
    return os.path.join('rbd', pool, 'snapshots' if snapshot else 'images',
            friendly_name)


def cmd_rbd_backup(config, args):
    settings = config['settings']
    object_name = config['rbd'][args.pool]['snapshots' if args.snapshot
            else 'images'][args.friendly_name]
    out_path = os.path.join(settings['root'],
            get_relroot(args.pool, args.friendly_name, snapshot=args.snapshot))
    basedir = make_dir_path(os.path.dirname(out_path))
    if args.snapshot:
        backup_snapshot(args.pool, object_name, out_path)
        if args.scrub:
            image = get_image_for_snapshot(args.pool, object_name)
            scrub_snapshot(args.pool, image, object_name, out_path)
    else:
        backup_image(args.pool, object_name, out_path)
        if args.scrub:
            snapshot = XAttrs(out_path)[ATTR_SNAPSHOT]
            scrub_snapshot(args.pool, object_name, snapshot, out_path)


def cmd_rbd_restore(config, args):
    restore_image(args.path, args.pool, args.image)


def cmd_rbd_drop(config, args):
    settings = config['settings']
    out_path = os.path.join(settings['root'],
            get_relroot(args.pool, args.friendly_name))
    drop_image_snapshots(args.pool, out_path)


def _setup():
    group = make_subcommand_group('rbd',
            help='low-level RBD image/snapshot support')

    parser = group.add_parser('backup',
            help='back up RBD image or snapshot')
    parser.set_defaults(func=cmd_rbd_backup)
    parser.add_argument('pool',
            help='pool name')
    parser.add_argument('friendly_name', metavar='config-name',
            help='config name for image or snapshot')
    parser.add_argument('-c', '--scrub', action='store_true',
            help='check backup data against original')
    parser.add_argument('-s', '--snapshot', action='store_true',
            help='requested object is a snapshot')

    parser = group.add_parser('restore',
            help='restore backup to a new RBD image')
    parser.set_defaults(func=cmd_rbd_restore)
    parser.add_argument('path',
            help='path to image or snapshot file')
    parser.add_argument('pool',
            help='destination pool name')
    parser.add_argument('image',
            help='destination image name')

    parser = group.add_parser('drop',
            help='remove RBD snapshots for image backup')
    parser.set_defaults(func=cmd_rbd_drop)
    parser.add_argument('pool',
            help='pool name')
    parser.add_argument('friendly_name', metavar='config-name',
            help='config name for image')

_setup()


class ImageTarget(Target):
    def __init__(self, settings, pool, friendly_name):
        Target.__init__(self, settings)
        self.root = get_relroot(pool, friendly_name)
        self.backup_args = ['rbd', 'backup', pool, friendly_name]
        if random_do_work(settings, 'rbd-scrub-probability', 0.0166):
            self.backup_args.append('-c')


class SnapshotTarget(ImageTarget):
    def __init__(self, settings, pool, friendly_name):
        ImageTarget.__init__(self, settings, pool, friendly_name)
        self.root = get_relroot(pool, friendly_name, snapshot=True)
        self.backup_args.append('-s')


class RBDSource(Source):
    LABEL = 'rbd'

    def get_targets(self):
        ret = []
        for pool, info in sorted(self._manifest.items()):
            for friendly_name in sorted(info.get('images', {})):
                ret.append(ImageTarget(self._settings, pool, friendly_name))
            for friendly_name in sorted(info.get('snapshots', {})):
                ret.append(SnapshotTarget(self._settings, pool, friendly_name))
        return ret
