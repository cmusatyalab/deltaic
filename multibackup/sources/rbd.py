import json
import os
import struct
import subprocess
import sys
import uuid
import xattr

from ..command import make_subcommand_group
from ..platform import punch
from ..source import Task, Source

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


def try_unlink(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def attr_get(attrs, attr):
    try:
        return attrs[attr]
    except KeyError:
        return None


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


def unpack_diff_to_file(ifh, path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
    total_size = 0
    total_changed = 0
    with os.fdopen(fd, 'r+b') as ofh:
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
    print '%d bytes written, %d total' % (total_changed, total_size)


def fetch_snapshot(pool, image, snapshot, path):
    if os.path.exists(path):
        os.unlink(path)
    proc = export_diff(pool, image, snapshot)
    try:
        unpack_diff_to_file(proc.stdout, path)
        if proc.wait():
            raise IOError('Export returned %d' % proc.returncode)
        attrs = xattr.xattr(path)
        attrs[ATTR_SNAPSHOT] = snapshot
    except Exception:
        os.unlink(path)
        raise


def fetch_image(pool, image, path):
    snapshot = create_snapshot(pool, image)
    try:
        fetch_snapshot(pool, image, snapshot, path)
    except Exception:
        delete_snapshot(pool, image, snapshot)
        raise


def make_patch(pool, image, path):
    attrs = xattr.xattr(path)
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
        attrs[ATTR_PENDING_SNAPSHOT] = new_snapshot
    except Exception:
        try_unlink(pending_path)
        delete_snapshot(pool, image, new_snapshot)
        raise


def apply_patch_and_rebase(pool, image, path):
    pending_path = path + PENDING_EXT
    attrs = xattr.xattr(path)
    old_snapshot = attrs[ATTR_SNAPSHOT]
    new_snapshot = attr_get(attrs, ATTR_PENDING_SNAPSHOT)
    if new_snapshot is None:
        # Nothing to do
        try_unlink(pending_path)
        return
    with open(pending_path, 'rb') as fh:
        unpack_diff_to_file(fh, path)
    delete_snapshot(pool, image, old_snapshot)
    attrs[ATTR_SNAPSHOT] = new_snapshot
    del attrs[ATTR_PENDING_SNAPSHOT]
    os.unlink(pending_path)


def backup_image(pool, image, path):
    attrs = xattr.xattr(path)
    old_snapshot = attr_get(attrs, ATTR_SNAPSHOT)
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
    attrs = xattr.xattr(path)
    if str(snapid) == attr_get(attrs, ATTR_SNAPID):
        return
    # Okay, snapshot has changed.  Delete the current backup and start over.
    # (Inefficient, but we assume this doesn't happen very often.)
    fetch_snapshot(pool, image, snapshot, path)
    attrs[ATTR_SNAPID] = str(snapid)


def drop_image_snapshots(pool, path):
    if not os.path.exists(path):
        return
    attrs = xattr.xattr(path)
    for attr in ATTR_SNAPSHOT, ATTR_PENDING_SNAPSHOT:
        snapshot = attr_get(attrs, attr)
        if snapshot:
            try:
                image = get_image_for_snapshot(pool, snapshot)
                delete_snapshot(pool, image, snapshot)
            except (KeyError, subprocess.CalledProcessError):
                pass
            del attrs[attr]
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
    basedir = os.path.dirname(out_path)
    if not os.path.exists(basedir):
        try:
            os.makedirs(basedir)
        except OSError:
            # Lost the race with another process?
            pass
    if args.snapshot:
        backup_snapshot(args.pool, object_name, out_path)
    else:
        backup_image(args.pool, object_name, out_path)


def cmd_rbd_drop(config, args):
    settings = config['settings']
    out_path = os.path.join(settings['root'],
            get_relroot(args.pool, args.friendly_name))
    drop_image_snapshots(args.pool, out_path)


def _setup():
    group = make_subcommand_group('rbd',
            help='RBD image/snapshot support')

    parser = group.add_parser('backup',
            help='back up RBD image or snapshot')
    parser.set_defaults(func=cmd_rbd_backup)
    parser.add_argument('pool',
            help='pool name')
    parser.add_argument('friendly_name', metavar='config-name',
            help='config name for image or snapshot')
    parser.add_argument('-s', '--snapshot', action='store_true',
            help='requested object is a snapshot')

    parser = group.add_parser('drop',
            help='remove RBD snapshots for image backup')
    parser.set_defaults(func=cmd_rbd_drop)
    parser.add_argument('pool',
            help='pool name')
    parser.add_argument('friendly_name', metavar='config-name',
            help='config name for image')

_setup()


class ImageTask(Task):
    def __init__(self, settings, pool, friendly_name):
        Task.__init__(self, settings)
        self.root = get_relroot(pool, friendly_name)
        self.args = ['rbd', 'backup', pool, friendly_name]


class SnapshotTask(ImageTask):
    def __init__(self, settings, pool, friendly_name):
        ImageTask.__init__(self, settings, pool, friendly_name)
        self.root = get_relroot(pool, friendly_name, snapshot=True)
        self.args.append('-s')


class RBDSource(Source):
    LABEL = 'rbd'

    def __init__(self, config):
        Source.__init__(self, config)
        for pool, info in self._manifest.items():
            for friendly_name in info.get('images', {}):
                self._queue.put(ImageTask(self._settings, pool,
                        friendly_name))
            for friendly_name in info.get('snapshots', {}):
                self._queue.put(SnapshotTask(self._settings, pool,
                        friendly_name))
