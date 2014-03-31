import json
import os
import struct
import subprocess
import sys
import uuid

from ..command import make_subcommand_group
from ..platform import punch
from ..source import Task, Source

BLOCKSIZE = 256 << 10
DIFF_MAGIC = 'rbd diff v1\n'

def rbd_exec(pool, cmd, *args):
    cmdline = ['rbd', cmd] + list(args) + ['-p', pool]
    print ' '.join(cmdline)
    subprocess.check_call(cmdline)


def rbd_pool_info(pool):
    proc = subprocess.Popen(['rbd', 'ls', '-l', '-p', pool, '--format=json'],
            stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    if proc.returncode:
        raise Exception('rbd ls -l returned %d' % proc.returncode)
    return json.loads(out)


def make_name(type):
    return 'backup-%s-%s' % (type, uuid.uuid1())


def read_items(fh, fmt):
    size = struct.calcsize(fmt)
    buf = fh.read(size)
    items = struct.unpack(fmt, buf)
    if len(items) == 1:
        return items[0]
    else:
        return items


def _unpack_diff_to_file(pool, image, snapshot, path):
    cmd = ['rbd', 'export-diff', '--no-progress', '-p', pool, image,
            '--snap', snapshot, '-']
    print ' '.join(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    ifh = proc.stdout
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
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
                size = read_items(ifh, '<Q')
                ofh.truncate(size)
            elif type == 'w':
                # Data
                offset, length = read_items(ifh, '<QQ')
                ofh.seek(offset)
                while length > 0:
                    buf = ifh.read(min(length, BLOCKSIZE))
                    ofh.write(buf)
                    length -= len(buf)
            elif type == 'z':
                # Zero data
                offset, length = read_items(ifh, '<QQ')
                punch(ofh, offset, length)
            elif type == 'e':
                if ifh.read(1) != '':
                    raise IOError("Expected EOF, didn't find it")
                break
            else:
                raise ValueError('Unknown record type: %s' % type)

    if proc.wait():
        raise Exception('Export returned %d' % proc.returncode)


def _store_snapshot(pool, image, snapshot, path):
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        try:
            os.makedirs(basedir)
        except OSError:
            # Lost the race with another process?
            pass

    _unpack_diff_to_file(pool, image, snapshot, path)


def store_snapshot(pool, snapshot, path):
    info = rbd_pool_info(pool)
    for cur in info:
        if cur.get('snapshot') == snapshot:
            image = cur['image']
            break
    else:
        raise Exception("Couldn't locate snapshot %s" % snapshot)
    return _store_snapshot(pool, image, snapshot, path)


def store_image(pool, image, path):
    temp_snapshot = make_name('snapshot')
    rbd_exec(pool, 'snap', 'create', '-i', image, '--snap', temp_snapshot)
    try:
        rbd_exec(pool, 'snap', 'protect', '-i', image, '--snap',
                temp_snapshot)
        try:
            _store_snapshot(pool, image, temp_snapshot, path)
        finally:
            rbd_exec(pool, 'snap', 'unprotect', '-i', image, '--snap',
                    temp_snapshot)
    finally:
        rbd_exec(pool, 'snap', 'rm', '-i', image, '--snap', temp_snapshot)


def cmd_rbd_backup(config, args):
    settings = config['settings']
    object_group = 'snapshots' if args.snapshot else 'images'
    object_name = config['rbd'][args.pool][object_group][args.friendly_name]
    out_path = os.path.join(settings['root'], 'rbd', args.pool, object_group,
            args.friendly_name)
    if args.snapshot:
        store_snapshot(args.pool, object_name, out_path)
    else:
        store_image(args.pool, object_name, out_path)


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

_setup()


class ImageTask(Task):
    def __init__(self, pool, friendly_name):
        Task.__init__(self)
        self.args = ['rbd', 'backup', pool, friendly_name]


class SnapshotTask(ImageTask):
    def __init__(self, pool, friendly_name):
        ImageTask.__init__(self, pool, friendly_name)
        self.args.append('-s')


class RBDSource(Source):
    LABEL = 'rbd'

    def __init__(self, config):
        Source.__init__(self, config)
        for pool, info in self._manifest.items():
            for friendly_name in info.get('images', {}):
                self._queue.put(ImageTask(pool, friendly_name))
            for friendly_name in info.get('snapshots', {}):
                self._queue.put(SnapshotTask(pool, friendly_name))
