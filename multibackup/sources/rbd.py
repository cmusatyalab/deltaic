import json
import os
import subprocess
import sys
import uuid

from ..command import make_subcommand_group

BLOCKSIZE = 256 << 10

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


def _sync_image_to_file(pool, image, path):
    cmd = ['rbd', 'export', '--no-progress', '-p', pool, image, '-']
    print ' '.join(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    # Update in place to avoid LVM snapshot COW
    off = 0
    zero = '\0' * BLOCKSIZE
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
    with os.fdopen(fd, 'r+b') as fh:
        while True:
            ibuf = proc.stdout.read(BLOCKSIZE)
            if not ibuf:
                break
            count = len(ibuf)
            obuf = fh.read(count)
            if not obuf and ibuf == zero[:count]:
                # Output file at EOF; input block all zeros => sparsify
                fh.seek(off + count)
            elif ibuf != obuf:
                # Write new data
                fh.seek(off)
                fh.write(ibuf)
            off += count
        fh.truncate(off)
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

    temp_image = make_name('image')
    rbd_exec(pool, 'clone', '-i', image, '--snap', snapshot,
            '--dest-pool', pool, '--dest', temp_image)
    try:
        _sync_image_to_file(pool, temp_image, path)
    finally:
        rbd_exec(pool, 'rm', '--no-progress', temp_image)


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


def cmd_rbdbackup(config, args):
    if args.snapshot:
        store_snapshot(args.pool, args.object_name, args.out_path)
    else:
        store_image(args.pool, args.object_name, args.out_path)


def _setup():
    group = make_subcommand_group('rbd',
            help='RBD image/snapshot support')

    parser = group.add_parser('backup',
            help='back up RBD image or snapshot')
    parser.set_defaults(func=cmd_rbdbackup)
    parser.add_argument('pool',
            help='pool name')
    parser.add_argument('object_name', metavar='image-or-snapshot',
            help='image or snapshot name')
    parser.add_argument('out_path', metavar='out-path',
            help='path to output file')
    parser.add_argument('-s', '--snapshot', action='store_true',
            help='requested object is a snapshot')

_setup()
