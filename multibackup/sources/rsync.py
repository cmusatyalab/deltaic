import os
import re
import subprocess
import sys

from ..command import make_subcommand_group
from ..source import Task, Source

def remote_command(host, command):
    args = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no',
            'root@%s' % host, command]
    print ' '.join(args)
    subprocess.check_call(args)


def run_rsync(cmd):
    print ' '.join(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    # Filter out spurious log output from
    # https://bugzilla.samba.org/show_bug.cgi?id=10496
    spurious = re.compile(r'[.h][dfL]\.{8}x ')
    for line in proc.stdout:
        if not spurious.match(line):
            print line.strip()

    return proc.wait()


def sync_host(host, root_dir, mounts, exclude=(), rsync=None):
    if rsync is None:
        rsync = 'rsync'
    args = [rsync, '-aHRxi', '--acls', '--xattrs', '--fake-super', '--delete',
            '--delete-excluded', '--numeric-ids', '--stats', '--partial',
            '--rsh=ssh -o BatchMode=yes -o StrictHostKeyChecking=no']
    args.extend(['--exclude=' + r for r in exclude])
    args.extend(['root@%s:%s' % (host, mount.rstrip('/') or '/')
            for mount in mounts])
    args.append(root_dir.rstrip('/'))

    ret = run_rsync(args)
    if ret == 2:
        # Feature not supported by negotiated protocol version;
        # start dropping newer features
        args.remove('--acls')
        args.remove('--xattrs')
        ret = run_rsync(args)

    if ret not in (0, 24):
        raise IOError('rsync failed with code %d' % ret)


def cmd_rsync_backup(config, args):
    settings = config['settings']
    info = config['rsync'][args.host]
    root_dir = os.path.join(settings['root'], 'rsync', args.host.split('.')[0])
    exclude = settings.get('rsync-exclude', [])
    exclude.extend(info.get('exclude', []))
    rsync = settings.get('rsync-local-binary')

    if 'pre' in info:
        remote_command(args.host, info['pre'])
    sync_host(args.host, root_dir, info['mounts'], exclude, rsync=rsync)
    if 'post' in info:
        remote_command(args.host, info['post'])


def _setup():
    group = make_subcommand_group('rsync',
            help='host filesystem support via rsync')

    parser = group.add_parser('backup',
            help='back up host filesystem')
    parser.set_defaults(func=cmd_rsync_backup)
    parser.add_argument('host',
            help='host to back up')

_setup()


class RsyncTask(Task):
    def __init__(self, hostname):
        Task.__init__(self)
        self.args = ['rsync', 'backup', hostname]


class RsyncSource(Source):
    LABEL = 'rsync'

    def __init__(self, config):
        Source.__init__(self, config)
        for hostname in sorted(self._manifest):
            self._queue.put(RsyncTask(hostname))
