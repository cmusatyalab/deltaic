import os
import subprocess
import sys

from ..command import make_subcommand_group

def remote_command(host, command):
    args = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no',
            'root@%s' % host, command]
    print ' '.join(args)
    subprocess.check_call(args)


def sync_host(host, root_dir, mounts, exclude=(), verbose=False):
    args = ['rsync', '-aHAXRx', '--fake-super', '--delete',
            '--delete-excluded', '--numeric-ids', '--stats', '--partial',
            '--rsh=ssh -o BatchMode=yes -o StrictHostKeyChecking=no']
    if verbose:
        args.append('-i')
    args.extend(['--exclude=' + r for r in exclude])
    args.extend(['root@%s:%s' % (host, mount.rstrip('/') or '/')
            for mount in mounts])
    args.append(root_dir.rstrip('/'))

    print ' '.join(args)
    ret = subprocess.call(args)
    if ret not in (0, 24):
        raise Exception('rsync failed with code %d' % ret)


def cmd_rsyncbackup(config, args):
    if args.pre:
        remote_command(args.host, args.pre)
    sync_host(args.host, args.root_dir, args.mounts, args.exclude,
            verbose=args.verbose)
    if args.post:
        remote_command(args.host, args.post)


def _setup():
    group = make_subcommand_group('rsync',
            help='host filesystem support via rsync')

    parser = group.add_parser('backup',
            help='back up host filesystem')
    parser.set_defaults(func=cmd_rsyncbackup)
    parser.add_argument('host',
            help='host to back up')
    parser.add_argument('root_dir', metavar='out-dir',
            help='path to output directory')
    parser.add_argument('mounts', metavar='mount', nargs='+',
            help='filesystem mount point to copy')
    parser.add_argument('--pre', metavar='COMMAND',
            help='command to run on remote side before backup')
    parser.add_argument('--post', metavar='COMMAND',
            help='command to run on remote side after backup')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show progress')
    parser.add_argument('-x', '--exclude', action='append', default=[],
            help='rsync exclude pattern (can be repeated)')

_setup()
