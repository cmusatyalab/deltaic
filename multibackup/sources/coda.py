import os
import re
import shutil
import stat
import subprocess
import tarfile
import xattr

from ..command import make_subcommand_group
from ..platform import lutime
from ..source import Task, Source
from ..util import BloomSet, update_file, random_do_work

ATTR_INCREMENTAL = 'user.coda.incremental-ok'
ATTR_STAT = 'user.rsync.%stat'

class DumpError(Exception):
    pass


def volutil_cmd(host, subcommand, args=(), volutil=None):
    if volutil is None:
        volutil = 'volutil'
    print '>', volutil, subcommand, ' '.join(args)
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no',
            'root@%s' % host, volutil, subcommand] + list(args)


def get_err_stream(verbose):
    if verbose:
        return None
    else:
        return open('/dev/null', 'r+')


def get_volume_ids(host, volume, verbose=False, volutil=None):
    proc = subprocess.Popen(volutil_cmd(host, 'info', [volume],
            volutil=volutil), stdout=subprocess.PIPE,
            stderr=get_err_stream(verbose))
    info, _ = proc.communicate()
    if proc.returncode:
        raise IOError("Couldn't get volume info for %s" % volume)

    match = re.search('^id = ([0-9a-f]+)', info, re.MULTILINE)
    if match is None:
        raise ValueError("Couldn't find volume ID for %s" % volume)
    volume_id = match.group(1)

    match = re.search(', backupId = ([0-9a-f]+)', info)
    if match is None:
        raise ValueError("Couldn't find backup ID for %s" % volume)
    backup_id = match.group(1)

    return volume_id, backup_id


def refresh_backup_volume(host, volume, verbose=False, volutil=None):
    volume_id, _ = get_volume_ids(host, volume, verbose, volutil=volutil)
    subprocess.check_call(volutil_cmd(host, 'lock', [volume_id],
            volutil=volutil), stdout=get_err_stream(verbose),
            stderr=get_err_stream(verbose))
    subprocess.check_call(volutil_cmd(host, 'backup', [volume_id],
            volutil=volutil), stdout=get_err_stream(verbose),
            stderr=get_err_stream(verbose))


def build_path(root_dir, path):
    normalized = os.path.normpath(path)
    if normalized.startswith('../'):
        raise ValueError('Attempted directory traversal: %s' % path)
    # Call normpath again for the case where normalized == '.'
    return os.path.normpath(os.path.join(root_dir, normalized))


def update_xattr(attrs, key, value):
    try:
        old_value = attrs[key]
    except KeyError:
        old_value = None
    if old_value != value:
        attrs[key] = value


class TarMemberFile(object):
    # Wrapper around tarfile.ExFileObject that raises DumpError when
    # the tar stream is truncated mid-file.  This prevents us from
    # replacing a complete backup file with a truncated one.

    def __init__(self, tar, entry):
        self._file = tar.extractfile(entry)
        self._remaining = entry.size

    def read(self, size=None):
        buf = self._file.read(size)
        self._remaining -= len(buf)
        if self._remaining > 0 and (buf == '' or size is None):
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
            raise ValueError('Unexpected file type %d' % entry.type)

        # Check for existing file
        path = build_path(root_dir, entry.name)
        try:
            st = os.lstat(path)
        except OSError:
            st = None

        # If entry has changed types, remove the old object
        if st:
            if stat.S_IFMT(st.st_mode) != entry_stat_type:
                if stat.S_ISDIR(st.st_mode):
                    shutil.rmtree(path)
                else:
                    os.unlink(path)
                st = None

        # Create parent directory if not present.  Parents are not
        # necessarily dumped before children.
        dirpath = os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        # Create new object
        if entry.isdir():
            if not os.path.exists(path):
                print 'd', path
                os.mkdir(path)
            # Go back and set mtime after directory has been populated
            directories.append(entry)
        elif entry.isfile():
            # update_file() will break hard links if it modifies the file. 
            # This is what we want because links may have also been broken
            # at the source.  codadump2tar always dumps hard links, so we
            # will rebuild any links that should still exist.
            if update_file(path, TarMemberFile(tar, entry)):
                print 'f', path
        elif entry.issym():
            if st is None or os.readlink(path) != entry.linkname:
                print 's', path
                if st is not None:
                    os.unlink(path)
                os.symlink(entry.linkname, path)
        elif entry.islnk():
            target_path = build_path(root_dir, entry.linkname)
            target_st = os.lstat(target_path)
            if (st is None or st.st_dev != target_st.st_dev or
                    st.st_ino != target_st.st_ino):
                print 'l', path
                if st is not None:
                    os.unlink(path)
                os.link(target_path, path)

        # Update metadata
        attrs = xattr.xattr(path, xattr.XATTR_NOFOLLOW)
        # owner and mode.  Hardlinks were updated with the primary, and we
        # can't set xattrs on symlinks.
        if entry.isfile() or entry.isdir():
            # rsync --fake-super compatible:
            # octal_mode_with_type major,minor uid:gid
            update_xattr(attrs, ATTR_STAT,
                    '%o 0,0 %d:%d' % (entry_stat_type | entry.mode,
                    entry.uid, entry.gid))
        # mtime.  Directories will be updated later, and hardlinks were
        # updated with the primary.
        if entry.isfile() or entry.issym():
            if os.lstat(path).st_mtime != entry.mtime:
                lutime(path, entry.mtime)

        # Protect from garbage collection
        valid_paths.add(path)

    # Deferred update of directory mtimes
    for entry in directories:
        path = build_path(root_dir, entry.name)
        if os.stat(path).st_mtime != entry.mtime:
            os.utime(path, (entry.mtime, entry.mtime))

    return valid_paths


def update_dir(host, backup_id, root_dir, incremental=False, volutil=None,
        codadump2tar=None):
    if codadump2tar is None:
        codadump2tar = 'codadump2tar'
    args = ['-i', '-1'] if incremental else []
    args.extend([backup_id, '|', codadump2tar, '-rn', '.'])
    with open('/dev/null', 'r+') as null:
        proc = subprocess.Popen(volutil_cmd(host, 'dump', args,
                volutil=volutil), stdout=subprocess.PIPE, stderr=null)

    tar = tarfile.open(fileobj=proc.stdout, mode='r|')
    valid_paths = update_dir_from_tar(tar, root_dir)

    if proc.wait():
        raise DumpError('Coda dump returned %d' % proc.returncode)
    return valid_paths


def remove_deleted(root_dir, valid_paths):
    def handle_err(err):
        raise err
    for dirpath, _, filenames in os.walk(root_dir, topdown=False,
            onerror=handle_err):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if filepath not in valid_paths:
                print '-', filepath
                os.unlink(filepath)
        if dirpath not in valid_paths:
            try:
                os.rmdir(dirpath)
                print '-', dirpath
            except OSError:
                # Directory not empty
                pass


def sync_backup_volume(host, volume, root_dir, incremental=False,
        verbose=False, volutil=None, codadump2tar=None):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    # Force a full backup unless the root_dir is marked incremental-ok.
    # This ensures that the first backup is a full.  Remove incremental-ok
    # attr when a full backup is requested, to ensure that full-backup
    # failures are reproducible in the next backup run.
    root_xattrs = xattr.xattr(root_dir, xattr.XATTR_NOFOLLOW)
    incremental_ok = ATTR_INCREMENTAL in root_xattrs
    if incremental_ok and not incremental:
        del root_xattrs[ATTR_INCREMENTAL]
    elif not incremental_ok:
        incremental = False

    _, backup_id = get_volume_ids(host, volume, verbose, volutil=volutil)

    # Retry dump a few times to paper over rpc2 timeouts
    for tries_remaining in range(4, -1, -1):
        try:
            valid_paths = update_dir(host, backup_id, root_dir,
                    incremental=incremental, volutil=volutil,
                    codadump2tar=codadump2tar)
            break
        except DumpError:
            if not tries_remaining:
                raise

    if not incremental:
        remove_deleted(root_dir, valid_paths)

    subprocess.check_call(volutil_cmd(host, 'ancient', [backup_id],
            volutil=volutil), stderr=get_err_stream(verbose))
    update_xattr(root_xattrs, ATTR_INCREMENTAL, '')


def get_relroot(hostname, volume):
    return os.path.join('coda', hostname.split('.')[0], volume)


def cmd_coda_backup(config, args):
    settings = config['settings']
    root_dir = os.path.join(settings['root'],
            get_relroot(args.host, args.volume))
    volutil = settings.get('coda-volutil-path', 'volutil')
    codadump2tar = settings.get('coda-codadump2tar-path', 'codadump2tar')
    if args.refresh:
        refresh_backup_volume(args.host, args.volume, verbose=args.verbose,
                volutil=volutil)
    sync_backup_volume(args.host, args.volume, root_dir,
            incremental=args.incremental, verbose=args.verbose,
            volutil=volutil, codadump2tar=codadump2tar)


def _setup():
    group = make_subcommand_group('coda',
            help='Coda support')

    parser = group.add_parser('backup',
            help='back up Coda volume')
    parser.set_defaults(func=cmd_coda_backup)
    parser.add_argument('host',
            help='Coda server hostname')
    parser.add_argument('volume',
            help='Coda volume name')
    parser.add_argument('-i', '--incremental', action='store_true',
            help='request incremental backup')
    parser.add_argument('-R', '--skip-refresh', dest='refresh', default=True,
            action='store_false',
            help='skip refreshing backup volume')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show volutil output')

_setup()


class CodaTask(Task):
    def __init__(self, settings, server, volume):
        Task.__init__(self, settings)
        self.root = get_relroot(server, volume)
        self.args = ['coda', 'backup', server, volume]
        if random_do_work(settings, 'coda-full-probability', 0.143):
            self.args.append('-i')


class CodaSource(Source):
    LABEL = 'coda'

    def __init__(self, config):
        Source.__init__(self, config)
        for group in self._manifest:
            for volume in group['volumes']:
                for server in group['servers']:
                    self._queue.put(CodaTask(self._settings, server, volume))
