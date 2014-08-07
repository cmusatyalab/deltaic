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

from __future__ import division
from hashlib import sha256
import os
import sys
from tempfile import TemporaryFile, NamedTemporaryFile, mkdtemp

from ..command import subparsers, make_subcommand_group
from ..sources import Source, Task
from ..storage import Snapshot, PhysicalSnapshot
from ..util import (Pipeline, XAttrs, make_dir_path, lockfile, write_atomic,
        humanize_size)

class Archiver(object):
    def __init__(self, profile_name, profile):
        self.profile_name = profile_name
        self.profile = profile

    @classmethod
    def get_archiver(cls, settings, profile_name):
        profile = settings['archivers'][profile_name]
        for subclass in cls.__subclasses__():
            if getattr(subclass, 'LABEL', None) == profile['archiver']:
                return subclass(profile_name, profile)
        raise ValueError('No such archiver')

    def list_sets(self):
        # Return dict: set_name -> dict of properties.
        # Properties:
        #   count:       number of archives in this set
        #   size:        size of the archives in this set, in bytes
        #   complete:    True/False
        #   protected:   True if this set should not be deleted for
        #                backend-specific reasons.
        raise NotImplementedError

    def complete_set(self, set_name):
        raise NotImplementedError

    def delete_set(self, set_name):
        raise NotImplementedError

    def list_set_archives(self, set_name):
        # Return dict: archive_name -> metadata
        raise NotImplementedError

    def upload_archive(self, set_name, archive_name, metadata, local_path):
        raise NotImplementedError

    def download_archive(self, set_name, archive_name, fh):
        # Return metadata
        raise NotImplementedError

    def resync(self):
        # Perform any necessary resynchronization of data and metadata.
        raise NotImplementedError

    def report_cost(self):
        # Print report on storage costs to stdout.
        raise NotImplementedError


class ArchiveInfo(object):
    ATTR_COMPRESSION = 'user.archive.compression'
    ATTR_ENCRYPTION = 'user.archive.encryption'
    ATTR_SHA256 = 'user.archive.sha256'

    def __init__(self, compression, encryption, sha256, size):
        self.compression = compression.encode('utf-8')
        self.encryption = encryption.encode('utf-8')
        self.sha256 = sha256.encode('utf-8')
        self.size = size

    @classmethod
    def from_file(cls, path):
        attrs = XAttrs(path)
        return cls(
            compression=attrs[cls.ATTR_COMPRESSION],
            encryption=attrs[cls.ATTR_ENCRYPTION],
            sha256=attrs[cls.ATTR_SHA256],
            size=os.stat(path).st_size,
        )

    def to_file(self, path):
        if os.stat(path).st_size != self.size:
            raise IOError('Size mismatch')
        attrs = XAttrs(path)
        attrs.update(self.ATTR_COMPRESSION, self.compression)
        attrs.update(self.ATTR_ENCRYPTION, self.encryption)
        attrs.update(self.ATTR_SHA256, self.sha256)


class ArchivePacker(object):
    BUFLEN = 4 << 20

    def __init__(self, settings):
        self._spool_dir = settings['archive-spool']
        self._tar = settings.get('archive-tar-path', 'tar')
        self._gpg = settings.get('archive-gpg-path', 'gpg2')
        self._gpg_recipients = settings.get('archive-gpg-recipients')
        self._gpg_signing_key = settings.get('archive-gpg-signing-key')

        self.compression = 'gzip'
        self.encryption = 'gpg' if self._gpg_recipients else 'none'

    @classmethod
    def _pipeline(cls, cmds, in_fh=None, out_fh=None):
        env = dict(os.environ)
        try:
            # Required for GPG passphrase prompting
            env['GPG_TTY'] = os.ttyname(sys.stdin.fileno())
        except OSError:
            # stdin is not a tty
            pass
        return Pipeline(cmds, in_fh=in_fh, out_fh=out_fh, env=env)

    def _gpg_cmd(self, args):
        return [self._gpg, '--batch', '--no-tty', '--no-options',
                '--personal-cipher-preferences', 'AES256,AES192,AES',
                '--personal-digest-preferences', 'SHA256,SHA1',
                '--personal-compress-preferences', 'none'] + args

    def pack(self, snapshot_name, snapshot_root, unit_name, out_fh):
        cmds = []
        cmds.append([self._tar, 'cz', '--force-local', '--format=gnu',
                '--sparse', '--acls', '--selinux', '--xattrs',
                '-V', '%s %s' % (snapshot_name, unit_name),
                '-C', snapshot_root, unit_name])
        if self.encryption == 'gpg':
            args = ['-seu', self._gpg_signing_key]
            for recipient in self._gpg_recipients:
                args.extend(['-r', recipient])
            cmds.append(self._gpg_cmd(args))

        pipe_r, pipe_w = os.pipe()
        with os.fdopen(pipe_r, 'r') as in_fh:
            with self._pipeline(cmds, out_fh=pipe_w):
                os.close(pipe_w)
                hash = sha256()
                while True:
                    buf = in_fh.read(self.BUFLEN)
                    if not buf:
                        break
                    hash.update(buf)
                    out_fh.write(buf)

        return ArchiveInfo(
            compression=self.compression,
            encryption=self.encryption,
            size=out_fh.tell(),
            sha256=hash.hexdigest(),
        )

    def unpack(self, in_file, info, out_root):
        if info.encryption != self.encryption:
            # If the archive server is compromised, don't allow it to bypass
            # signature checking by replacing the archive and changing
            # the encryption field to "none"
            raise ValueError("Archive encryption doesn't match " +
                    "local settings; check metadata integrity")

        fh = open(in_file, 'rb')
        try:
            if info.encryption == 'gpg':
                # We can't decrypt and untar in a single pipeline, because
                # GPG wouldn't verify the signature until after tar had
                # already received a lot of untrusted data.
                fh2 = TemporaryFile(dir=self._spool_dir, prefix='unpack-')
                try:
                    self._pipeline([self._gpg_cmd(['-d'])], in_fh=fh,
                            out_fh=fh2).close()
                    fh2.seek(0)
                finally:
                    fh.close()
                    fh = fh2

            elif info.encryption != 'none':
                raise ValueError('Unknown encryption method')

            self._pipeline([[self._tar, 'xz', '--force-local', '--acls',
                    '--selinux', '--xattrs', '-C', out_root]],
                    in_fh=fh).close()
        finally:
            fh.close()


class _SHA256File(object):
    def __init__(self, fh):
        self._hash = sha256()
        self._fh = fh

    def write(self, buf):
        self._hash.update(buf)
        self._fh.write(buf)

    @property
    def digest(self):
        return self._hash.hexdigest()


class _ArchiveTask(Task):
    def __init__(self, settings, archiver, snapshot, snapshot_dir, units):
        self._settings = settings
        self._archiver = archiver
        self._snapshot = snapshot
        self._snapshot_dir = snapshot_dir
        thread_count = archiver.profile.get('workers', 8)
        Task.__init__(self, thread_count, units)

    def _execute(self, unit):
        log_dir = make_dir_path(self._settings['root'], 'Logs',
                'Archive', self._archiver.profile_name, unit.root)
        args = ['archive', '-p', self._archiver.profile_name, 'unit',
                self._snapshot.name, self._snapshot_dir, unit.root]
        return self._run_subcommand(unit.root, args, log_dir)


class SnapshotArchiveSet(object):
    def __init__(self, archiver, snapshot, count=None, size=None,
            complete=None, protected=None):
        self.archiver = archiver
        self.snapshot = snapshot
        self.count = count
        self.size = size
        self.complete = complete
        self.protected = protected

    def __str__(self):
        return str(self.snapshot)

    @classmethod
    def list(cls, archiver):
        sets = []
        for set_name, metadata in sorted(archiver.list_sets().items()):
            sets.append(cls(archiver, Snapshot(set_name),
                    complete=metadata['complete'],
                    protected=metadata['protected'],
                    count=metadata['count'],
                    size=metadata['size']))
        return sets

    def get_archives(self):
        result = self.archiver.list_set_archives(self.snapshot.name)
        archives = {}
        for unit_name, metadata in result.items():
            archives[unit_name] = _Archive(self.archiver, self.snapshot,
                    unit_name, size=int(metadata['size']))
        return archives

    def get_archive(self, unit_name):
        return _Archive(self.archiver, self.snapshot, unit_name)

    def mark_complete(self):
        self.archiver.complete_set(self.snapshot.name)

    def delete(self):
        self.archiver.delete_set(self.snapshot.name)


class _Archive(object):
    def __init__(self, archiver, snapshot, unit_name, size=None):
        self.archiver = archiver
        self.snapshot = snapshot
        self.unit_name = unit_name
        self.size = size

    def store(self, in_path, info):
        metadata = {
            'compression': info.compression,
            'encryption': info.encryption,
            'sha256': info.sha256,
            'size': str(info.size),
        }
        self.archiver.upload_archive(self.snapshot.name, self.unit_name,
                metadata, in_path)

    def retrieve(self, out_dir):
        out_path = os.path.join(out_dir, '%s:%s' % (self.snapshot.name,
                self.unit_name.replace('/', '-')))
        if os.path.exists(out_path):
            raise ValueError('Output file already exists')
        with write_atomic(out_path, prefix='.retrieve-') as fh:
            hasher = _SHA256File(fh)
            metadata = self.archiver.download_archive(self.snapshot.name,
                    self.unit_name, hasher)
            if not metadata:
                raise ValueError('No such archive')
            if fh.tell() != int(metadata['size']):
                raise IOError('Size mismatch')
            if hasher.digest != metadata['sha256']:
                raise IOError('SHA-256 mismatch')
        return (out_path, ArchiveInfo(
            compression=metadata['compression'],
            encryption=metadata['encryption'],
            size=int(metadata['size']),
            sha256=metadata['sha256'],
        ))


def archive_unit(settings, archive, snapshot_root):
    spool_dir = settings['archive-spool']
    packer = ArchivePacker(settings)
    with NamedTemporaryFile(dir=spool_dir, prefix='archive-') as out_fh:
        info = packer.pack(archive.snapshot.name, snapshot_root,
                archive.unit_name, out_fh)
        out_fh.flush()
        archive.store(out_fh.name, info)


def archive_snapshot(config, archiver, snapshot):
    settings = config['settings']
    set = SnapshotArchiveSet(archiver, snapshot)
    archives = set.get_archives()

    spool_dir = settings['archive-spool']
    snapshot_dir = mkdtemp(dir=spool_dir, prefix='snapshot-')
    try:
        snapshot.mount(snapshot_dir)
        try:
            units = []
            sources = Source.get_sources()
            for source_label in sorted(sources):
                source = sources[source_label](config)
                for unit in source.get_units():
                    unit_path = os.path.join(snapshot_dir, unit.root)
                    # Skip units already archived or not yet backed up.
                    if (os.path.exists(unit_path) and
                            unit.root not in archives):
                        units.append(unit)
            if units:
                task = _ArchiveTask(settings, archiver, snapshot,
                        snapshot_dir, units)
                task.start()
                if not task.wait():
                    return False
            set.mark_complete()
            return True
        finally:
            snapshot.umount(snapshot_dir)
    finally:
        os.rmdir(snapshot_dir)


def prune(settings, archiver):
    keep_count = archiver.profile.get('keep-count', 1)
    sets = SnapshotArchiveSet.list(archiver)
    # Delete all incomplete sets, ignoring the most recent set
    delete = [s for s in sets[:-1] if not s.complete]
    # Delete all complete sets except the most recent keep_count
    delete += [s for s in sets if s.complete][:-keep_count]
    # ...but don't delete protected sets
    delete = [s for s in delete if not s.protected]
    for set in delete:
        print 'Pruning%s archive set %s' % (
                ' incomplete' if not set.complete else '', set)
        set.delete()


def cmd_run(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    if args.snapshot and args.resume:
        raise ValueError('Cannot specify snapshot with --resume')
    elif args.snapshot:
        for snapshot in PhysicalSnapshot.list():
            if snapshot.name == args.snapshot:
                break
        else:
            raise ValueError('No such snapshot')
        print 'Archiving selected snapshot', snapshot
    elif args.resume:
        set = SnapshotArchiveSet.list(archiver)[-1]
        snapshot = set.snapshot
        if set.complete:
            raise ValueError('%s already completely archived' % snapshot)
        print 'Resuming archive of snapshot', snapshot
    else:
        snapshot = PhysicalSnapshot.list()[-1]
        print 'Archiving snapshot', snapshot
    snapshot = snapshot.get_physical(settings)
    with lockfile(settings, 'archive'):
        if not archive_snapshot(config, archiver, snapshot):
            print >>sys.stderr, ('Archiving failed for some units.  ' +
                    'Not marking archive set complete.')
            print >>sys.stderr, 'Use "deltaic archive run -r" to resume.'
            return 1


def cmd_cost(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    archiver.report_cost()


def cmd_ls(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    for set in SnapshotArchiveSet.list(archiver):
        if args.set and args.set != set.snapshot.name:
            continue
        if args.sets:
            print '%s %5d %10s  %s %s' % (set.snapshot.name, set.count,
                    humanize_size(set.size),
                    '  complete' if set.complete else 'incomplete',
                    'protected' if set.protected else '')
        else:
            for _, archive in sorted(set.get_archives().items()):
                print '%s %10s %s' % (archive.snapshot.name,
                        humanize_size(archive.size), archive.unit_name)


def cmd_retrieve(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    set = SnapshotArchiveSet(archiver, Snapshot(args.snapshot))
    archive = set.get_archive(args.unit)
    out_path, info = archive.retrieve(args.destdir)
    info.to_file(out_path)


def cmd_unpack(config, args):
    settings = config['settings']
    packer = ArchivePacker(settings)
    info = ArchiveInfo.from_file(args.file)
    packer.unpack(args.file, info, args.destdir)


def cmd_prune(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    prune(settings, archiver)


def cmd_resync(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    archiver.resync()


def cmd_unit(config, args):
    settings = config['settings']
    archiver = Archiver.get_archiver(settings, args.profile)
    set = SnapshotArchiveSet(archiver, Snapshot(args.snapshot))
    archive = set.get_archive(args.unit)
    archive_unit(settings, archive, args.mountpoint)


def _setup():
    group = make_subcommand_group('archive',
            help='offsite archiving')
    group.parser.add_argument('-p', '--profile', default='default',
            help='archiver profile')

    parser = group.add_parser('run',
            help='create and upload an offsite archive for every unit')
    parser.set_defaults(func=cmd_run)
    parser.add_argument('snapshot', nargs='?',
            help='snapshot name')
    parser.add_argument('-r', '--resume', action='store_true',
            help='resume previous run')

    parser = group.add_parser('cost',
            help='calculate storage costs')
    parser.set_defaults(func=cmd_cost)

    parser = group.add_parser('ls',
            help='list existing offsite archives')
    parser.add_argument('-s', '--sets', action='store_true',
            help='list archive sets')
    parser.add_argument('set', nargs='?',
            help='archive set to examine')
    parser.set_defaults(func=cmd_ls)

    parser = group.add_parser('retrieve',
            help='download an offsite archive to the specified directory')
    parser.set_defaults(func=cmd_retrieve)
    parser.add_argument('snapshot',
            help='snapshot name')
    parser.add_argument('unit',
            help='unit name')
    parser.add_argument('destdir', metavar='dest-dir',
            help='destination directory')

    parser = group.add_parser('unpack',
            help='unpack a downloaded archive to the specified directory')
    parser.set_defaults(func=cmd_unpack)
    parser.add_argument('file',
            help='archive file')
    parser.add_argument('destdir', metavar='dest-dir',
            help='destination root directory')

    parser = group.add_parser('prune',
            help='delete old offsite archives')
    parser.set_defaults(func=cmd_prune)

    parser = group.add_parser('resync',
            help='resynchronize index with data')
    parser.set_defaults(func=cmd_resync)

    parser = group.add_parser('unit',
            help='create and upload a single offsite archive')
    parser.set_defaults(func=cmd_unit)
    parser.add_argument('snapshot',
            help='snapshot name')
    parser.add_argument('mountpoint',
            help='current mountpoint of specified snapshot')
    parser.add_argument('unit',
            help='unit name')

_setup()


# Now import submodules that need these definitions
from . import aws
