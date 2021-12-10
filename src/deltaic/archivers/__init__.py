#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014-2021 Carnegie Mellon University
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

import contextlib
import os
import subprocess
import sys
from collections import deque
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryFile, mkdtemp
from typing import IO, Dict, List, Optional

import click
from pkg_resources import iter_entry_points

from ..command import pass_config
from ..sources import Source, Task
from ..storage import PhysicalSnapshot, Snapshot
from ..util import Pipeline, XAttrs, humanize_size, lockfile, make_dir_path


class Archiver:
    def __init__(self, profile_name, profile):
        self.profile_name = profile_name
        self.profile = profile

    @classmethod
    def get_archiver(cls, settings, profile_name):
        try:
            profile = settings["archivers"][profile_name]
        except KeyError:
            raise click.UsageError(f"Archive profile '{profile_name}' not found")

        archiver = profile.get("archiver")
        for entry_point in iter_entry_points("deltaic.archivers", archiver):
            subclass = entry_point.load()
            assert entry_point.name == subclass.LABEL
            return subclass(profile_name, profile)

        raise click.UsageError(
            f"Archiver '{archiver}' in profile '{profile_name}' not found"
        )

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

    def download_archives(self, set_name, archive_list, max_rate=None):
        # archive_list is a sequence of (archive_name, local_path) tuples.
        # Returns an iterator yielding one of:
        #   (archive_name, metadata)  -> Success
        #   (archive_name, exception) -> Failure
        # Archive retrievals should be initiated in order but may complete
        # out of order.
        raise NotImplementedError

    def resync(self):
        # Perform any necessary resynchronization of data and metadata.
        raise NotImplementedError

    def report_cost(self):
        # Print report on storage costs to stdout.
        raise NotImplementedError


class DownloadState:
    # Retrieval helper class for Archiver subclasses.

    def __init__(self, items):
        # items is an iterable of (name, size) tuples.
        self.name = None
        self.offset = 0
        self.remaining = 0
        self.size = 0
        self.requests_done = False
        self.done = False

        self._pending = deque(items)
        self._outstanding_requests = {}  # name -> count
        self._failed = set()  # name
        self._next_item()

    def _next_item(self):
        try:
            self.name, self.size = self._pending.popleft()
            self.offset = 0
            self.remaining = self.size
            self._outstanding_requests[self.name] = 0
        except IndexError:
            # Out of items.
            self.name = None
            self.offset = self.size = self.remaining = 0
            self.requests_done = True

    def requested(self, count):
        # We just made a retrieval request of @count bytes.
        assert count <= self.remaining
        self.offset += count
        self.remaining -= count
        self._outstanding_requests[self.name] += 1
        if self.remaining == 0:
            self._next_item()

    def response_failed(self, name):
        # We just processed one failed response for @name.  Mark @name as
        # failed, skip the rest of the item if not completely requested,
        # and return True if this is its first failure.
        assert self._outstanding_requests.get(name, 0) > 0
        ret = name not in self._failed
        self._failed.add(name)
        if name == self.name:
            self._next_item()
        self.response_processed(name)
        return ret

    def response_processed(self, name):
        # We just processed one successful response for @name.  Return True
        # if @name is complete and successful, False otherwise.
        assert self._outstanding_requests.get(name, 0) > 0
        self._outstanding_requests[name] -= 1
        if name != self.name and self._outstanding_requests[name] == 0:
            # Done with @name.
            del self._outstanding_requests[name]
            if self.name is None and not self._outstanding_requests:
                self.done = True
            ret = name not in self._failed
            self._failed.discard(name)
            return ret
        else:
            return False


class ArchiveInfo:
    ATTR_COMPRESSION = "user.archive.compression"
    ATTR_ENCRYPTION = "user.archive.encryption"
    ATTR_SHA256 = "user.archive.sha256"

    def __init__(self, compression, encryption, sha256, size):
        self.compression = compression.encode("utf-8")
        self.encryption = encryption.encode("utf-8")
        self.sha256 = sha256.encode("utf-8")
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
            raise OSError("Size mismatch")
        attrs = XAttrs(path)
        attrs.update(self.ATTR_COMPRESSION, self.compression)
        attrs.update(self.ATTR_ENCRYPTION, self.encryption)
        attrs.update(self.ATTR_SHA256, self.sha256)


class ArchivePacker:
    BUFLEN = 4 << 20

    def __init__(self, settings):
        self._spool_dir = settings["archive-spool"]
        self._tar = settings.get("archive-tar-path", "tar")
        self._gpg = settings.get("archive-gpg-path", "gpg2")
        self._gpg_recipients = settings.get("archive-gpg-recipients")
        self._gpg_signing_key = settings.get("archive-gpg-signing-key")

        self._gpg_env = dict(os.environ)
        with contextlib.suppress(OSError):  # stdin is not a tty
            # Required for GPG passphrase prompting
            self._gpg_env["GPG_TTY"] = os.ttyname(sys.stdin.fileno())

        self.encryption = "gpg" if self._gpg_recipients else "none"

    @classmethod
    def _compress_option(self, compression):
        if compression == "gzip":
            return "--gzip"
        elif compression == "lzop":
            return "--lzop"
        elif compression == "none":
            return None
        else:
            raise ValueError("Unknown compression algorithm")

    def _gpg_cmd(self, args):
        return [
            self._gpg,
            "--batch",
            "--no-tty",
            "--no-options",
            "--personal-cipher-preferences",
            "AES256,AES192,AES",
            "--personal-digest-preferences",
            "SHA256,SHA1",
            "--personal-compress-preferences",
            "Uncompressed",
        ] + args

    def pack(self, snapshot_name, snapshot_root, unit_name, compression, out_fh):
        cmds = []

        tar = [
            self._tar,
            "c",
            "--force-local",
            "--format=gnu",
            "--sparse",
            "--acls",
            "--selinux",
            "--xattrs",
            "-V",
            f"{snapshot_name} {unit_name}",
            "-C",
            snapshot_root,
            unit_name,
        ]
        compress_opt = self._compress_option(compression)
        if compress_opt:
            tar.append(compress_opt)
        cmds.append(tar)

        if self.encryption == "gpg":
            args = ["-seu", self._gpg_signing_key]
            for recipient in self._gpg_recipients:
                args.extend(["-r", recipient])
            cmds.append(self._gpg_cmd(args))

        pipe_r, pipe_w = os.pipe()
        with contextlib.ExitStack() as stack:
            in_fh = stack.enter_context(os.fdopen(pipe_r, "rb"))
            stack.enter_context(Pipeline(cmds, out_fh=pipe_w, env=self._gpg_env))

            os.close(pipe_w)
            hash = sha256()
            while True:
                buf = in_fh.read(self.BUFLEN)
                if not buf:
                    break
                hash.update(buf)
                out_fh.write(buf)

        return ArchiveInfo(
            compression=compression,
            encryption=self.encryption,
            size=out_fh.tell(),
            sha256=hash.hexdigest(),
        )

    def unpack(self, in_file, out_root):
        info = ArchiveInfo.from_file(in_file)
        if info.encryption != self.encryption:
            # If the archive server is compromised, don't allow it to bypass
            # signature checking by replacing the archive and changing
            # the encryption field to "none"
            raise ValueError(
                "Archive encryption doesn't match "
                + "local settings; check metadata integrity"
            )
        compress_opt = self._compress_option(info.compression)

        with open(in_file, "rb") as fh:
            in_fh: IO[bytes] = fh
            if info.encryption == "gpg":
                # We can't decrypt and untar in a single pipeline, because
                # GPG wouldn't verify the signature until after tar had
                # already received a lot of untrusted data.
                fh2 = TemporaryFile(dir=self._spool_dir, prefix="unpack-")
                pipe_r, pipe_w = os.pipe()
                with os.fdopen(pipe_r, "r") as status_pipe:
                    # Ensure we won't be fooled by an archive which is
                    # encrypted but not signed.  While we're at it, ensure
                    # the configured signing key was used.
                    cmd = self._gpg_cmd(["-d", "--status-fd", str(pipe_w)])
                    proc = subprocess.Popen(
                        cmd, stdin=in_fh, stdout=fh2, env=self._gpg_env
                    )
                    os.close(pipe_w)

                    verified = False
                    configured_key = self._gpg_signing_key.lower()
                    for line in status_pipe:
                        words = line.split()
                        # Compare configured key with the long key ID and
                        # the key fingerprint
                        with contextlib.suppress(IndexError):
                            if words[2].lower() == configured_key and words[1] in (
                                "GOODSIG",
                                "VALIDSIG",
                            ):
                                verified = True

                    proc.wait()
                    if proc.returncode:
                        raise OSError(f"gpg failed with exit status {proc.returncode}")
                    if not verified:
                        raise OSError(
                            "Could not verify GPG signature with configured signing key"
                        )
                    fh2.seek(0)

                    # finally
                    # fh.close()
                    in_fh = fh2

            elif info.encryption == "none":
                # No signature, so check SHA-256.
                hash = sha256()
                while True:
                    buf = in_fh.read(self.BUFLEN)
                    if not buf:
                        break
                    hash.update(buf)
                if hash.hexdigest() != info.sha256:
                    raise OSError("SHA-256 mismatch")
                in_fh.seek(0)

            else:
                raise ValueError("Unknown encryption method")

            tar = [
                self._tar,
                "x",
                "--force-local",
                "--acls",
                "--selinux",
                "--xattrs",
                "-C",
                out_root,
            ]
            if compress_opt:
                tar.append(compress_opt)
            Pipeline([tar], in_fh=in_fh).close()


class _ArchiveTask(Task):
    def __init__(self, settings, archiver, snapshot, snapshot_dir, units):
        self._settings = settings
        self._archiver = archiver
        self._snapshot = snapshot
        self._snapshot_dir = snapshot_dir
        thread_count = archiver.profile.get("workers", 8)
        Task.__init__(self, thread_count, units)

    def _execute(self, unit):
        log_dir = make_dir_path(
            self._settings["root"],
            "Logs",
            "Archive",
            self._archiver.profile_name,
            unit.root,
        )
        args = [
            "archive",
            "-p",
            self._archiver.profile_name,
            "unit",
            self._snapshot.name,
            self._snapshot_dir,
            unit.root,
        ]
        return self._run_subcommand(unit.root, args, log_dir)


class SnapshotArchiveSet:
    def __init__(
        self, archiver, snapshot, count=None, size=None, complete=None, protected=None
    ):
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
            sets.append(
                cls(
                    archiver,
                    Snapshot(set_name),
                    complete=metadata["complete"],
                    protected=metadata["protected"],
                    count=metadata["count"],
                    size=metadata["size"],
                )
            )
        return sets

    def get_archives(self):
        result = self.archiver.list_set_archives(self.snapshot.name)
        archives = {}
        for unit_name, metadata in result.items():
            archives[unit_name] = _Archive(
                self.archiver, self.snapshot, unit_name, size=int(metadata["size"])
            )
        return archives

    def get_archive(self, unit_name):
        return _Archive(self.archiver, self.snapshot, unit_name)

    def mark_complete(self):
        self.archiver.complete_set(self.snapshot.name)

    def retrieve_archives(self, out_dir, archives, max_rate=None):
        # Returns an iterator yielding one of:
        #   (archive, file_path) -> Success
        #   (archive, exception) -> Failure

        # Collect metadata
        archive_lookup = {}  # name -> _Archive
        archive_paths = {}  # _Archive -> out_path
        archive_list = []  # [(name, out_path)]
        for archive in archives:
            out_path = os.path.join(
                out_dir,
                f"{self.snapshot.name}:{archive.unit_name.replace('/', '-')}",
            )
            if not os.path.exists(out_path):
                archive_lookup[archive.unit_name] = archive
                archive_paths[archive] = out_path
                archive_list.append((archive.unit_name, out_path))
            else:
                yield (archive, ValueError("Output file already exists"))

        # Retrieve
        for archive_name, result in self.archiver.download_archives(
            self.snapshot.name, archive_list, max_rate=max_rate
        ):
            archive = archive_lookup[archive_name]
            out_path = archive_paths[archive]
            if not isinstance(result, Exception):
                try:
                    ArchiveInfo(
                        compression=result["compression"],
                        encryption=result["encryption"],
                        size=int(result["size"]),
                        sha256=result["sha256"],
                    ).to_file(out_path)
                    result = out_path
                except Exception as e:
                    result = e
            yield (archive, result)

    def delete(self):
        self.archiver.delete_set(self.snapshot.name)


class _Archive:
    def __init__(self, archiver, snapshot, unit_name, size=None):
        self.archiver = archiver
        self.snapshot = snapshot
        self.unit_name = unit_name
        self.size = size

    def store(self, in_path, info):
        metadata = {
            "compression": info.compression,
            "encryption": info.encryption,
            "sha256": info.sha256,
            "size": str(info.size),
        }
        self.archiver.upload_archive(
            self.snapshot.name, self.unit_name, metadata, in_path
        )


def archive_unit(config, archive, snapshot_root):
    settings = config["settings"]
    profile = settings["archivers"][archive.archiver.profile_name]
    manifest = config.get("archivers", {}).get(archive.archiver.profile_name, {})
    compression = manifest.get(archive.unit_name, {}).get("compression")
    if not compression:
        compression = profile.get("compression", "gzip")
    spool_dir = settings["archive-spool"]
    packer = ArchivePacker(settings)
    with NamedTemporaryFile(dir=spool_dir, prefix="archive-") as out_fh:
        info = packer.pack(
            archive.snapshot.name, snapshot_root, archive.unit_name, compression, out_fh
        )
        out_fh.flush()
        archive.store(out_fh.name, info)


def archive_snapshot(config, archiver, snapshot):
    settings = config["settings"]
    set = SnapshotArchiveSet(archiver, snapshot)
    archives = set.get_archives()

    spool_dir = settings["archive-spool"]
    snapshot_dir = mkdtemp(dir=spool_dir, prefix="snapshot-")
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
                    if os.path.exists(unit_path) and unit.root not in archives:
                        units.append(unit)
            if units:
                task = _ArchiveTask(settings, archiver, snapshot, snapshot_dir, units)
                task.start()
                if not task.wait():
                    return False
            set.mark_complete()
            return True
        finally:
            snapshot.umount(snapshot_dir)
    finally:
        os.rmdir(snapshot_dir)


def prune_archives(settings, archiver):
    keep_count = archiver.profile.get("keep-count", 1)
    sets = SnapshotArchiveSet.list(archiver)
    # Delete all incomplete sets, ignoring the most recent set
    delete = [s for s in sets[:-1] if not s.complete]
    # Delete all complete sets except the most recent keep_count
    delete += [s for s in sets if s.complete][:-keep_count]
    # ...but don't delete protected sets
    delete = [s for s in delete if not s.protected]
    for set in delete:
        incomplete = " incomplete" if not set.complete else ""
        print(f"Pruning{incomplete} archive set {set}")
        set.delete()


@click.group()
@click.option(
    "-p",
    "--profile",
    metavar="PROFILE",
    default="default",
    show_default=True,
    help="archiver profile",
)
@click.pass_context
def archive(ctx, profile):
    """offsite archiving"""
    settings = ctx.obj["settings"]
    ctx.obj = Archiver.get_archiver(settings, profile)


pass_archiver = click.make_pass_decorator(Archiver)


@archive.command()
@click.option("-r", "--resume", is_flag=True, help="resume previous run")
@click.argument("snapshot", required=False)
@pass_archiver
@pass_config
def run(config: Dict, archiver: Archiver, resume: bool, snapshot: str):
    """create and upload an offsite archive for every unit

    Will archive the latest snapshot unless a specific SNAPSHOT has been
    specified.
    """
    settings = config["settings"]

    if snapshot is not None and resume:
        raise click.UsageError("Cannot specify snapshot with --resume")

    elif snapshot is not None:
        for chosen_snapshot in PhysicalSnapshot.list():
            if chosen_snapshot.name == snapshot:
                break
        else:
            raise click.UsageError("No such snapshot")
        print("Archiving selected snapshot", chosen_snapshot)

    elif resume:
        set = SnapshotArchiveSet.list(archiver)[-1]
        chosen_snapshot = set.snapshot
        if set.complete:
            raise click.UsageError(f"{chosen_snapshot} already completely archived")
        print("Resuming archive of snapshot", chosen_snapshot)

    else:
        chosen_snapshot = PhysicalSnapshot.list()[-1]
        print("Archiving snapshot", chosen_snapshot)

    physical_snapshot = chosen_snapshot.get_physical(settings)
    with lockfile(settings, "archive"):
        if not archive_snapshot(config, archiver, physical_snapshot):
            print(
                "Archiving failed for some units.  "
                "Not marking archive set complete.",
                file=sys.stderr,
            )
            print('Use "deltaic archive run -r" to resume.', file=sys.stderr)
            sys.exit(1)


@archive.command()
@pass_archiver
def cost(archiver: Archiver):
    """calculate storage costs"""
    archiver.report_cost()


@archive.command()
@click.option("-s", "--sets", is_flag=True, help="list archive sets")
@click.argument("set_", required=False)
@pass_archiver
def ls(archiver: Archiver, sets: bool, set_: Optional[str]):
    """list existing offsite archives

    Optionally specify which archive SET to examine
    """
    for archive_set in SnapshotArchiveSet.list(archiver):
        if set_ and set_ != archive_set.snapshot.name:
            continue
        if sets:
            print(
                "%s %5d %10s  %s %s"
                % (
                    archive_set.snapshot.name,
                    archive_set.count,
                    humanize_size(archive_set.size),
                    "  complete" if archive_set.complete else "incomplete",
                    "protected" if archive_set.protected else "",
                )
            )
        else:
            for _, archive in sorted(archive_set.get_archives().items()):
                print(
                    "%s %10s %s"
                    % (
                        archive.snapshot.name,
                        humanize_size(archive.size),
                        archive.unit_name,
                    )
                )


@archive.command()
@click.option(
    "-r",
    "--max-rate",
    type=float,
    help="maximum retrieval rate in (possibly fractional) GiB/hour",
)
@click.argument("snapshot")
@click.argument(
    "destdir",
    type=click.Path(
        exists=False, file_okay=False, writable=True, resolve_path=True, path_type=Path
    ),
)
@click.argument("unit", required=True, nargs=-1)
@pass_archiver
def retrieve(
    archiver: Archiver,
    max_rate: Optional[float],
    snapshot: str,
    destdir: Path,
    unit: List[str],
):
    """download offsite archives to the specified directory"""
    set = SnapshotArchiveSet(archiver, Snapshot(snapshot))
    archives = [set.get_archive(unit_) for unit_ in unit]
    max_rate = int(max_rate * (1 << 30)) if max_rate is not None else None

    destdir.mkdir(parents=True, exist_ok=True)

    ret = 0
    for archive, result in set.retrieve_archives(destdir, archives, max_rate=max_rate):
        if isinstance(result, Exception):
            print(f"{archive.unit_name}: {result}", file=sys.stderr)
            ret = 1
        else:
            print(archive.unit_name)
    if ret != 0:
        sys.exit(ret)


@archive.command()
@click.argument(
    "destdir",
    type=click.Path(
        exists=False, file_okay=False, writable=True, resolve_path=True, path_type=Path
    ),
)
@click.argument("file_", required=True, nargs=-1)
@pass_config
def unpack(config: Dict, destdir: Path, file_: List[str]):
    """unpack downloaded archives to the specified directory

    To avoid repeated passphrase prompts, ensure gpg-agent is running in your
    session.
    """
    settings = config["settings"]

    packer = ArchivePacker(settings)

    destdir.mkdir(parents=True, exist_ok=True)

    for filename in file_:
        packer.unpack(filename, destdir)


@archive.command()
@pass_archiver
@pass_config
def prune(config: Dict, archiver: Archiver):
    """delete old offsite archives"""
    settings = config["settings"]
    prune_archives(settings, archiver)


@archive.command()
@pass_archiver
def resync(archiver: Archiver):
    """resynchronize index with data"""
    archiver.resync()


@archive.command()
@click.argument("snapshot")
@click.argument("mountpoint")
@click.argument("unit")
@pass_archiver
@pass_config
def unit(
    config: Dict,
    archiver: Archiver,
    snapshot: str,
    mountpoint: str,
    unit: str,
):
    """low-level command to upload a single offsite archive

    MOUNTPOINT is the current mountpoint of the specified snapshot
    """
    archive_set = SnapshotArchiveSet(archiver, Snapshot(snapshot))
    archive = archive_set.get_archive(unit)
    archive_unit(config, archive, mountpoint)
