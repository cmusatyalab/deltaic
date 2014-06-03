# Deltaic

_delta, n.: a landform created by sediment flowing downstream_

Deltaic is a tool for backing up data from several types of data sources
into a filesystem.  Historical data is maintained by taking an LVM
thin-provisioned snapshot of the backup filesystem after each backup.
Backed-up data is periodically checked against the original for
discrepancies.

These data sources are currently supported:

*   Remote filesystems (via rsync)
*   GitHub organizations
*   RADOS Gateway buckets
*   RADOS Block Device images and snapshots
*   Coda volumes

Deltaic is released under the [GNU General Public License,
version 2](COPYING).


## Requirements

*   Python 2.6 or 2.7 and virtualenv
*   sudo
*   An LVM thin pool (see **lvmthin**(7)) to store the backup filesystem and
    associated snapshots.
*   A backup filesystem with extended-attribute support (the `user_xattr`
    mount option for ext4) and discard support (`discard` for ext4). 
    Note than on RHEL 6, ext4 filesystems are limited to 16 TB.  Mounting
    `noatime` is recommended.

These are the requirements for just the base system.  Individual data
sources have additional requirements; see below.


## Setting up Deltaic

1.  Create a user account to run backups.

1.  Create an LVM volume group, a thin-provisioning pool, and a
    thin-provisioned backup volume within it.

    LVM limits the size of the thin-pool metadata volume to 16 GiB.  This
    volume requires 64 bytes of capacity per data chunk, plus additional
    overhead for volume snapshots.  Configure a 16 GiB metadata volume and a
    chunk size consistent with the expected maximum size of your pool.  For
    example, for a 200 TiB pool, a chunk size of 2 MiB seems reasonable:

    ```shell
    $ thin_metadata_size -b 2m -s 200t -m 1000 -u g
    thin_metadata_size - 3.18 gigabytes estimated metadata area size for "--block-size=2megabytes --pool-size=200terabytes --max-thins=1000"
    ```

    So, for example, if your backup device is `/dev/md0` and your user
    account is `user`:

    ```shell
    pvcreate /dev/md0
    vgcreate backups /dev/md0
    lvcreate -l 100%FREE -T backups/pool --chunksize 2m --poolmetadatasize 16g
    lvcreate -V200t -T backups/pool -n current
    mkfs.ext4 -m 0 -E lazy_itable_init /dev/backups/current
    mkdir /srv/backup
    mount /dev/backups/current /srv/backup
    chown user.user /srv/backup
    chmod 700 /srv/backup
    echo "/dev/backups/current /srv/backup ext4 user_xattr,discard,noatime 0 2" >> /etc/fstab
    ```

1.  Create a config file in `~/.config/deltaic.conf`.  See
    [example-config.yaml](example-config.yaml) for the available settings. 
    Here is a minimal config file for the above configuration:

    ```yaml
    settings:
      root: /srv/backup
      backup-lv: backups/current
    ```

1.  From the backup user account:

    ```shell
    virtualenv env
    env/bin/pip install https://github.com/cmusatyalab/deltaic
    env/bin/deltaic mkconf sudoers | sudo tee /etc/sudoers.d/backup
    env/bin/deltaic mkconf crontab --email your@email.address | crontab
    ```

1. Set up data sources as described below.


## Data Sources

### `coda`: The Coda distributed filesystem

`coda` backs up individual replicas of Coda volumes by connecting to a
Coda server via SSH and running `volutil dump` and `codadump2tar`.
File/directory UIDs and mode bits are stored as rsync-compatible extended
attributes.  ACLs are not stored.

For performance, `coda` takes incremental volume dumps when possible. 
However, incremental dumps do not record deleted files, so in each backup
run, `coda` will perform full dumps on a random subset of volumes (1 in 7
by default).

#### Requirements

*   For best results, Coda 6.9.6 or above.
*   SSH `authorized_keys` access to root on the Coda servers

#### Restoring

Authenticate a Coda client on a remote host, then run:

    deltaic rsync restore --coda -u <remote-unix-user> <coda-backup-dir> <remote-host> <destination-coda-filesystem-path>

This command runs rsync with the necessary options to restore all
backed-up metadata.  Additional options specified to the restore command
(such as `--delete`) will be forwarded to rsync.


### `github`: GitHub organizations

`github` backs up GitHub teams, repositories, wikis, issues, milestones,
commit comments, release metadata, and release assets for all accessible
repositories within configured GitHub organizations.  Repositories and wikis
are stored as bare Git repositories, release assets as individual files, and
all other metadata in JSON format.

#### Requirements

*   Git 1.7.1.1 or above
*   Configured API key obtained with `deltaic github auth`

#### Restoring

Repository and wiki data can be restored with `git push`.  Restore tools for
other metadata have not yet been written.  (Note that it is impossible to
restore all data types to GitHub in their original form.  Creator
information for e.g. individual issue comments cannot be restored without
access to the creators' GitHub accounts, and timestamps cannot be restored
at all.)


### `rbd`: Ceph's block device layer

`rbd` backs up RADOS Block Device images and snapshots into large image
files.  It backs up RBD images by creating an RBD snapshot, fetching its data
with `rbd export-diff`, and applying the resulting patch to the backup image
file.  It backs up RBD snapshots by fetching their data with `rbd export-diff`.
RBD images are backed up incrementally, but snapshots are not: if a snapshot
is recreated in RBD, during the next backup it will be retrieved again from
scratch.  `rbd` supports multiple RADOS pools.

Any image backed up by `rbd` will have one or more snapshots that have been
created without the knowledge of the higher-level system (such as OpenStack
Cinder) that uses the image.  As a result, attempts to delete the image
through the higher-level system will fail.  Before deleting such an image,
remove its backup snapshots with `deltaic rbd drop`.

#### Requirements

*   Ceph installed on the backup server, and sufficient credentials to run
    `rbd` commands

#### Restoring

Use `deltaic rbd restore` to restore a backup image or snapshot to a new
RBD image.


### `rgw`: Ceph's S3-compatible storage layer

`rgw` backs up RADOS Gateway buckets using the S3 web API.  It accesses
buckets using the credentials of the bucket owner, which it obtains using
`radosgw-admin`.

`rgw` backs up object data, metadata, and ACLs, and bucket ACLs.  If an
object has not changed since the last backup, `rgw` assumes that its ACLs
have not changed either, except during a periodic scrub.  If manually
backing up an individual bucket, pass `--scrub-acls` to
`deltaic rgw backup` to override this assumption and re-download ACLs
for all objects.

#### Requirements

*   Ceph installed on the backup server, and sufficient credentials to run
    `radosgw-admin` commands
*   All objects in a backed-up bucket must be readable by the bucket owner.
    This can be accomplished manually, or with the
    [`rgw_defer_to_bucket_acls`](https://github.com/ceph/ceph/commit/1d7c2041)
    config option on Ceph 0.73 or above.

#### Restoring

Use `deltaic rgw restore` to restore a bucket.  The bucket owner, and all
users mentioned in the bucket and object ACLs, must already exist in
radosgw.  Last-Modified and ETag headers are not restored, since the
S3 API does not permit it.


### `rsync`: Remote filesystems

`rsync` backs up remote filesystems using the file synchronization program
of the same name.  File/directory ownership, modes, ACLs, etc. are stored
as extended attributes using the rsync `--fake-super` option.

#### Requirements

*   rsync 3.1.0 or above on the backup server.  For best results, use a
    current version.
*   SSH `authorized_keys` access to root on the remote hosts

#### Restoring

Use `deltaic rsync restore` to restore a file or directory from the backup
filesystem to a destination host.  This command runs rsync with the
necessary options to restore all backed-up metadata.  Additional options
specified to the restore command (such as `--delete`) will be forwarded to
rsync.
