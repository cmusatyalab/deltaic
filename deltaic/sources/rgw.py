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

import boto
from boto.s3.connection import OrdinaryCallingFormat
import dateutil.parser
import json
from multiprocessing import Pool
import os
import subprocess
import sys
import time
import xml.etree.cElementTree as ET

from ..command import make_subcommand_group
from ..util import (BloomSet, UpdateFile, update_file, random_do_work,
        datetime_to_time_t, make_dir_path)
from . import Source, Unit

KEY_METADATA_ATTRS = {
    'cache_control': 'Cache-Control',
    'content_disposition': 'Content-Disposition',
    'content_encoding': 'Content-Encoding',
    'content_language': 'Content-Language',
    'content_type': 'Content-Type',
    'etag': 'ETag',
    'last_modified': 'Last-Modified',
}

KEY_UPLOAD_HEADERS = set([
    'cache-control',
    'content-disposition',
    'content-encoding',
    'content-language',
    'content-type',
])

S3_NAMESPACE = 'http://s3.amazonaws.com/doc/2006-03-01/'

SCRUB_NONE = 0
SCRUB_ACLS = 1
SCRUB_ALL = 2

def radosgw_admin(*args):
    proc = subprocess.Popen(['radosgw-admin', '--format=json'] +
            list(args), stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    if proc.returncode:
        raise IOError('radosgw-admin returned %d' % proc.returncode)
    return json.loads(out)


def get_bucket_credentials(bucket_name):
    info = radosgw_admin('bucket', 'stats', '--bucket', bucket_name)
    return get_user_credentials(info['owner'])


def get_user_credentials(userid):
    info = radosgw_admin('user', 'info', '--uid', userid)
    creds = info['keys'][0]
    return creds['access_key'], creds['secret_key']


def connect(server, access_key, secret_key, secure=False):
    return boto.connect_s3(host=server, is_secure=secure or False,
            aws_access_key_id=access_key, aws_secret_access_key=secret_key,
            calling_format=OrdinaryCallingFormat())


def add_type_code(path, code):
    return path + '_' + code


def split_type_code(path):
    if path[-2] != '_':
        raise ValueError('Path has no type code: %s' % path)
    return path[:-2], path[-1]


def key_name_to_path(root_dir, key_name, type_code):
    key_name = key_name.encode('utf-8')
    rel_dirpath, filename = os.path.split(key_name)
    # Don't rewrite root directory to "_d"
    if rel_dirpath:
        rel_dirpath = '/'.join([add_type_code(n, 'd') for n in
                rel_dirpath.split('/')])
    filename = add_type_code(filename, type_code)
    return os.path.join(root_dir, rel_dirpath, filename)


def path_to_key_name(root_dir, path):
    relpath = os.path.relpath(path, root_dir)
    components_in = relpath.split('/')
    components_out = []
    for component in components_in[:-1]:
        component, code = split_type_code(component)
        if code != 'd':
            raise ValueError('Path element missing directory type code: %s' %
                    component)
        components_out.append(component)
    component, _ = split_type_code(components_in[-1])
    components_out.append(component)
    return '/'.join(components_out).decode('utf-8')


def enumerate_keys(bucket):
    names = BloomSet()
    def iter():
        for key in bucket.list():
            names.add(key.name)
            yield (key.name, key.size, key.last_modified)
    return iter(), names


def enumerate_keys_from_directory(root_dir):
    def handle_err(err):
        raise err
    for dirpath, _, filenames in os.walk(root_dir, onerror=handle_err):
        for filename in sorted(filenames):
            filepath = os.path.join(dirpath, filename)
            try:
                _, code = split_type_code(filepath)
            except ValueError:
                continue
            if code != 'k':
                continue
            yield path_to_key_name(root_dir, filepath)


def sync_pool_init(root_dir_, server, bucket_name, access_key, secret_key,
        secure, scrub_):
    global root_dir, download_bucket, scrub
    root_dir = root_dir_
    scrub = scrub_
    conn = connect(server, access_key, secret_key, secure=secure)
    download_bucket = conn.get_bucket(bucket_name)


def sync_key(args):
    key_name, key_size, key_date = args
    key_time = datetime_to_time_t(dateutil.parser.parse(key_date))
    out_data = key_name_to_path(root_dir, key_name, 'k')
    out_meta = key_name_to_path(root_dir, key_name, 'm')
    out_acl = key_name_to_path(root_dir, key_name, 'a')
    out_dir = os.path.dirname(out_data)

    try:
        st = os.stat(out_data)
        update_data = (scrub == SCRUB_ALL or st.st_size != key_size or
                st.st_mtime != key_time)
    except OSError:
        update_data = True

    if not update_data and scrub == SCRUB_NONE:
        return (None, None)

    make_dir_path(out_dir)

    key = download_bucket.new_key(key_name)
    updated = False
    try:
        if update_data:
            with UpdateFile(out_data, suffix='_t') as fh:
                key.get_contents_to_file(fh)
            updated |= fh.modified
            metadata = {
                'metadata': key.metadata,
            }
            for attr, name in KEY_METADATA_ATTRS.items():
                value = getattr(key, attr, None)
                if value:
                    metadata[name] = value
            updated |= update_file(out_meta, json.dumps(metadata,
                    sort_keys=True), suffix='_t')
        updated |= update_file(out_acl, key.get_xml_acl(), suffix='_t')
        if update_data:
            for path in out_data, out_meta:
                if os.stat(path).st_mtime != key_time:
                    os.utime(path, (key_time, key_time))
            # Don't utimes out_acl, since the Last-Modified time doesn't
            # apply to it
        if updated:
            return (key_name, None)
        else:
            return (None, None)
    except Exception, e:
        for path in out_data, out_meta, out_acl:
            try:
                os.unlink(path)
            except OSError:
                pass
        if isinstance(e, boto.exception.BotoServerError):
            e = e.error_code
        return (key_name, "Couldn't fetch %s: %s" % (key_name, e))


def sync_bucket(server, bucket_name, root_dir, workers, scrub, secure):
    warned = set()
    def warn(msg, *args):
        print >>sys.stderr, msg % args
        warned.add(True)

    # Connect
    access_key, secret_key = get_bucket_credentials(bucket_name)
    conn = connect(server, access_key, secret_key, secure=secure)
    bucket = conn.get_bucket(bucket_name)

    # Create root directory
    make_dir_path(root_dir)

    # Keys
    start_time = time.time()
    pool = Pool(workers, sync_pool_init, [root_dir, server, bucket_name,
            access_key, secret_key, secure, scrub])
    iter, key_set = enumerate_keys(bucket)
    for path, error in pool.imap_unordered(sync_key, iter):
        if error:
            warn(error)
        elif path:
            print path
    pool.close()
    pool.join()

    # Bucket metadata
    update_file(key_name_to_path(root_dir, 'bucket', 'A'),
            bucket.get_xml_acl())
    path = key_name_to_path(root_dir, 'bucket', 'C')
    try:
        update_file(path, bucket.get_cors_xml())
    except boto.exception.S3ResponseError, e:
        if e.status == 404:
            try:
                os.unlink(path)
            except OSError:
                pass
        else:
            raise

    # Collect garbage
    def handle_err(err):
        warn("Couldn't list directory: %s", err)
    for dirpath, _, filenames in os.walk(root_dir, topdown=False,
            onerror=handle_err):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                _, code = split_type_code(filepath)
            except ValueError:
                # Delete files without type codes
                delete = True
            else:
                if code in ('A', 'C'):
                    # Bucket metadata
                    delete = False
                elif code == 't':
                    # Leftover temporary file
                    delete = True
                else:
                    key_name = path_to_key_name(root_dir, filepath)
                    delete = key_name not in key_set
                    if delete and os.stat(filepath).st_mtime > start_time:
                        # Probably a failure to non-destructively encode the
                        # key name in the filesystem path.
                        warn('Warning: Deleting file that we just created: %s',
                                filepath)
            if delete:
                print 'Deleting', filepath
                try:
                    os.unlink(filepath)
                except OSError, e:
                    warn("Couldn't unlink: %s", e)
        try:
            os.rmdir(dirpath)
        except OSError:
            # Directory not empty
            pass

    # Return True on success, False if there were warnings
    return not warned


def upload_pool_init(root_dir_, server, bucket_name, secure):
    global root_dir, upload_server, upload_bucket_name, upload_secure
    global upload_buckets
    root_dir = root_dir_
    upload_server = server
    upload_bucket_name = bucket_name
    upload_secure = secure
    upload_buckets = {}  # owner -> bucket


def get_owner_name(acl_xml):
    return ET.fromstring(acl_xml).find('{%(ns)s}Owner/{%(ns)s}ID' %
            {'ns': S3_NAMESPACE}).text


def upload_get_bucket(owner):
    if owner not in upload_buckets:
        access_key, secret_key = get_user_credentials(owner)
        conn = connect(upload_server, access_key, secret_key,
                secure=upload_secure)
        upload_buckets[owner] = conn.get_bucket(upload_bucket_name,
                validate=False)
    return upload_buckets[owner]


def upload_key(args):
    key_name = args
    in_data = key_name_to_path(root_dir, key_name, 'k')
    in_meta = key_name_to_path(root_dir, key_name, 'm')
    in_acl = key_name_to_path(root_dir, key_name, 'a')

    key = None
    try:
        with open(in_acl, 'rb') as fh:
            key_acl = fh.read()
        owner = get_owner_name(key_acl)
        key = upload_get_bucket(owner).new_key(key_name)

        with open(in_meta, 'rb') as fh:
            meta = json.load(fh)
        key.metadata.update(meta['metadata'])
        headers = dict((k, v) for k, v in meta.items()
                if k.lower() in KEY_UPLOAD_HEADERS)
        with open(in_data, 'rb') as fh:
            key.set_contents_from_file(fh, headers=headers)
        key.set_xml_acl(key_acl)
        return (key_name, None)
    except Exception, e:
        if key:
            key.delete()
        if isinstance(e, boto.exception.BotoServerError):
            e = e.error_code
        return (key_name, "Couldn't upload %s: %s" % (key_name, e))


def restore_bucket(root_dir, server, dest_bucket_name, force, secure, workers):
    # Check for valid bucket dir
    bucket_acl_path = key_name_to_path(root_dir, 'bucket', 'A')
    if not os.path.exists(bucket_acl_path):
        raise IOError('No backups at %s' % root_dir)

    # Get bucket ACL and owner
    with open(bucket_acl_path) as fh:
        bucket_acl = fh.read()
    owner = get_owner_name(bucket_acl)

    # Connect
    access_key, secret_key = get_user_credentials(owner)
    conn = connect(server, access_key, secret_key, secure=secure)

    # Get or create bucket
    bucket = conn.lookup(dest_bucket_name)
    if bucket is None:
        bucket = conn.create_bucket(dest_bucket_name)
    elif not force and bucket.get_all_keys(max_keys=5):
        raise IOError('Destination bucket is not empty; force with --force')

    # Set bucket metadata
    bucket.set_xml_acl(bucket_acl)
    cors_path = key_name_to_path(root_dir, 'bucket', 'C')
    if os.path.exists(cors_path):
        with open(cors_path) as fh:
            bucket.set_cors_xml(fh.read())

    # Upload keys
    pool = Pool(workers, upload_pool_init, [root_dir, server,
            dest_bucket_name, secure])
    iter = enumerate_keys_from_directory(root_dir)
    for path, error in pool.imap_unordered(upload_key, iter):
        if error:
            raise IOError(error)
        elif path:
            print path
    pool.close()
    pool.join()


def get_relroot(bucket):
    return os.path.join('rgw', bucket)


def cmd_rgw_backup(config, args):
    settings = config['settings']
    root_dir = os.path.join(settings['root'], get_relroot(args.bucket))
    server = settings['rgw-server']
    secure = settings.get('rgw-secure', False)
    workers = settings.get('rgw-threads', 4)
    if args.scrub:
        scrub = SCRUB_ALL
    elif args.scrub_acls:
        scrub = SCRUB_ACLS
    else:
        scrub = SCRUB_NONE
    if not sync_bucket(server, args.bucket, root_dir, workers=workers,
            scrub=scrub, secure=secure):
        return 1


def cmd_rgw_restore(config, args):
    settings = config['settings']
    if '/' in args.source:
        root_dir = os.path.abspath(args.source)
    else:
        root_dir = os.path.join(settings['root'], get_relroot(args.source))
    server = settings['rgw-server']
    secure = settings.get('rgw-secure', False)
    workers = settings.get('rgw-threads', 4)
    restore_bucket(root_dir, server, args.dest_bucket, force=args.force,
            workers=workers, secure=secure)


def _setup():
    group = make_subcommand_group('rgw',
            help='low-level radosgw support')

    parser = group.add_parser('backup',
            help='back up radosgw bucket')
    parser.set_defaults(func=cmd_rgw_backup)
    parser.add_argument('bucket',
            help='bucket name')
    parser.add_argument('-A', '--scrub-acls', action='store_true',
            help='update ACLs for unmodified keys')
    parser.add_argument('-c', '--scrub', action='store_true',
            help='check backup data against original')

    parser = group.add_parser('restore',
            help='restore radosgw bucket')
    parser.set_defaults(func=cmd_rgw_restore)
    parser.add_argument('source', metavar='origin-bucket-or-path',
            help='origin bucket name or filesystem path')
    parser.add_argument('dest_bucket', metavar='dest-bucket',
            help='destination bucket for restore')
    parser.add_argument('-f', '--force', action='store_true',
            help='force restore to non-empty bucket')

_setup()


class BucketUnit(Unit):
    def __init__(self, settings, name):
        Unit.__init__(self)
        self.root = get_relroot(name)
        self.backup_args = ['rgw', 'backup', name]
        if random_do_work(settings, 'rgw-scrub-acl-probability', 0):
            self.backup_args.append('-A')
        if random_do_work(settings, 'rgw-scrub-probability', 0.0166):
            self.backup_args.append('-c')


class RGWSource(Source):
    LABEL = 'rgw'

    def get_units(self):
        ret = []
        for name in self._manifest:
            ret.append(BucketUnit(self._settings, name))
        return ret
