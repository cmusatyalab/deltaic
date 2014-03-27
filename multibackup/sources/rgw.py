import boto
from boto.s3.connection import OrdinaryCallingFormat
import dateutil.parser
import json
from multiprocessing import Pool
import os
from pybloom import ScalableBloomFilter
import subprocess
import sys
import time

from ..command import make_subcommand_group

KEY_METADATA_ATTRS = {
    'cache_control': 'Cache-Control',
    'content_disposition': 'Content-Disposition',
    'content_encoding': 'Content-Encoding',
    'content_language': 'Content-Language',
    'content_type': 'Content-Type',
    'etag': 'ETag',
    'last_modified': 'Last-Modified',
}

def warn(msg, *args):
    print >>sys.stderr, msg % args


def radosgw_admin(*args):
    proc = subprocess.Popen(['radosgw-admin', '--format=json'] +
            list(args), stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    if proc.returncode:
        raise Exception('radosgw-admin returned %d' % proc.returncode)
    return json.loads(out)


def get_bucket_credentials(bucket_name):
    info = radosgw_admin('bucket', 'stats', '--bucket', bucket_name)
    owner = info['owner']

    info = radosgw_admin('user', 'info', '--uid', owner)
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


def update_file(path, data):
    # Avoid unnecessary LVM COW by only updating the file if its data has
    # changed
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
    with os.fdopen(fd, 'r+b') as fh:
        if fh.read() != data:
            fh.seek(0)
            fh.write(data)
            fh.truncate()


class KeyEnumerator(object):
    BLOOM_INITIAL_CAPACITY = 1000
    BLOOM_ERROR_RATE = 0.0001

    def __init__(self, bucket):
        self._bucket = bucket
        self._object_set = ScalableBloomFilter(
                initial_capacity=self.BLOOM_INITIAL_CAPACITY,
                error_rate=self.BLOOM_ERROR_RATE,
                mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        # False positives in the Bloom filter will cause us to fail to
        # garbage-collect an object.  Salt the Bloom filter to ensure
        # that we get a different set of false positives on every run.
        self._bloom_salt = os.urandom(2)

    def __iter__(self):
        for key in self._bucket.list():
            self._object_set.add(self._bloom_key(key.name))
            yield (key.name, key.size, key.last_modified)

    def __contains__(self, key_name):
        # Only works after iteration completes.  May return false positives.
        return self._bloom_key(key_name) in self._object_set

    def _bloom_key(self, key_name):
        return self._bloom_salt + key_name.encode('utf-8')


def pool_init(root_dir_, server, bucket_name, access_key, secret_key, secure,
        force_acls_):
    global root_dir, download_bucket, force_acls
    root_dir = root_dir_
    force_acls = force_acls_
    conn = connect(server, access_key, secret_key, secure=secure)
    download_bucket = conn.get_bucket(bucket_name)


def sync_key(args):
    key_name, key_size, key_date = args
    key_time = time.mktime(dateutil.parser.parse(key_date).timetuple())
    out_data = key_name_to_path(root_dir, key_name, 'k')
    out_meta = key_name_to_path(root_dir, key_name, 'm')
    out_acl = key_name_to_path(root_dir, key_name, 'a')
    out_dir = os.path.dirname(out_data)

    try:
        st = os.stat(out_data)
        need_update = st.st_size != key_size or st.st_mtime != key_time
    except OSError:
        need_update = True

    if not need_update and not force_acls:
        return (None, None)

    if not os.path.isdir(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError:
            # Either we lost the race with other threads, or we failed for
            # other reasons.  In the latter case, writing the data will fail.
            pass

    key = download_bucket.new_key(key_name)
    try:
        if need_update:
            with open(out_data, 'wb') as fh:
                key.get_contents_to_file(fh)
            metadata = {
                'metadata': key.metadata,
            }
            for attr, name in KEY_METADATA_ATTRS.items():
                value = getattr(key, attr, None)
                if value:
                    metadata[name] = value
            update_file(out_meta, json.dumps(metadata, sort_keys=True))
        update_file(out_acl, key.get_xml_acl())
        if need_update:
            os.utime(out_data, (key_time, key_time))
            os.utime(out_meta, (key_time, key_time))
            # Don't utimes out_acl, since the Last-Modified time doesn't
            # apply to it
        return (key_name, None)
    except Exception, e:
        for path in out_data, out_meta, out_acl:
            try:
                os.unlink(path)
            except OSError:
                pass
        if isinstance(e, boto.exception.BotoServerError):
            e = e.error_code
        return (key_name, "Couldn't fetch %s: %s" % (key_name, e))


def sync_bucket(server, bucket_name, root_dir, workers, force_acls, secure):
    # Connect
    access_key, secret_key = get_bucket_credentials(bucket_name)
    conn = connect(server, access_key, secret_key, secure=secure)
    bucket = conn.get_bucket(bucket_name)

    # Create root directory
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    # Keys
    start_time = time.time()
    pool = Pool(workers, pool_init, [root_dir, server, bucket_name,
            access_key, secret_key, secure, force_acls])
    keys = KeyEnumerator(bucket)
    for path, error in pool.imap_unordered(sync_key, keys):
        if error:
            warn(error)
        elif path:
            print path
    pool.close()
    pool.join()

    # Bucket metadata
    update_file(key_name_to_path(root_dir, 'bucket', 'A'),
            bucket.get_xml_acl())

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
                if code == 'A':
                    # Bucket ACL
                    delete = False
                else:
                    key_name = path_to_key_name(root_dir, filepath)
                    delete = key_name not in keys
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


def cmd_rgw_backup(config, args):
    sync_bucket(args.server, args.bucket, args.root_dir, workers=args.workers,
            force_acls=args.force_acls, secure=args.secure)


def _setup():
    group = make_subcommand_group('rgw',
            help='radosgw support')

    parser = group.add_parser('backup',
            help='back up radosgw bucket')
    parser.set_defaults(func=cmd_rgw_backup)
    parser.add_argument('server',
            help='server hostname')
    parser.add_argument('bucket',
            help='bucket name')
    parser.add_argument('root_dir', metavar='out-dir',
            help='path to output directory')
    parser.add_argument('-A', '--force-acls', action='store_true',
            help='update ACLs for unmodified keys')
    parser.add_argument('-j', '--jobs', metavar='COUNT', dest='workers',
            type=int, default=4,
            help='number of worker processes to start [4]')
    parser.add_argument('-s', '--secure', action='store_true',
            help='connect securely')

_setup()
