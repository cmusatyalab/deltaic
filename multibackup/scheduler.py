from datetime import date
import subprocess

from .command import subparsers
from .source import Source
from . import sources as _  # Load all sources

def run_tasks(config, source_names):
    source_map = Source.get_sources()
    sources = []
    for name in source_names:
        source = source_map[name](config)
        source.start()
        sources.append(source)
    for source in sources:
        source.wait()


def create_snapshot(settings):
    today = date.today().strftime('%Y%m%d')
    backup_lv = settings['backup-lv']
    vg = backup_lv.split('/')[0]
    with open('/dev/null', 'r+') as null:
        # Give up eventually in case test keeps failing
        for n in range(1, 100):
            snapshot_lv = '%s-%d' % (today, n)
            ret = subprocess.call(['sudo', 'lvs',
                    '%s/%s' % (vg, snapshot_lv)], stdout=null, stderr=null)
            if ret:
                break
        else:
            raise Exception("Couldn't locate unused snapshot LV")
    subprocess.check_call(['sudo', 'lvcreate', '-s', backup_lv, '-p', 'r',
            '-n', snapshot_lv, '--addtag', 'backup-snapshot'])


def cmd_run(config, args):
    run_tasks(config, args.sources)
    if args.snapshot:
        create_snapshot(config['settings'])


def _setup():
    parser = subparsers.add_parser('run',
            help='run a backup')
    parser.set_defaults(func=cmd_run)
    source_list = ', '.join(sorted(Source.get_sources()))
    parser.add_argument('-s', '--source', dest='sources', action='append',
            default=[], metavar='SOURCE',
            help='backup source to enable (%s)' % source_list)
    parser.add_argument('-S', '--no-snapshot', dest='snapshot',
            action='store_false', default=True,
            help='skip snapshot of backup volume')

_setup()
