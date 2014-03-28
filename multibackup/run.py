from .command import parser, subparsers
from .lvm import Snapshot
from .source import Source
from . import sources as _  # Load all sources

def run_tasks(config, source_names, global_args=None):
    source_map = Source.get_sources()
    sources = []
    for name in source_names:
        source = source_map[name](config)
        source.start(global_args)
        sources.append(source)
    for source in sources:
        source.wait()


def cmd_run(config, args):
    global_args = []
    if args.config_file != parser.get_default('config_file'):
        global_args.extend(['-c', args.config_file])
    run_tasks(config, args.sources, global_args)
    if args.snapshot:
        vg, lv = config['settings']['backup-lv'].split('/')
        Snapshot.create(vg, lv, verbose=True)


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
