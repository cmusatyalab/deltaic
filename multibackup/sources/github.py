from getpass import getpass
import github3
import yaml

from ..command import make_subcommand_group

OAUTH_SCOPES = ('read:org', 'repo')
TOKEN_NOTE = 'multibackup github source'
TOKEN_NOTE_URL = 'https://github.com/cmusatyalab/multibackup'

def cmd_github_auth(config, args):
    settings = config['settings']
    token = settings.get('github-token')
    if token:
        gh = github3.login(token=token)
        try:
            gh.rate_limit()
            return
        except github3.GitHubError:
            print 'Stored token was not accepted; reauthorizing'

    user = raw_input('Username: ')
    passwd = getpass()
    def get_2fa_code():
        return raw_input('Two-factor authentication code: ')

    gh = github3.login(user, passwd, two_factor_callback=get_2fa_code)
    auth = gh.authorize(user, passwd, OAUTH_SCOPES, TOKEN_NOTE,
            TOKEN_NOTE_URL)
    structured = {
        'settings': {
            'github-token': auth.token,
        }
    }
    print '\n' + yaml.safe_dump(structured, default_flow_style=False).strip()


def _setup():
    group = make_subcommand_group('github',
            help='GitHub support')

    parser = group.add_parser('auth',
            help='obtain OAuth token for config file')
    parser.set_defaults(func=cmd_github_auth)

_setup()
