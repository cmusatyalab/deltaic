from getpass import getpass
import github3
import json
import os
import re
import subprocess
import sys
import time
import yaml

from ..command import make_subcommand_group, get_cmdline_for_subcommand
from ..source import Task, Source
from ..util import update_file

OAUTH_SCOPES = ('read:org', 'repo')
TOKEN_NOTE = 'multibackup github source'
TOKEN_NOTE_URL = 'https://github.com/cmusatyalab/multibackup'
USER_AGENT = 'multibackup-github/1'

def write_json(path, info):
    update_file(path, json.dumps(info, sort_keys=True) + '\n')


def update_git(url, root_dir, token, git_path=None):
    if git_path is None:
        git_path = 'git'
    if not os.path.exists(root_dir):
        cmd = [git_path, 'clone', '--mirror', url, root_dir]
        cwd = None
    else:
        cmd = [git_path, 'remote', 'update', '--prune']
        cwd = root_dir

    askpass = os.path.join(os.path.dirname(sys.argv[0]), 'mb-askpass')
    askpass = os.path.abspath(askpass)
    env = dict(os.environ)
    env.update({
        'GIT_ASKPASS': askpass,
        'MB_ASKPASS_USER': token,
        'MB_ASKPASS_PASS': '',
    })

    for tries_remaining in range(4, -1, -1):
        try:
            print ' '.join(cmd)
            subprocess.check_call(cmd, cwd=cwd, env=env)
            break
        except subprocess.CalledProcessError:
            if not tries_remaining:
                raise
        time.sleep(1)


def sync_repo(repo, root_dir, token, git_path=None):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    # Repo metadata
    info = {
        'description': repo.description,
        'has_issues': repo.has_issues,
        'has_wiki': repo.has_wiki,
        'homepage': repo.homepage,
        'private': repo.private,
    }
    write_json(os.path.join(root_dir, 'info.json'), info)

    # Git
    update_git(repo.clone_url, os.path.join(root_dir, 'repo'), token,
            git_path=git_path)
    if repo.has_wiki:
        update_git(re.sub('\.git$', '.wiki', repo.clone_url),
                os.path.join(root_dir, 'wiki'), token, git_path=git_path)


def sync_org(org, root_dir):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    teams = {}
    for team in org.iter_teams():
        teams[team.name] = {
            'permission': team.permission,
            'members': [u.login for u in team.iter_members()],
            'repos': [r.name for r in team.iter_repos()],
        }
    write_json(os.path.join(root_dir, 'teams.json'), teams)


def github_login(*args, **kwargs):
    gh = github3.login(*args, **kwargs)
    gh.set_user_agent(USER_AGENT)
    return gh


def cmd_github_auth(config, args):
    settings = config['settings']

    token = settings.get('github-token')
    if token:
        gh = github_login(token=token)
        try:
            gh.rate_limit()
            return
        except github3.GitHubError:
            print 'Stored token was not accepted; reauthorizing'

    user = raw_input('Username: ')
    passwd = getpass()
    def get_2fa_code():
        return raw_input('Two-factor authentication code: ')

    gh = github_login(user, passwd, two_factor_callback=get_2fa_code)
    auth = gh.authorize(user, passwd, OAUTH_SCOPES, TOKEN_NOTE,
            TOKEN_NOTE_URL)
    structured = {
        'settings': {
            'github-token': auth.token,
        }
    }
    print '\n' + yaml.safe_dump(structured, default_flow_style=False).strip()


def cmd_github_backup(config, args):
    settings = config['settings']
    token = settings['github-token']
    org_dir = os.path.join(settings['root'], 'github', args.organization)
    gh = github_login(token=token)

    if args.repo is not None:
        sync_repo(gh.repository(args.organization, args.repo),
                os.path.join(org_dir, args.repo), token,
                git_path=settings.get('github-git-path'))
    else:
        sync_org(gh.organization(args.organization), org_dir)

    print gh.ratelimit_remaining, 'requests left in quota'


def cmd_github_ls(config, args):
    settings = config['settings']
    gh = github_login(token=settings['github-token'])
    org = gh.organization(args.organization)
    for repo in sorted(org.iter_repos(), key=lambda r: r.name.lower()):
        print repo.name


def _setup():
    group = make_subcommand_group('github',
            help='GitHub support')

    parser = group.add_parser('auth',
            help='obtain OAuth token for config file')
    parser.set_defaults(func=cmd_github_auth)

    parser = group.add_parser('backup',
            help='back up GitHub organization')
    parser.set_defaults(func=cmd_github_backup)
    parser.add_argument('organization',
            help='organization name')
    parser.add_argument('repo', nargs='?',
            help='repository name (omit to back up organization metadata)')

    parser = group.add_parser('ls',
            help='list repositories in the specified GitHub organization')
    parser.set_defaults(func=cmd_github_ls)
    parser.add_argument('organization',
            help='organization name')

_setup()


class GitHubTask(Task):
    def __init__(self, org, repo=None):
        Task.__init__(self)
        self.args = ['github', 'backup', org]
        if repo:
            self.args.append(repo)


class GitHubSource(Source):
    LABEL = 'github'

    def __init__(self, config):
        Source.__init__(self, config)
        for org in self._manifest:
            # Dynamically obtain list of repos
            cmd = get_cmdline_for_subcommand(['github', 'ls', org])
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            out, _ = proc.communicate()
            if proc.returncode:
                raise IOError("Couldn't list GitHub repos for %s" % org)
            repos = [r for r in out.split('\n') if r]
            # Update org metadata
            self._queue.put(GitHubTask(org))
            # Update repos
            for repo in repos:
                self._queue.put(GitHubTask(org, repo))
