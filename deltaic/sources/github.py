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
from ..util import (BloomSet, UpdateFile, update_file, XAttrs,
        random_do_work, datetime_to_time_t, gc_directory_tree, make_dir_path)
from . import Source, Unit

ATTR_CONTENT_TYPE = 'user.github.content-type'
ATTR_ETAG = 'user.github.etag'
GIT_ATTEMPTS = 5
OAUTH_SCOPES = ('read:org', 'repo')
TOKEN_NOTE = 'Deltaic GitHub source'
TOKEN_NOTE_URL = 'https://github.com/cmusatyalab/deltaic'
USER_AGENT = 'deltaic-github/1'

def user_str(user):
    return user.login if user else None


def timestamp_str(timestamp):
    return timestamp.isoformat() if timestamp else None


def gc_report(path, is_dir):
    print '-', path


def write_json(path, info, timestamp=None):
    if update_file(path, json.dumps(info, sort_keys=True) + '\n'):
        print 'f', path
    if timestamp is not None:
        mtime = datetime_to_time_t(timestamp)
        if os.stat(path).st_mtime != mtime:
            os.utime(path, (mtime, mtime))


class cond_iter(object):
    '''An iterable that wraps a github3 iterator, enabling conditional
    requests.  The ETag is saved as an xattr on the specified path.  func is
    the github3 iter_* function.  If scrub is True, perform the request
    unconditionally.  *args and **kwargs are passed to the func.  If the
    request returns 304 Not Modified, the iterator returns no objects and
    the skipped attribute is set to True.'''

    def __init__(self, path, func, scrub=False, *args, **kwargs):
        self._attrs = XAttrs(path)
        etag = self._attrs.get(ATTR_ETAG)
        if etag and not scrub:
            kwargs['etag'] = etag
        self._func = lambda: func(*args, **kwargs)
        self.skipped = None

    def __iter__(self):
        iter = self._func()
        for item in iter:
            yield item
        if iter.etag:
            self._attrs.update(ATTR_ETAG, iter.etag)
        self.skipped = iter.last_status == 304


def update_git(url, root_dir, token, scrub=False, ignore_clone_errors=False,
        git_path=None):
    if git_path is None:
        git_path = 'git'
    exists = os.path.exists(root_dir)
    if not exists:
        cmd = [git_path, 'clone', '--mirror', url, root_dir]
        cwd = None
    else:
        cmd = [git_path, 'remote', 'update', '--prune']
        cwd = root_dir

    askpass = os.path.join(os.path.dirname(sys.argv[0]), 'dt-askpass')
    askpass = os.path.abspath(askpass)
    env = dict(os.environ)
    env.update({
        'GIT_ASKPASS': askpass,
        'DT_ASKPASS_USER': token,
        'DT_ASKPASS_PASS': '',
    })

    for tries_remaining in range(GIT_ATTEMPTS - 1, -1, -1):
        try:
            print ' '.join(cmd)
            subprocess.check_call(cmd, cwd=cwd, env=env)
            break
        except subprocess.CalledProcessError:
            if ignore_clone_errors and not exists:
                return
            if not tries_remaining:
                raise
        time.sleep(1)

    if scrub:
        cmd = [git_path, 'fsck', '--no-dangling', '--no-progress']
        print ' '.join(cmd)
        subprocess.check_call(cmd, cwd=root_dir)


def update_issues(repo, root_dir, scrub=False):
    # Issues
    valid_paths = BloomSet()
    issue_dir = make_dir_path(root_dir, 'issues')
    valid_paths.add(issue_dir)
    issue_iter = cond_iter(issue_dir, repo.iter_issues, scrub=scrub,
            state='all')
    for issue in issue_iter:
        path = os.path.join(issue_dir, '%d.json' % issue.number)
        valid_paths.add(path)
        try:
            # We need to make additional requests to get comments and events.
            # Avoid if possible.
            if (not scrub and os.stat(path).st_mtime ==
                    datetime_to_time_t(issue.updated_at)):
                continue
        except OSError:
            pass

        if issue.comments:
            comments = [{
                    'created_at': timestamp_str(comment.created_at),
                    'updated_at': timestamp_str(comment.updated_at),
                    'user': user_str(comment.user),
                    'body': comment.body,
                } for comment in issue.iter_comments()]
        else:
            comments = []

        info = {
            'assignee': user_str(issue.assignee),
            'body': issue.body,
            'closed_at': timestamp_str(issue.closed_at),
            'closed_by': user_str(issue.closed_by),
            'comments': comments,
            'created_at': timestamp_str(issue.created_at),
            'events': [{
                    'actor': user_str(event.actor),
                    'commit_id': event.commit_id,
                    'created_at': timestamp_str(event.created_at),
                    'event': event.event,
                } for event in issue.iter_events()],
            'labels': [l.name for l in issue.labels],
            'milestone': issue.milestone.number if issue.milestone else None,
            'number': issue.number,
            'state': issue.state,
            'title': issue.title,
            'updated_at': timestamp_str(issue.updated_at),
            'user': user_str(issue.user),
        }
        write_json(path, info, issue.updated_at)
    if not issue_iter.skipped:
        gc_directory_tree(issue_dir, valid_paths, gc_report)

    # Milestones
    valid_paths = BloomSet()
    milestone_dir = make_dir_path(root_dir, 'milestones')
    valid_paths.add(milestone_dir)
    milestone_iter = cond_iter(milestone_dir, repo.iter_milestones,
            scrub=scrub, state='all')
    for milestone in milestone_iter:
        info = {
            'created_at': timestamp_str(milestone.created_at),
            'creator': user_str(milestone.creator),
            'description': milestone.description,
            'due_on': timestamp_str(milestone.due_on),
            'state': milestone.state,
            'title': milestone.title,
            'updated_at': timestamp_str(milestone.updated_at),
        }
        path = os.path.join(milestone_dir, '%d.json' % milestone.number)
        valid_paths.add(path)
        write_json(path, info, milestone.updated_at)
    if not milestone_iter.skipped:
        gc_directory_tree(milestone_dir, valid_paths, gc_report)


def update_comments(repo, root_dir, scrub=False):
    # Group by commit
    valid_paths = BloomSet()
    comment_dir = make_dir_path(root_dir, 'comments')
    valid_paths.add(comment_dir)
    commit_comments = {}
    commit_timestamps = {}
    comment_iter = cond_iter(comment_dir, repo.iter_comments, scrub=scrub)
    for comment in comment_iter:
        info = {
            'body': comment.body,
            'created_at': timestamp_str(comment.created_at),
            'commit_id': comment.commit_id,
            'line': comment.line,
            'path': comment.path,
            'position': comment.position,
            'updated_at': timestamp_str(comment.updated_at),
            'user': user_str(comment.user),
        }
        commit_id = comment.commit_id
        commit_comments.setdefault(commit_id, []).append(info)
        commit_timestamps[commit_id] = max(comment.updated_at,
                commit_timestamps.get(commit_id, comment.updated_at))
    if not comment_iter.skipped:
        for commit_id in commit_comments:
            path = os.path.join(comment_dir, '%s.json' % commit_id)
            valid_paths.add(path)
            write_json(path, commit_comments[commit_id],
                    commit_timestamps[commit_id])
        gc_directory_tree(comment_dir, valid_paths, gc_report)


def update_releases(repo, root_dir, scrub=False):
    valid_paths = BloomSet()
    releases_dir = make_dir_path(root_dir, 'releases')
    valid_paths.add(releases_dir)
    # Releases response includes asset data, so cond_iter is safe
    release_iter = cond_iter(releases_dir, repo.iter_releases, scrub=scrub)
    for release in release_iter:
        release_dir = make_dir_path(releases_dir, release.tag_name)

        # Metadata
        info = {
            'created_at': timestamp_str(release.created_at),
            'description': release.body,
            'draft': release.draft,
            'name': release.name,
            'published_at': timestamp_str(release.published_at),
            'tag_name': release.tag_name,
        }
        metadata_path = os.path.join(release_dir, 'info.json')
        write_json(metadata_path, info)
        valid_paths.add(metadata_path)

        # Assets
        asset_dir = os.path.join(release_dir, 'assets')
        for asset in release.assets:
            make_dir_path(asset_dir)
            asset_path = os.path.join(asset_dir, asset.name)
            mtime = datetime_to_time_t(asset.updated_at)
            valid_paths.add(asset_path)

            try:
                st = os.stat(asset_path)
                if (not scrub and st.st_mtime == mtime and
                        st.st_size == asset.size):
                    continue
            except OSError:
                pass

            with UpdateFile(asset_path) as fh:
                asset.download(fh)
            if fh.modified:
                print 'f', asset_path
            if os.stat(asset_path).st_mtime != mtime:
                os.utime(asset_path, (mtime, mtime))
            XAttrs(asset_path).update(ATTR_CONTENT_TYPE,
                    asset.content_type.encode('utf-8'))

    # Collect garbage, if anything has changed
    if not release_iter.skipped:
        gc_directory_tree(releases_dir, valid_paths, gc_report)


def sync_repo(repo, root_dir, token, scrub=False, git_path=None):
    make_dir_path(root_dir)

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
            scrub=scrub, git_path=git_path)
    if repo.has_wiki:
        # The wiki repo doesn't necessarily exist, even though the API
        # claims it does.  Ignore errors during initial clone.
        update_git(re.sub('\.git$', '.wiki', repo.clone_url),
                os.path.join(root_dir, 'wiki'), token, scrub=scrub,
                ignore_clone_errors=True, git_path=git_path)

    # Issues
    if repo.has_issues:
        update_issues(repo, root_dir, scrub=scrub)

    # Commit comments
    update_comments(repo, root_dir, scrub=scrub)

    # Releases
    update_releases(repo, root_dir, scrub=scrub)


def sync_org(org, root_dir):
    make_dir_path(root_dir)

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


def get_relroot(organization, repo=None):
    return os.path.join('github', organization,
            repo if repo else '@organization')


def cmd_github_backup(config, args):
    settings = config['settings']
    token = settings['github-token']
    gh = github_login(token=token)

    if args.repo is not None:
        root_dir = os.path.join(settings['root'],
                get_relroot(args.organization, args.repo))
        sync_repo(gh.repository(args.organization, args.repo),
                root_dir, token, scrub=args.scrub,
                git_path=settings.get('github-git-path'))
    else:
        root_dir = os.path.join(settings['root'],
                get_relroot(args.organization))
        sync_org(gh.organization(args.organization), root_dir)

    print gh.ratelimit_remaining, 'requests left in quota'


def cmd_github_ls(config, args):
    settings = config['settings']
    gh = github_login(token=settings['github-token'])
    org = gh.organization(args.organization)
    for repo in sorted(org.iter_repos(), key=lambda r: r.name.lower()):
        print repo.name


def _setup():
    group = make_subcommand_group('github',
            help='low-level GitHub support')

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
    parser.add_argument('-c', '--scrub', action='store_true',
            help='check backup data against original')

    parser = group.add_parser('ls',
            help='list repositories in the specified GitHub organization')
    parser.set_defaults(func=cmd_github_ls)
    parser.add_argument('organization',
            help='organization name')

_setup()


class GitHubUnit(Unit):
    def __init__(self, settings, org, repo=None):
        Unit.__init__(self)
        self.root = get_relroot(org, repo)
        self.backup_args = ['github', 'backup', org]
        if repo:
            self.backup_args.append(repo)
        if random_do_work(settings, 'github-scrub-probability', 0.0166):
            self.backup_args.append('-c')


class GitHubSource(Source):
    LABEL = 'github'

    def get_units(self):
        ret = []
        for org, info in self._manifest.items():
            info = info or {}
            # Dynamically obtain list of repos
            cmd = get_cmdline_for_subcommand(['github', 'ls', org])
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            out, _ = proc.communicate()
            if proc.returncode:
                raise IOError("Couldn't list GitHub repos for %s" % org)
            repos = [r for r in out.split('\n') if r]
            # Update org metadata
            if info.get('organization-metadata', True):
                ret.append(GitHubUnit(self._settings, org))
            # Update repos
            for repo in repos:
                ret.append(GitHubUnit(self._settings, org, repo))
        return ret
