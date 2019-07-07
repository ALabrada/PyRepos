import concurrent.futures
import sys
import time
import dateutil
from datetime import datetime
from threading import Lock
from urllib.error import HTTPError

import networkx as nx
from github import Github, Repository, NamedUser, Commit, PaginatedList, RateLimitExceededException, GithubException


def wait_for_reset(client: Github):
    available, _ = client.rate_limiting
    timestamp = client.rate_limiting_resettime
    if timestamp and available == 0:
        print('Waiting for rate limit reset...')
        t = timestamp - time.time()
        while t > 0:
            mins, secs = divmod(t, 60)
            time_format = 'Remaining {:02.0f}:{:02.0f}'.format(mins, secs)
            print(time_format, end='\r')
            time.sleep(1)
            t = timestamp - time.time()


class GithubCrawler:
    def __init__(self, token: str, user: str, password: str):
        assert user is None or isinstance(user, str)
        assert password is None or isinstance(password, str)
        assert token is None or isinstance(token, str)

        self.client = Github(login_or_token=token or user, password=password, retry=5, per_page=100)

    def find(self, query: str, limit: int = None, since: datetime = None, previous: nx.Graph = None):
        assert query is None or isinstance(query, str)
        assert limit is None or isinstance(limit, int) and limit >= 0
        assert since is None or isinstance(since, datetime)
        assert previous is None or isinstance(previous, nx.Graph)

        print('Finding repositories with "{0}"'.format(query or "NO QUERY"))
        g = previous or nx.Graph()
        graph_lock = Lock()
        completed = False
        repos: PaginatedList = self.client.search_repositories(query) if query else self.client.get_repos(since=since)
        page = 1
        page_repos = []
        count = 0
        while not completed:
            wait_for_reset(self.client)

            def link_user(repo_id: str, user_id: str, **attr):
                if user_id is None:
                    return
                with graph_lock:
                    if user_id not in g:
                        g.add_node(user_id, bipartite=1)
                    if (user_id, repo_id) not in g.edges:
                        g.add_edge(user_id, repo_id, **attr)

            def import_repo(repo: Repository):
                repo_id = repo.full_name
                if repo_id is None:
                    return repo, []

                with graph_lock:
                    if repo_id in g:
                        return repo, []

                if repo.fork and repo.parent and repo.parent.full_name and repo.owner:
                    import_repo(repo.parent)
                    link_user(repo.parent.full_name, repo.owner.login, relation='fork', fork_source=repo_id, date=repo.created_at.isoformat())

                language = repo.language or '?'
                weight = repo.watchers_count or 0
                with graph_lock:
                    g.add_node(repo_id, bipartite=0, language=language, weight=weight, date=repo.created_at.isoformat())

                repo_forks = []
                try:
                    if since is None:
                        if repo.owner is not None:
                            link_user(repo_id, repo.owner.login, relation='owner', date=repo.pushed_at.isoformat())

                        contributors = repo.get_contributors()
                        for user in contributors:
                            link_user(repo_id, user.login or user.email, relation='contributor')
                    else:
                        commits = [x for x in repo.get_commits(since=since) if x.author and x.commit.author]
                        commits = sorted(commits, key=lambda x: x.commit.author.date)
                        for commit in commits:
                            date: datetime = commit.commit.author.date
                            link_user(repo_id, commit.author.login or commit.author.email, relation="committer", date=date.isoformat())

                    repo_forks = list(repo.get_forks())
                except RateLimitExceededException:
                    with graph_lock:
                        g.remove_node(repo_id)
                    raise
                except GithubException:
                    with graph_lock:
                        g.remove_node(repo_id)
                except Exception:
                    raise

                return repo, repo_forks

            try:
                print('Finding more repositories.')
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                    while not limit or count < limit:
                        if not page_repos:
                            page_repos = repos.get_page(page)
                            page += 1
                            if len(page_repos) == 0:
                                break

                        workers = (executor.submit(import_repo, worker) for worker in page_repos)

                        for worker in concurrent.futures.as_completed(workers):
                            repo: Repository
                            repo_forks: [Repository]
                            repo, repo_forks = worker.result()
                            if repo is not None:
                                print('Analyzed repo {0}.'.format(repo.full_name or '?'))
                                page_repos.remove(repo)
                                count += 1
                                page_repos.extend(repo_forks)
                completed = True

            except HTTPError:
                completed = True
                print(sys.exc_info())
                print('Communication error with GitHub. Graph completed prematurely.')
            except RateLimitExceededException:
                print(sys.exc_info())
                print('The GitHub rate limit was triggered. Please try again later. '
                      'See https://developer.github.com/v3/#abuse-rate-limits')
            except GithubException:
                completed = True
                print(sys.exc_info())
                print('Communication error with GitHub. Graph completed prematurely.')

            yield g