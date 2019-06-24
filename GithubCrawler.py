import concurrent.futures
import sys
import time
from datetime import datetime
from threading import Lock
from urllib.error import HTTPError

import networkx as nx
from github import Github, Repository, NamedUser, Commit, PaginatedList, RateLimitExceededException, GithubException


def wait_for_reset(client: Github):
    available, _ = client.rate_limiting
    timestamp = client.rate_limiting_resettime
    if timestamp and available == 0:
        t = timestamp - time.time()
        while t > 0:
            mins, secs = divmod(t, 60)
            time_format = 'Waiting {:02.0f}:{:02.0f}'.format(mins, secs)
            print(time_format, end='\r')
            time.sleep(1)
            t = timestamp - time.time()


class GithubCrawler:
    def __init__(self, user: str, password: str):
        assert user is None or isinstance(user, str)
        assert password is None or isinstance(password, str)

        self.client = Github(user, password, retry=5)

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

            def import_repo(repo: Repository) -> (Repository, None):
                repo_id = repo.full_name
                if repo_id is None:
                    return repo

                with graph_lock:
                    if repo_id in g:
                        return repo
                    # print('Analyzing repo {0}...'.format(repo.full_name))
                    language = repo.language or '?'
                    weight = repo.watchers_count or 0
                    g.add_node(repo_id, bipartite=0, language=language, weight=weight)

                if repo.fork and repo.parent is not None:
                    parent_id = import_repo(repo.parent)
                    if parent_id:
                        with graph_lock:
                            # g.add_edge(repo_id, parent_id)
                            pass

                def link_user(user_id: str, relation: str = None):
                    if user_id is None:
                        return
                    with graph_lock:
                        if user_id not in g:
                            g.add_node(user_id, bipartite=1)
                        g.add_edge(user_id, repo_id, relation=relation)

                try:
                    if since is None:
                        if repo.owner is not None:
                            link_user(repo.owner.login, relation='owner')

                        contributors = repo.get_contributors()
                        for user in contributors:
                            link_user(user.login or user.email, relation='contributor')
                    else:
                        commits = repo.get_commits(since=since)
                        for commit in commits:
                            link_user(commit.author.login or commit.author.email, relation="committer")
                except RateLimitExceededException:
                    with graph_lock:
                        g.remove_node(repo_id)
                    raise
                except GithubException:
                    with graph_lock:
                        g.remove_node(repo_id)
                except Exception:
                    raise

                return repo

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
                            repo: Repository = worker.result()
                            if repo is not None:
                                print('Analyzed repo {0}.'.format(repo.full_name or '?'))
                                page_repos.remove(repo)
                                count += 1
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