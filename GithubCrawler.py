import concurrent.futures
import sys
import time
from datetime import datetime
from threading import Lock
from urllib.error import HTTPError

import networkx as nx
from github import Github, Repository, NamedUser, PaginatedList, RateLimitExceededException, GithubException


def wait_for_reset(client: Github):
    available, _ = client.rate_limiting
    timestamp = client.rate_limiting_resettime
    if timestamp and available == 0:
        t = timestamp - time.time()
        if t > 0:
            print("Waiting {0} seconds.".format(t))
            time.sleep(t)

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
        repos = self.client.search_repositories(query) if query else self.client.get_repos(since=since)
        if limit:
            repos = repos[:limit]

        wait_for_reset(self.client)

        repo_list = []
        for repo in repos:
            wait_for_reset(self.client)
            repo_list.append(repo)

        return GithubProgressiveIterator(self.client, previous, repo_list, since=since)


class GithubProgressiveIterator:
    completed = False
    lock = Lock()

    def __init__(self, client: Github, original: nx.Graph, repos: [Repository], since: datetime):
        assert isinstance(client, Github)
        assert original is None or isinstance(original, nx.Graph)
        assert isinstance(repos, list)
        assert since is None or isinstance(since, datetime)

        self.client = client
        self.original = original
        self.repos = repos
        self.since = since

    def __iter__(self):
        return self

    def __next__(self) -> nx.Graph:
        g = self.original or nx.Graph()
        graph_lock = Lock()

        wait_for_reset(self.client)

        def import_repo(repo: Repository) -> (Repository, None):
            repo_id = repo.full_name
            if repo_id is None:
                return None

            with graph_lock:
                if repo_id in g:
                    return None
                print('Analyzing repo {0}...'.format(repo.full_name))
                language = repo.language or '?'
                weight = repo.watchers_count or 0
                g.add_node(repo_id, bipartite=0, language=language, weight=weight)

            if repo.fork and repo.parent is not None:
                parent_id = import_repo(repo.parent)
                if parent_id:
                    with graph_lock:
                        # g.add_edge(repo_id, parent_id)
                        pass

            def link_user(user: NamedUser, relation: str = None):
                user_id = user.login
                if user_id is None:
                    return
                with graph_lock:
                    if user_id not in g:
                        g.add_node(user_id, bipartite=1)
                    g.add_edge(user_id, repo_id, relation=relation)

            try:
                if self.since is None:
                    if repo.owner is not None:
                        link_user(repo.owner, relation='owner')

                    contributors = repo.get_contributors()
                    for user in contributors:
                        link_user(user, relation='contributor')
                else:
                    commits = repo.get_commits(since=self.since)
                    for commit in commits:
                        link_user(commit.author, relation="committer")
            except Exception:
                with graph_lock:
                    g.remove_node(repo_id)
                raise

            return repo

        try:
            print('Finding more repositories.')
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                workers = (executor.submit(import_repo, worker) for worker in self.repos)

                for worker in concurrent.futures.as_completed(workers):
                    with self.lock:
                        self.repos.remove(worker.result())
            self.completed = True

        except HTTPError:
            print(sys.exc_info())
            print('Communication error with GitHub. Graph completed prematurely.')
        except RateLimitExceededException:
            print(sys.exc_info())
            print('The GitHub rate limit was triggered. Please try again later. '
                  'See https://developer.github.com/v3/#abuse-rate-limits')
        except GithubException:
            print(sys.exc_info())
            print('Communication error with GitHub. Graph completed prematurely.')

        return g
