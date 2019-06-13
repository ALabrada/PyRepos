import concurrent.futures
import sys
import time
from datetime import datetime
from threading import Lock
from urllib.error import HTTPError

import networkx as nx
from github import Github, Repository, NamedUser, RateLimitExceededException, GithubException


class GithubCrawler:
    offset = 0
    completed = False

    def __init__(self, user: str, password: str):
        assert user is None or isinstance(user, str)
        assert password is None or isinstance(password, str)

        self.client = Github(user, password, retry=5)

    def reset(self):
        self.offset = 0

    def find_all(self, query: str, limit: int = None, since: datetime = None, previous: nx.Graph = None,
                 wait_time: int = 0):
        assert isinstance(wait_time, int)

        g = previous
        previous_count = nx.number_of_nodes(g) if isinstance(g, nx.Graph) else 0
        timestamp = None
        while not self.completed:
            if timestamp is not None:
                t = timestamp - time.time()
                if t > 0:
                    print("Waiting {0} seconds.".format(t))
                    time.sleep(t)
            g = self.find(query, limit=limit, since=since, previous=g)
            limit = None if limit is None else max(0, limit - nx.number_of_nodes(g) + previous_count)
            timestamp = self.client.rate_limiting_resettime
            if wait_time:
                timestamp = max(time.time() + wait_time, timestamp)
            yield g

    def find(self, query: str, limit: int = None, since: datetime = None, previous: nx.Graph = None) -> nx.Graph:
        assert query is None or isinstance(query, str)
        assert limit is None or isinstance(limit, int) and limit >= 0
        assert since is None or isinstance(since, datetime)
        assert previous is None or isinstance(previous, nx.Graph)

        g = previous or nx.Graph()
        graph_lock = Lock()

        def import_repo(repo: Repository) -> (str, None):
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
                if since is None:
                    if repo.owner is not None:
                        link_user(repo.owner, relation='owner')

                    contributors = repo.get_contributors()
                    for user in contributors:
                        link_user(user, relation='contributor')
                else:
                    commits = repo.get_commits(since=since)
                    for commit in commits:
                        link_user(commit.author, relation="committer")
            except Exception:
                with graph_lock:
                    g.remove_node(repo_id)
                raise

            return repo.full_name

        try:
            print('Finding repositories with "{0}"'.format(query or "NO QUERY"))
            repos = self.client.search_repositories(query) if query else self.client.get_repos(since=since)
            if self.offset:
                repos = repos[self.offset:]
            if limit:
                repos = repos[:limit]
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for repo_id in executor.map(import_repo, repos):
                    self.offset += 1
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