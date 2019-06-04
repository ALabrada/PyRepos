import networkx as nx
import concurrent.futures
from threading import Lock
from github import Github, NamedUser, Repository
from urllib.error import HTTPError
import os
import sys


class GithubCrawler:
    def __init__(self, user, password, query=None):
        self.client = Github(user, password, retry=5)
        self.query = query

    def find(self):
        G = nx.Graph()
        users = set()
        users_lock = Lock()
        graph_lock = Lock()

        def import_repo(repo):
            with graph_lock:
                G.add_node(repo.full_name, type='repo')
            print('Analyzing repo {0}...'.format(repo.full_name))
            contributors = repo.get_contributors()

            for user in contributors:
                with users_lock:
                    if user.login not in users:
                        users.add(user.login)
                        with graph_lock:
                            G.add_node(user.login, type='user')
                with graph_lock:
                    G.add_edge(repo.full_name, user.login)

        try:
            repos = self.client.search_repositories(self.query) if isinstance(self.query, str) else self.client.get_repos()
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(import_repo, repos)

        except HTTPError:
            print('Communication error with GitHub. Graph completed prematurely.')

        return G


if __name__ == "__main__":
    #os.environ['https_proxy'] = "http://192.168.43.176:8020"
    c = GithubCrawler(sys.argv[1], sys.argv[2], sys.argv[3])
    print(list(c.find()))