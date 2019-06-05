import networkx as nx
import concurrent.futures
from threading import Lock
from github import Github, NamedUser, Repository
from urllib.error import HTTPError
import argparse


class GithubCrawler:
    def __init__(self, user, password):
        self.client = Github(user, password, retry=5)

    def find(self, query, limit=None):
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
            repos = self.client.search_repositories(query) if isinstance(query, str) else self.client.get_repos()
            if isinstance(limit, int):
                repos = repos[0:limit]
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(import_repo, repos)

        except HTTPError:
            print('Communication error with GitHub. Graph completed prematurely.')

        return G


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze the network of code projects in a code repository.')
    parser.add_argument('-s', '--source', default='GitHub', choices=['GitHub', 'GitLab'], help='The type of repository.')
    parser.add_argument('-u', '--user', help='The user name to use for login. '
                                             'Login is not usually required but can offer advantages.')
    parser.add_argument('-p', '--password', help='THe password to use for login. '
                                                 'Login is not usually required but can offer advantages.')
    parser.add_argument('-l', '--limit', type=int, help='The maximum number of repositories to include in the graph.')
    parser.add_argument('-q', '--query', help='Specify the projects of interest.')
    parser.add_argument('-o', '--output', help='Specify a path to save the resulting graph in GEXF format.')
    args = parser.parse_args()

    #os.environ['https_proxy'] = "http://192.168.43.176:8020"
    c = GithubCrawler(args.user, args.password)
    g = c.find(args.query, limit=args.limit)
    print(list(g))
    if isinstance(args.output, str):
        nx.write_gexf(g, args.output)
