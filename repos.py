import networkx as nx
import concurrent.futures
from threading import Lock
from github import Github, NamedUser, Repository, Commit
from urllib.error import HTTPError
import argparse
import datetime


class GithubCrawler:
    def __init__(self, user, password):
        self.client = Github(user, password, retry=5)

    def find(self, query, limit=None, since=None):
        G = nx.DiGraph()
        graph_lock = Lock()

        def import_repo(repo):
            if repo in G:
                return
            if repo.fork and repo.parent is not None and repo.parent.full_name not in G:
                import_repo(repo.parent)

            print('Analyzing repo {0}...'.format(repo.full_name))

            with graph_lock:
                G.add_node(repo.full_name, type='repo', language=repo.language)
                if repo.fork and repo.parent is not None:
                    G.add_edge(repo.full_name, repo.parent.full_name)

            def link_user(user, relation=None):
                with graph_lock:
                    if user.login not in G:
                        G.add_node(user.login, type='user')
                    G.add_edge(user.login, repo.full_name, relation=relation)

            if repo.owner is not None:
                link_user(repo.owner, relation='owner')

            contributors = repo.get_contributors()
            for user in contributors:
                link_user(user, relation='contributor')

            commits = repo.get_commits() if since is None else repo.get_commits(since=since)
            for commit in commits:
                with graph_lock:
                    commit_id = '{0}/commit/{1}'.format(repo.full_name, commit.sha)
                    G.add_node(commit_id, type='commit')
                    G.add_edge(commit_id, repo.full_name)
                    if commit.author is None: continue
                    if commit.author.login not in G:
                        G.add_node(commit.author.login, type='user')
                    G.add_edge(commit_id, commit.author.login)

        try:
            repos = self.client.search_repositories(query) if isinstance(query, str) else self.client.get_repos(since=since)
            if isinstance(limit, int):
                repos = repos[0:limit]
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(import_repo, repos)

        except HTTPError:
            print('Communication error with GitHub. Graph completed prematurely.')

        return G


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze the network of code projects in a code repository.')
    parser.add_argument('-s', '--source', default='GitHub', choices=['GitHub', 'GitLab'],
                        help='The type of repository.')
    parser.add_argument('-u', '--user', help='The user name to use for login. '
                                             'Login is not usually required but can offer advantages.')
    parser.add_argument('-p', '--password', help='The password to use for login. '
                                                 'Login is not usually required but can offer advantages.')
    parser.add_argument('-l', '--limit', type=int, help='The maximum number of repositories to include in the graph.')
    parser.add_argument('-q', '--query', help='Specify the projects of interest.')
    parser.add_argument('-d', '--date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
                        help='Starting date.')
    parser.add_argument('-o', '--output', help='Specify a path to save the resulting graph in GEXF format.')
    args = parser.parse_args()

    #os.environ['https_proxy'] = "http://192.168.43.176:8020"
    c = GithubCrawler(args.user, args.password)
    g = c.find(args.query, limit=args.limit, since=args.date)
    print(list(g))
    if isinstance(args.output, str):
        nx.write_gexf(g, args.output)
