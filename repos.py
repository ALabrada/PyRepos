import networkx as nx
import concurrent.futures
from threading import Lock
from github import Github, GithubException, RateLimitExceededException, NamedUser, Repository, Commit
from urllib.error import HTTPError
import argparse
import datetime


class GithubCrawler:
    def __init__(self, user, password):
        self.client = Github(user, password, retry=5)

    def find(self, query, limit=None, since=None, previous=None):
        g = previous if isinstance(previous, nx.Graph) else nx.Graph()
        graph_lock = Lock()

        def import_repo(repo):
            repo_id = repo.full_name
            if repo_id is None or repo_id in g:
                return
            if repo.fork and repo.parent is not None and repo.parent.full_name not in g:
                import_repo(repo.parent)

            with graph_lock:
                print('Analyzing repo {0}...'.format(repo.full_name))
                language = repo.language or '?'
                downloads = repo.downloads or 0
                g.add_node(repo_id, bipartite=0, language=language, downloads=downloads)
                if repo.fork and repo.parent is not None:
                    #g.add_edge(repo_id, repo.parent.full_name)
                    pass

            def link_user(user, relation=None):
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

        try:
            repos = self.client.search_repositories(query) if isinstance(query, str) else self.client.get_repos(since=since)
            if isinstance(limit, int):
                repos = repos[0:limit]
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(import_repo, repos)

        except HTTPError:
            print('Communication error with GitHub. Graph completed prematurely.')
        except RateLimitExceededException:
            print('The GitHub rate limit was triggered. Please try again later. '
                  'See https://developer.github.com/v3/#abuse-rate-limits')
        except GithubException:
            print('Communication error with GitHub. Graph completed prematurely.')

        return g


def analize_graph(g, limit=1):
    nodes = g.nodes(data=True)
    repos = {n for n, d in nodes if d['bipartite'] == 0}
    print('Repositories: {0}'.format(len(repos)))
    users = set(g) - repos
    print('Users: {0}'.format(len(users)))
    print('Connected components: {0}'.format(sum(1 for _ in nx.connected_components(g))))
    popular_repos = sorted(repos, key=lambda n: -g.degree[n])
    print('Most popular projects: {0}'.format({n: g.degree[n] for n in popular_repos[0:limit]}))
    deg1_repos = [n for n in repos if g.degree[n] <= 1]
    print('Number of risked projects: {0}'.format(len(deg1_repos)))
    deg1_repos = sorted(deg1_repos, key=lambda n: -nodes[n].get('downloads', 0))
    print('Most risked projects: {0}'.format(deg1_repos[0:limit]))
    active_users = sorted(users, key=lambda u: -g.degree[u])
    print('Most active users: {0}'.format({u: g.degree[u] for u in active_users[0:limit]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze the network of code projects in a code repository.')
    parser.add_argument('-i', '--input', help='Path of a previously saved graph in GEXF format.')
    parser.add_argument('-c', '--continue', dest='scan', action='store_true',
                        help='Include more repositories from the search results. '
                             'It is ignored if there is no input graph.')
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
    g = None if args.input is None else nx.read_gexf(args.input)
    if nx.number_of_nodes(g) == 0 or args.scan:
        g = c.find(args.query, limit=args.limit, since=args.date, previous=g)
    analize_graph(g)
    if args.output:
        nx.write_gexf(g, args.output)
