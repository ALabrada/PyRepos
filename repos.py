import networkx as nx
import itertools
import concurrent.futures
from threading import Lock
from github import Github, GithubException, RateLimitExceededException, NamedUser, Repository, Commit
from urllib.error import HTTPError
import argparse
from datetime import datetime, timedelta
import time
import sys


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


def analize_graph(g: nx.Graph, limit: int = 3, clean: bool = True):
    assert isinstance(g, nx.Graph)
    assert isinstance(limit, int) and limit >= 0
    assert isinstance(clean, bool)

    def take_by_value(items, l, f=None):
        items = sorted(items, key=lambda t: -t[1])
        if f is not None:
            items = filter(f, items)
        return [k for k, v in itertools.islice(items, 0, l)]

    print('Graph analysis:')
    nodes = g.nodes(data=True)
    repos = {n for n, d in nodes if d['bipartite'] == 0}
    print('Repositories: {0}'.format(len(repos)))
    users = set(g) - repos
    print('Users: {0}'.format(len(users)))
    components = list(nx.connected_components(g))
    print('Connected components: \n{0}'.format(sum(1 for _ in components)))

    if clean:
        repo_count = len(repos)
        for component in components:
            component_repos = repos.intersection(component)
            if len(component_repos) == 1:
                repos.difference_update(component)
                users.difference_update(component)
                # g.remove_nodes_from(component)

        if len(repos) < repo_count:
            print('Excluded {0} isolated projects.'.format(repo_count - len(repos)))
            g = nx.classes.graphviews.subgraph_view(g, filter_node=lambda n: n in repos or n in users)

    bridges = {(n1, n2): len(next((c for c in components if n1 in c), [])) for n1, n2 in nx.algorithms.bridges(g)}
    bridges = take_by_value(bridges.items(), limit)
    print('Connecting memberships: \n{0}'.format(list(bridges)))

    deg1_repos = [n for n in repos if g.degree[n] <= 1]
    print('Number of risked projects: \n{0}'.format(len(deg1_repos)))
    deg1_repos = sorted(deg1_repos, key=lambda n: -nodes[n].get('weight', 0))
    print('Most risked projects: \n{0}'.format(deg1_repos[0:limit]))

    repo_centrality = nx.algorithms.bipartite.degree_centrality(g, repos)
    repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
    print('Most popular projects: \n{0}'.format(repo_centrality))

    repo_centrality = nx.algorithms.bipartite.closeness_centrality(g, repos, normalized=True)
    repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
    print('Most relatable projects: \n{0}'.format(repo_centrality))

    repo_centrality = nx.algorithms.bipartite.betweenness_centrality(g, repos)
    repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
    print('Most connecting projects: \n{0}'.format(repo_centrality))

    user_centrality = nx.algorithms.bipartite.degree_centrality(g, users)
    user_centrality = take_by_value(user_centrality.items(), limit, f=lambda t: t[0] in users)
    print('Most active users: \n{0}'.format(user_centrality))

    user_centrality = nx.algorithms.bipartite.betweenness_centrality(g, users)
    user_centrality = take_by_value(user_centrality.items(), limit, f=lambda t: t[0] in users)
    print('Most connecting users: \n{0}'.format(user_centrality))


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
    parser.add_argument('-d', '--date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help='Starting date.')
    parser.add_argument('-o', '--output', help='Specify a path to save the resulting graph in GEXF format.')
    args = parser.parse_args()

    c = GithubCrawler(args.user, args.password)
    g = None if args.input is None else nx.read_gexf(args.input)
    if g is None or nx.number_of_nodes(g) == 0 or args.scan:
        for g in c.find_all(args.query, limit=args.limit, since=args.date, previous=g):
            if args.output:
                nx.write_gexf(g, args.output)
            analize_graph(g)
    else:
        analize_graph(g)