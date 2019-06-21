import networkx as nx
import itertools
import argparse
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib import cm, colors

from GithubCrawler import GithubCrawler


def analize_graph(g: nx.Graph, limit: int = 3, clean: bool = True, draw: bool = False):
    assert isinstance(g, nx.Graph)
    assert isinstance(limit, int)

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
        components = sorted(components, key=lambda c: len(c))
        min_size = len(components[-1])/2

        for component in components:
            component_repos = repos.intersection(component)
            if len(component_repos) <= 5 or len(component) < min_size:
                repos.difference_update(component)
                users.difference_update(component)
                # g.remove_nodes_from(component)

        if len(repos) < repo_count:
            print('Excluded {0} isolated projects.'.format(repo_count - len(repos)))
            # g = nx.classes.graphviews.subgraph_view(g, filter_node=lambda n: n in repos or n in users)
            g = nx.subgraph(g, repos.union(users))

    labels = set()
    if limit:
        bridges = {(n1, n2): len(next((c for c in components if n1 in c), [])) for n1, n2 in nx.algorithms.bridges(g)}
        bridges = take_by_value(bridges.items(), limit)
        print('Connecting memberships: \n{0}'.format(list(bridges)))

        deg1_repos = [n for n in repos if g.degree[n] <= 1]
        print('Number of risked projects: \n{0}'.format(len(deg1_repos)))
        deg1_repos = sorted(deg1_repos, key=lambda n: -nodes[n].get('weight', 0))
        print('Most risked projects: \n{0}'.format(deg1_repos[0:limit]))

        repo_centrality = nx.algorithms.bipartite.degree_centrality(g, repos)
        repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
        labels.update(repo_centrality)
        print('Most popular projects: \n{0}'.format(repo_centrality))

        repo_centrality = nx.algorithms.bipartite.closeness_centrality(g, repos, normalized=True)
        repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
        labels.update(repo_centrality)
        print('Most relatable projects: \n{0}'.format(repo_centrality))

        repo_centrality = nx.algorithms.bipartite.betweenness_centrality(g, repos)
        repo_centrality = take_by_value(repo_centrality.items(), limit, f=lambda t: t[0] in repos)
        labels.update(repo_centrality)
        print('Most connecting projects: \n{0}'.format(repo_centrality))

        user_centrality = nx.algorithms.bipartite.degree_centrality(g, users)
        user_centrality = take_by_value(user_centrality.items(), limit, f=lambda t: t[0] in users)
        labels.update(user_centrality)
        print('Most active users: \n{0}'.format(user_centrality))

        user_centrality = nx.algorithms.bipartite.betweenness_centrality(g, users)
        user_centrality = take_by_value(user_centrality.items(), limit, f=lambda t: t[0] in users)
        labels.update(user_centrality)
        print('Most connecting users: \n{0}'.format(user_centrality))

    if draw:
        draw_communities(g, labels=list(labels))


def draw_communities(G: nx.Graph, labels=None):
    assert isinstance(G, nx.Graph)
    assert labels is None or isinstance(labels, list)

    labels = {n: n for n in labels} if labels else {}

    print('Drawing graph...')
    pos = nx.spring_layout(G)
    nodes = G.nodes(data=True)
    repos = {n for n, d in nodes if d['bipartite'] == 0}
    users = set(G) - repos
    norm = colors.Normalize(vmin=0, vmax=2)
    node_list = list(G.nodes)
    color_list = [cm.jet(norm(d['bipartite'])) for n, d in nodes]
    size_list = [1 if d['bipartite'] else 5 for n, d in nodes]

    fig, ax = plt.subplots(figsize=(16, 9))
    plt.title("Github repositories")
    nx.draw_networkx(G, pos=pos, nodelist=node_list, node_color=color_list, node_size=size_list,
                     with_labels=True, labels=labels, ax=ax, edge_color='0.5')
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze the network of code projects in a code repository.')
    parser.add_argument('-i', '--input', help='Path of a previously saved graph in GEXF format.')
    parser.add_argument('-c', '--continue', dest='scan', action='store_true',
                        help='Include more repositories from the search results. '
                             'It is ignored if there is no input graph.')
    parser.add_argument('--stats', type=int, help='Specify the amount of results to display in graph analysis. '
                                        'Use 0 to disable graph analysis.')
    parser.add_argument('--draw', dest='draw', action='store_true', help='Draw the resulting graph.')
    parser.add_argument('-s', '--source', default='GitHub', choices=['GitHub', 'GitLab'],
                        help='The type of repository.')
    parser.add_argument('-u', '--user', help='The user name to use for login. '
                                             'Login is not usually required but can offer advantages.')
    parser.add_argument('-p', '--password', help='The password to use for login. '
                                                 'Login is not usually required but can offer advantages.')
    parser.add_argument('-l', '--limit', type=int, default=1000,
                        help='The maximum number of repositories to include in the graph.')
    parser.add_argument('-q', '--query', help='Specify the projects of interest.')
    parser.add_argument('-d', '--date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help='Starting date.')
    parser.add_argument('-o', '--output', help='Specify a path to save the resulting graph in GEXF format.')
    args = parser.parse_args()

    c = GithubCrawler(args.user, args.password)

    g = None if args.input is None else nx.read_gexf(args.input)
    if g is None or nx.number_of_nodes(g) == 0 or args.scan:
        for g in c.find(args.query, limit=args.limit, since=args.date, previous=g):
            if args.output:
                nx.write_gexf(g, args.output)
            analize_graph(g, limit=args.stats, draw=args.draw)
    else:
        analize_graph(g, limit=args.stats, draw=args.draw)