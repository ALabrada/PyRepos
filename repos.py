import networkx as nx
import concurrent.futures
from threading import Lock
from github import Github, NamedUser, Repository
from urllib.error import HTTPError
import os
import sys


class GithubCrawler:
    def __init__(self, user, password, seed=None):
        self.client = Github(user, password, retry=5)
        self.seed = [self.client.get_user()] if seed is None or seed is list and len(seed) == 0 else seed

    def find(self):
        G = nx.Graph()
        users = set()
        user_map = dict()
        users_lock = Lock()
        graph_lock = Lock()
        queue_lock = Lock()

        queue = list(self.client.get_repo(x) for x in self.seed)
        try:
            while len(queue) > 0:
                repo = queue.pop()
                contributors = repo.get_contributors()
                user_map[repo.full_name] = {user.node_id for user in contributors}
                G.add_node(repo.full_name)
                print('Analyzing repo {0}...'.format(repo.full_name))

                def create_edges_from(user):
                    with users_lock:
                        if user.node_id in users:
                            return
                        users.add(user.node_id)

                    user_repos = user.get_repos()
                    try:
                        for other_repo in user_repos:
                            if other_repo.full_name in user_map:
                                w = len(user_map[repo.full_name].intersection(user_map[other_repo.full_name]))
                                with graph_lock:
                                    G.add_edge(repo.full_name, other_repo.full_name, weight=1 / w)
                            else:
                                with queue_lock:
                                    queue.append(other_repo)
                    except:
                        print('Could not get the repositories of user {0}: {1}'.format(user.node_id, sys.exc_info()[0]))

                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    executor.map(create_edges_from, contributors)

        except HTTPError:
            print('Communication error with GitHub. Graph completed prematurely.')

        return G


if __name__ == "__main__":
    #os.environ['https_proxy'] = "http://192.168.43.176:8020"
    c = GithubCrawler(sys.argv[0], sys.argv[1], ["cocoapods/cocoapods"])
    print(list(c.find()))