import concurrent.futures
import sys
from datetime import datetime
from threading import Lock
from urllib.error import HTTPError

import networkx as nx
from gitlab import Gitlab, GitlabGetError


class GitlabCrawler:
    def __init__(self, url: str, user: str, password: str):
        assert url is None or isinstance(url, str)
        assert user is None or isinstance(user, str)
        assert password is None or isinstance(password, str)

        self.client = Gitlab(url or 'https://gitlab.com/', email=user, password=password)
        if user and password:
            self.client.auth()

    def find(self, query: str, limit: int = None, since: datetime = None, previous: nx.Graph = None):
        assert query is None or isinstance(query, str)
        assert limit is None or isinstance(limit, int) and limit >= 0
        assert since is None or isinstance(since, datetime)
        assert previous is None or isinstance(previous, nx.Graph)

        print('Finding repositories with "{0}"'.format(query or "NO QUERY"))
        g = previous or nx.Graph()
        graph_lock = Lock()
        completed = False
        query_params = dict()
        if since:
            query_params['from'] = repr(since)
        if query:
            query_params['search'] = query
        repos = self.client.projects
        page = 1
        page_repos = []
        count = 0
        while not completed:
            obey_rate_limit = True

            def link_user(repo_id: str, user_id: str, **attr):
                if user_id is None:
                    return
                with graph_lock:
                    if user_id not in g:
                        g.add_node(user_id, bipartite=1)
                    g.add_edge(user_id, repo_id, **attr)

            def import_repo(repo):
                repo_id = repo.path_with_namespace
                if repo_id is None:
                    return repo

                with graph_lock:
                    if repo_id in g:
                        return repo

                #if repo.fork and repo.parent and repo.parent.path_with_namespace and repo.owner:
                #    import_repo(repo.parent)
                #    link_user(repo.parent.path_with_namespace, repo.namespace['path'], relation='fork', fork_source=repo_id)

                languages = repo.languages()
                language = sorted(languages.items(), key=lambda t: t[1], reverse=True)[0][0] \
                    if len(languages) > 0 else '?'
                weight = repo.star_count or 0
                with graph_lock:
                    g.add_node(repo_id, bipartite=0, language=language, weight=weight)

                try:
                    if since is None:
                        if repo.namespace is not None:
                            link_user(repo_id, repo.namespace['path'], relation='owner')

                        contributors = repo.repository_contributors(all=True, obey_rate_limit=False)
                        for user in contributors:
                            link_user(repo_id, user['email'], relation='contributor')
                    else:
                        commits = repo.get_commits(since=since)
                        for commit in commits:
                            link_user(repo_id, commit.author.login or commit.author.email, relation="committer")
                except GitlabGetError:
                    with graph_lock:
                        g.remove_node(repo_id)
                    error: GitlabGetError
                    _, error, _ = sys.exc_info()
                    if error.response_code == 429:
                        raise
                except Exception:
                    raise

                return repo

            try:
                print('Finding more repositories.')
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                    while not limit or count < limit:
                        if not page_repos:
                            page_repos = repos.list(page=page, per_page=30, obey_rate_limit=obey_rate_limit,
                                                    query_parameters=query_params)
                            obey_rate_limit = False
                            page += 1
                            if len(page_repos) == 0:
                                break

                        workers = (executor.submit(import_repo, worker) for worker in page_repos)

                        for worker in concurrent.futures.as_completed(workers):
                            repo = worker.result()
                            if repo is not None:
                                print('Analyzed repo {0}.'.format(repo.path_with_namespace or '?'))
                                page_repos.remove(repo)
                                count += 1
                completed = True

            except HTTPError:
                completed = True
                print(sys.exc_info())
                print('Communication error with GitHub. Graph completed prematurely.')
            except GitlabGetError:
                error: GitlabGetError
                _, error, _ = sys.exc_info()
                print(error)
                if error.response_code == 429:
                    print('The GitLab rate limit was triggered. Please try again later. ')

            yield g