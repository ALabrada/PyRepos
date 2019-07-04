# RepoNetwork
A python based tool that analyses the network structure of the repositories stored in Version Control stores such as 
GitHub and GitLab.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install RepoNetwork.

```bash
pip install reponetwork
```

## Usage

You can execute the script from the command line or import the package in python to extend it's functionalities.

### Examples

```bash
reponet -t MY_AUTH_TOKEN -q MVC -o path/to/file.gexf
```

Build the network of repositories in Github related to MVC and save the resulting graph in your computer at the 
specified path. Authenticate with the specified personal private token from your GitHub account. 
Authentication is not required, but encouraged as it allows performing more API requests per hour. 
See [Gihub rate limit](https://developer.github.com/v3/#rate-limiting).

```bash
reponet -s https://gitlab.com/ -t MY_AUTH_TOKEN -q MVC --stats 1
```

Build the network of repositories in the main [GitLab](https://gitlab.com/) server related to MVC and analyze 
the results, showing only the best candidate for each analysis criteria. Authenticate with the specified personal 
private token from your GitLab account. Authentication is not required, but encouraged as GitLab does not provide
enough information in un-authenticated queries and thus, the resulting network may be incomplete. 

```bash
reponet -u USER_NAME -p PASSWORD -q MVC -o path/to/file.gexf --stats 1 --draw --since 2015-01-01
```

Build the network of repositories in Github server related to MVC and then analyse, draw and save the resulting graphs. 
Authenticate with the user name and password from your Github account. Include only repositories and interactions 
since the beginning of 2015. The graph will be saved and analysed after exceeding rate limits, so you can wait or 
stop the process without loosing information.

```bash
reponet -i path/to/file.gexf --stats 1 --draw --since 2015-01-01
```

Load a previously saved network graph and then analyse and draw a visual representation of it. 
Include only repositories and interactions since the beginning of 2015. 

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)