import toml

import sql_to_odata


with open('pyproject.toml') as project_file:
    project = toml.load(project_file)


def test_version():
    version = project['tool']['poetry']['version']
    assert sql_to_odata.__version__ == version
