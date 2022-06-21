# common

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.8](https://img.shields.io/badge/python-3.8-green?style=for-the-badge)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/pilot-platform-common?style=for-the-badge)](https://pypi.org/project/pilot-platform-common/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/pilotdataplatform/common/Run%20Tests?style=for-the-badge)](https://github.com/PilotDataPlatform/common/actions)
[![codecov](https://img.shields.io/codecov/c/github/PilotDataPlatform/common?style=for-the-badge)](https://codecov.io/gh/PilotDataPlatform/common)


Importable Pip package that generates entity ID and connects with Vault (secret engine) to retrieve credentials.

## Import package
Pip install command from GitLab:
```
pip install common --index-url https://__token__:<GITLAB_PAT>@git.indocresearch.org/api/v4/projects/158/packages/pypi/simple
```

Pip install command from `.whl` file:
```
pip install common-<VERSION>-py3-none-any.whl
```

In `requirements.txt`:
```
--index-url https://__token__:<GITLAB_PAT>@git.indocresearch.org/api/v4/projects/158/packages/pypi/simple
common
```

`<GITLAB_PAT>` is a GitLab personal access token with the `read_api` scope.

## Update package
Refer to documentation: https://docs.gitlab.com/ee/user/packages/pypi_repository/#publish-a-pypi-package-by-using-twine

1. Update the package version in `setup.py`. Otherwise, the push will fail with duplicate version number.

2. Install the python `build` and `twine` package:
```
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine
```

3. Build the common package:
```
python3 -m build
```

Now you will see two more folder generated under ./common/
```
|- common.egg-info/
|- dist/
```

4. Push to GitLab:
```
TWINE_PASSWORD=<PASS>  TWINE_USERNAME=<USER> python3 -m twine upload --repository-url https://git.indocresearch.org/api/v4/projects/158/packages/pypi dist/*
```
