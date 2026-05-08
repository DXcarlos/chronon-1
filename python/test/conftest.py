
#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import os
import sys

import pytest


# conftest.py or test_file.py

import pytest
import sys
import copy

@pytest.fixture(scope="module",autouse=True)
def clean_imports_and_path():
    original_path = copy.copy(sys.path)
    modules_to_clean = ['joins', 'staging_queries', 'group_bys', 'models', "inferences"]
    yield
    sys.path = original_path
    to_clean = [mod_name for mod_name in sys.modules if any([mod_name.startswith(m) for m in modules_to_clean])]
    for mod in to_clean:
        del sys.modules[mod]

@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def teams_json(rootdir):
    return os.path.join(rootdir, 'sample/teams.json')


@pytest.fixture
def repo(rootdir):
    path =  os.path.join(rootdir, 'sample/')
    sys.path.insert(0, path)
    return path

@pytest.fixture
def canary(rootdir):
    path = os.path.join(rootdir, 'canary/')
    sys.path.insert(0, path)
    return path

@pytest.fixture(scope="session")
def gcp_resources():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(os.path.dirname(test_dir), 'src/ai/chronon/resources/gcp')
    if path not in sys.path:
        sys.path.insert(0, path)
    return path

@pytest.fixture(scope="session")
def aws_resources():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(os.path.dirname(test_dir), 'src/ai/chronon/resources/aws')
    if path not in sys.path:
        sys.path.insert(0, path)
    return path

@pytest.fixture(scope="session")
def azure_resources():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(os.path.dirname(test_dir), 'src/ai/chronon/resources/azure')
    if path not in sys.path:
        sys.path.insert(0, path)
    return path

@pytest.fixture
def test_online_group_by(repo):
    return os.path.join(repo, 'production/group_bys/sample_team/event_sample_group_by.v1')

@pytest.fixture
def online_join_conf():
    """ Standard online join for tests """
    return "compiled/joins/gcp/demo.v1__1"

@pytest.fixture
def sleepless():
    def justpass(seconds):
        pass
    return justpass
