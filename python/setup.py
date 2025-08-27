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

import glob
import os
import re

from setuptools import find_packages, setup

current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, "README.md"), "r") as fh:
    long_description = fh.read()


with open(os.path.join(current_dir, "requirements/base.in"), "r") as infile:
    basic_requirements = [line for line in infile]


__version__ = "0.0.1"
__branch__ = "main"


def get_version():
    version_str = os.environ.get("VERSION", __version__)
    branch_str = os.environ.get("BRANCH", __branch__)
    # Replace "-SNAPSHOT" with ".dev"
    version_str = version_str.replace("-SNAPSHOT", ".dev")
    # If the prefix is the branch name, then convert it as suffix after '+' to make it Python PEP440 complaint
    if version_str.startswith(branch_str + "-"):
        version_str = "{}+{}".format(
            version_str.replace(branch_str + "-", ""), branch_str
        )

    # Replace multiple continuous '-' or '_' with a single period '.'.
    # In python version string, the label identifier that comes after '+', is all separated by periods '.'
    version_str = re.sub(r"[-_]+", ".", version_str)

    return version_str


resources = [f for f in glob.glob('test/sample/**/*', recursive=True) if os.path.isfile(f)]
setup(
    classifiers=[
        "Programming Language :: Python :: 3.11"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "zipline=ai.chronon.repo.zipline:zipline",
        ]
    },
    description="Zipline python API library",
    install_requires=basic_requirements,
    name="zipline-ai",
    packages=find_packages(),
    include_package_data=True,
    package_data={"ai.chronon": ["resources/**/*"]},
    extras_require={
        # Extra requirement to have access to cli commands in python2 environments.
        "pip2compat": ["click<8"]
    },
    python_requires=">=3.11",
    url=None,
    version=get_version(),
    zip_safe=False,
)
