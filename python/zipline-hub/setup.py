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
import re

from setuptools import find_namespace_packages, setup

current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, "README.md"), "r") as fh:
    long_description = fh.read()


# Hub-specific requirements (if any, beyond what zipline-ai provides)
hub_requirements = [
    "requests>=2.31.0",
    "google-auth>=2.23.0",
    "google-cloud-iam>=2.12.0",
]

with open(os.path.join(current_dir, "requirements/base.in"), "r") as infile:
    base_requirements = [line.strip() for line in infile if line.strip()]

# Combine base and hub-specific requirements
install_requirements = base_requirements + hub_requirements

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


setup(
    classifiers=[
        "Programming Language :: Python :: 3.11"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    description="Zipline Hub integration - Extends zipline-ai with Hub functionality",
    install_requires=[
        f"zipline-ai>={get_version()}",  # Depends on core package
    ] + install_requirements,
    name="zipline-hub",
    packages=find_namespace_packages(where="src", include=["ai.*"]),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.11",
    url=None,
    version=get_version(),
    zip_safe=False,
)
