# Dramatiq-abort is a middleware to abort Dramatiq tasks.
# Copyright (C) 2019 Flare Systems Inc. <oss@flare.systems>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def rel(*xs: str) -> str:
    return os.path.join(here, *xs)


with open(rel("README.md")) as f:
    long_description = f.read()


with open(rel("src", "dramatiq_abort", "__init__.py"), "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")


dependencies = [
    "dramatiq",
]

extra_dependencies = {
    "gevent": ["gevent>=1.1"],
    "redis": ["redis>=2.0,<4.0"],
}

extra_dependencies["all"] = list(set(sum(extra_dependencies.values(), [])))
extra_dependencies["dev"] = extra_dependencies["all"] + [
    # Linting
    "flake8",
    "flake8-bugbear",
    "flake8-quotes",
    "isort",
    "mypy",
    "types-redis",
    "black",
    # Testing
    "pytest",
    "pytest-cov",
    "tox>=3.14.0",
    # Docs
    "sphinx",
    "sphinx-autodoc-typehints",
    # Build
    "build",
    "wheel",
    "twine",
]

setup(
    name="dramatiq-abort",
    version=version,
    author="Flare Systems Inc.",
    author_email="oss@flare.systems",
    description="Dramatiq middleware to abort tasks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flared/dramatiq-abort",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=dependencies,
    extras_require=extra_dependencies,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "Development Status :: 4 - Beta",
        "Topic :: System :: Distributed Computing",
        (
            "License :: OSI Approved :: "
            "GNU Lesser General Public License v3 or later (LGPLv3+)"
        ),
    ],
)
