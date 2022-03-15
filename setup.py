import re
from setuptools import find_packages, setup


with open("README.rst") as fp:
    long_description = fp.read()
long_description = long_description.replace(".. doctest::", ".. code-block:: python")
long_description = re.sub(":(math)|(func)|(class):", ":code:", long_description)
long_description = re.sub(
    r"((\.\. automodule:: .*?$)|(\.\. toctree::)|(\.\. plot:: .*?$))", r".. code-block::\n\n  \1",
    long_description, flags=re.MULTILINE
)

# Load the version number
try:
    with open('VERSION') as fp:
        version = fp.read().strip()
except FileNotFoundError:
    version = 'dev'

setup(
    name="beaver-build",
    packages=find_packages(),
    author="Till Hoffmann",
    author_email="niches_osmosis0a@icloud.com",
    version=version,
    install_requires=[
        "aiohttp",
    ],
    long_description=long_description,
    long_description_content_type="text/x-rst",
    extras_require={
        "tests": [
            "flake8",
            "pytest",
            "pytest-cov",
            "twine",
        ],
        "docs": [
            "sphinx",
            "sphinx_rtd_theme",
        ]
    }
)
