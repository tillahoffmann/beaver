from setuptools import find_packages, setup


setup(
    name="beaver-build",
    packages=find_packages(),
    version="0.1.0",
    install_requires=[
        "aiohttp",
    ],
    extras_require={
        "tests": [
            "flake8",
            "pytest",
            "pytest-cov",
        ],
        "docs": [
            "sphinx",
            "sphinx_rtd_theme",
        ]
    }
)
