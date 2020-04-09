import pathlib

from setuptools import setup, find_packages

try:
    long_description = (pathlib.Path(__file__).parent / "README.md").read_text()
except:
    long_description = None
setup(
    name="orm",
    version="0.0.1",
    author="rshanker779",
    author_email="rshanker779@gmail.com",
    description="Basic ORM",
    long_description=long_description if long_description is not None else "Basic ORM",
    license="MIT",
    python_requires=">=3.7",
    install_requires=[
        "rshanker779_common",
        "graphs @ git+ssh://git@bitbucket.org:rshanker779/graphs.git",
    ],
    packages=find_packages(),
    entry_points={},
)
