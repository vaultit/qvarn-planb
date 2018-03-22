#!/usr/bin/python3

from setuptools import setup


def read_requirements(filename):
    with open(filename) as f:
        return [req for req in (req.partition('#')[0].strip() for req in f) if req]


setup(
    name='qvarn',
    description='backend service for JSON and binary data storage',
    author='Mantas Zimnickas',
    author_email='sirexas@gmail.com',
    packages=['qvarn'],
    setup_requires=['setuptools_scm'],
    install_requires=read_requirements('requirements.in'),
    use_scm_version=True,
)
