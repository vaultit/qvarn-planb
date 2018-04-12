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
    version='0.0.1',
    licence='AGPL',
    packages=['qvarn'],
    install_requires=read_requirements('requirements.in'),
    entry_points={
        'console_scripts': [
            'qvarn = qvarn.app:main',
        ]
    },
)
