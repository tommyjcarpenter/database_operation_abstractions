import os
from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession

setup(
    name='db_op_abstractions',
    version='0.1.0',
    packages=find_packages(),
    author = "Tommy Carpenter, Michael Hwang",
    author_email = "",
    description='Contains higher level operation functions for MySQL, PG, Redshift, etc.',
    license = "",
    keywords = "",
    url = "https://github.com/tommyjcarpenter/db-op-abstractions",
    zip_safe=False,
    install_requires = [str(ir.req) for ir in parse_requirements("requirements.txt", session=PipSession())],
)
