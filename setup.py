import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='Informatics On Demand',
    version='0.0.1',
    description='Library will in the future power InVis.',
    long_description=README,
    packages=['iod'],
    include_package_data=False,
)
