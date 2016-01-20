__author__ = 'Jeff West @ ApigeeCorporation'

from setuptools import setup, find_packages

VERSION = '0.0.1'

setup(
    name='usergrid-util',
    version=VERSION,
    description='Tools for working with Apache Usergrid',
    url='http://usergrid.apache.org',
    download_url="https://codeload.github.com/jwest-apigee/usergrid-util-python/zip/v0.0.1",
    author='Jeff West',
    author_email='jwest@apigee.com',
    packages=find_packages(),
    install_requires=[
        'requests',
        'usergrid'
    ],
    entry_points={
    }
)
