__author__ = 'Jeff West @ ApigeeCorporation'

from setuptools import setup, find_packages

VERSION = '0.0.4'

setup(
        name='usergrid-tools',
        version=VERSION,
        description='Tools for working with Apache Usergrid',
        url='http://usergrid.apache.org',
        download_url="https://codeload.github.com/jwest-apigee/usergrid-util-python/zip/%s" % VERSION,
        author='Jeff West',
        author_email='jwest@apigee.com',
        packages=find_packages(),
        install_requires=[
            'requests',
            'usergrid',
            'argparse'
        ],
        entry_points={
            'console_scripts': [
                'usergrid_data_migrator = usergrid_tools.migration.usergrid_data_migrator:main',
                'usergrid_index_test = usergrid_tools.indexing.index_test:main',
                'usergrid_parse_importer = usergrid_tools.parse_importer.parse_importer:main',
                'usergrid_deleter = usergrid_tools.parse_importer.parse_importer:main',
                'usergrid_library_check = usergrid_tools.library_check:main',
            ]
        }
)
