import argparse
import datetime
import logging
import sys
from logging.handlers import RotatingFileHandler

from usergrid import UsergridQuery

__author__ = 'Jeff West @ ApigeeCorporation'

"""
This script simply iterates a collection/url using a cursor
"""

logger = logging.getLogger('CollectionIterator')

collection_url_template = "{protocol}://{host}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
collection_query_url_template = "{protocol}://{host}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"

config = {}


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './deleter.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(name)s | %(processName)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=204857600,
                                                         backupCount=10)
    rotating_file.setFormatter(log_formatter)
    rotating_file.setLevel(logging.INFO)

    root_logger.addHandler(rotating_file)
    root_logger.setLevel(logging.INFO)

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid Collection (cursor) Iterator')

    parser.add_argument('-o', '--org',
                        help='Name of the org',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='name of apps',
                        required=True,
                        type=str)

    parser.add_argument('-c', '--collection',
                        help='name of collection',
                        required=True,
                        type=str)

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


def main():
    global config

    start = datetime.datetime.now()

    config = parse_args()

    init_logging()

    url = collection_url_template.format(org=config['org'],
                                         app=config['app'],
                                         collection=config['collection'],
                                         **config.get('source_endpoint'))
    print url

    q = UsergridQuery(url)

    print 'done'
    counter = 0

    for e in q:
        counter += 1
        print 'Name/UUID: %s/%s' % (e.get('name'), e.get('uuid'))

    finish = datetime.datetime.now()

    logger.warning('Done!  Took: %s ' % (finish - start))


main()
