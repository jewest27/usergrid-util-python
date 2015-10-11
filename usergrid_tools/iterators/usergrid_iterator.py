from Queue import Empty
import argparse
import json
import logging
import sys
from multiprocessing import Queue, Process
import datetime
import requests
import traceback
from logging.handlers import RotatingFileHandler
import time
from usergrid import UsergridClient, UsergridError

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('UsergridIterator')


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './app_iterator.log'
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


config = {}


class Worker(Process):
    def __init__(self, queue, source_client, target_client, handler_function):
        super(Worker, self).__init__()
        logger.warning('Creating worker!')
        self.queue = queue
        self.handler_function = handler_function
        self.source_client = source_client
        self.target_client = target_client

    def run(self):

        logger.info('starting run()...')
        keep_going = True

        count_processed = 0

        while keep_going:
            empty_count = 0

            try:
                app, collection_name, entity = self.queue.get(timeout=60)

                if self.handler_function is not None:
                    processed = self.handler_function(app, collection_name, entity, self.source_client,
                                                      self.target_client)

                    if processed:
                        count_processed += 1
                        logger.info('Processed [%sth] app/collection/name = %s / %s / %s' % (
                            count_processed, app, collection_name, entity.get('uuid')))

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1
                if empty_count > 10:
                    keep_going = False


def create_new(org_name, app_name, collection_name, source_entity, source_client, target_client, attempts=0):
    attempts += 1

    if 'metadata' in source_entity: source_entity.pop('metadata')

    target_org = config.get('org_mapping', {}).get(org_name, org_name)
    target_app = config.get('app_mapping', {}).get(app_name, app_name)
    target_collection = config.get('collection_mapping', {}).get(collection_name, collection_name)

    try:
        org = target_client.organization(target_org)
        app = org.application(target_app)
        collection = app.collection(target_collection)

        e = collection.entity_from_data(source_entity)
        e.put()

        return True

    except UsergridError, e:
        print traceback.format_exc()
        logger.error(e)

    return False


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid App/Collection Iterator')

    parser.add_argument('-o', '--org',
                        help='Name of the org to migrate',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='Multiple, name of apps to include',
                        required=True,
                        action='append')

    parser.add_argument('-c', '--collection',
                        help='Multiple, name of collections to include, include \'*\' to do all collections',
                        required=True,
                        action='append')

    parser.add_argument('-s', '--source_config',
                        help='The configuration of the source endpoint/org',
                        type=str,
                        default='source.json')

    parser.add_argument('-d', '--target_config',
                        help='The configuration of the target endpoint/org',
                        type=str,
                        default='destination.json')

    parser.add_argument('-w', '--workers',
                        help='The number of worker threads',
                        type=int,
                        default=1)

    parser.add_argument('-f', '--force',
                        help='Force an update regardless of modified date',
                        type=bool,
                        default=False)

    parser.add_argument('--ql',
                        help='The QL to use in the filter',
                        type=str,
                        default='select *')

    parser.add_argument('--map_app',
                        help="A colon-separated string such as 'apples:oranges' which indicates to put data from the app named 'apples' from the source endpoint into app named 'oranges' in the target endpoint",
                        default=[],
                        action='append')

    parser.add_argument('--map_collection',
                        help="A colon-separated string such as 'cats:dogs' which indicates to put data from collections named 'cats' from the source endpoint into a collection named 'dogs' in the target endpoint, applicable to all apps",
                        default=[],
                        action='append')

    parser.add_argument('--map_org',
                        help="A colon-separated string such as 'red:blue' which indicates to put data from org named 'red' from the source endpoint into a collection named 'blue' in the target endpoint, applicable to all apps",
                        default=[],
                        action='append')

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


def init():
    global config

    config['collection_mapping'] = {}
    config['app_mapping'] = {}
    config['org_mapping'] = {}

    with open(config.get('source_config'), 'r') as f:
        config['source_config'] = json.load(f)

    with open(config.get('target_config'), 'r') as f:
        config['target_config'] = json.load(f)

    for mapping in config.get('map_collection', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['collection_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping malformed Collection mapping: [%s]' % mapping)

    for mapping in config.get('map_app', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['app_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping malformed App mapping: [%s]' % mapping)

    for mapping in config.get('map_org', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['org_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping Org mapping: [%s]' % mapping)

    config['source_endpoint'] = config['source_config'].get('endpoint').copy()
    config['source_endpoint'].update(config['source_config']['credentials'][config['org']])

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))

    config['target_endpoint'] = config['target_config'].get('endpoint').copy()
    config['target_endpoint'].update(config['target_config']['credentials'][target_org])


def wait_for(threads, sleep_time=3000):
    wait = True

    while wait:
        wait = False

        for t in threads:

            if t.is_alive():
                wait = True
                time.sleep(sleep_time)
                break


def main():
    global config

    config = parse_args()
    init()

    init_logging()
    queue = Queue()
    logger.warning('Starting workers...')

    apps_to_process = config.get('app')
    collections_to_process = config.get('collection')
    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))

    source_client = UsergridClient(api_url=config.get('source_endpoint').get('api_url'),
                                   org_name=config.get('org'))

    source_client.authenticate_management_client(config['source_config']['credentials'][config.get('org')])

    target_client = UsergridClient(api_url=config.get('target_endpoint').get('api_url'),
                                   org_name=target_org)

    target_client.authenticate_management_client(config['target_config']['credentials'][target_org])

    workers = [Worker(queue, source_client, target_client, create_new) for x in xrange(config.get('workers'))]
    [w.start() for w in workers]

    for app in source_client.list_apps():

        if app not in apps_to_process and '*' not in apps_to_process:
            logger.warning('Skipping app=[%s]' % app)
            continue

        logger.warning('Processing app=[%s]' % app)

        target_app_name = config.get('app_mapping', {}).get(app, app)

        target_app = target_client.organization(target_org).application(target_app_name)

        for collection_name, collection in target_app.list_collections():

            if len(collections_to_process) > 0 and collection_name not in collections_to_process:
                logger.warning('Skipping collection=[%s]' % collection_name)
                continue

            logger.warning('Processing collection=%s' % collection_name)

            counter = 0

            try:
                for entity in collection.query(ql=config.get('ql'),
                                               limit=config.get('source_endpoint').get('limit')):
                    counter += 1
                    queue.put((config.get('org'), app, collection_name, entity))

            except KeyboardInterrupt:
                [w.terminate() for w in workers]

        logger.info('DONE!')

    wait_for(workers)
    logger.info('workers DONE!')


main()
