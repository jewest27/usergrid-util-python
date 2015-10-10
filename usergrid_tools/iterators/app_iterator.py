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

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('AppIterator')


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

org_management_url_template = "{protocol}://{host}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_url_template = "{protocol}://{host}/{org}?client_id={client_id}&client_secret={client_secret}"
app_url_template = "{protocol}://{host}/{org}/{app}?client_id={client_id}&client_secret={client_secret}"
collection_url_template = "{protocol}://{host}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}"
collection_query_url_template = "{protocol}://{host}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"
get_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}&connections=none"
put_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"


class Worker(Process):
    def __init__(self, queue, handler_function):
        super(Worker, self).__init__()
        logger.warning('Creating worker!')
        self.queue = queue
        self.handler_function = handler_function

    def run(self):

        logger.info('starting run()...')
        keep_going = True

        count_processed = 0

        while keep_going:
            empty_count = 0

            try:
                app, collection_name, entity = self.queue.get(timeout=60)

                if self.handler_function is not None:
                    processed = self.handler_function(app, collection_name, entity)

                    if processed:
                        count_processed += 1
                        logger.info('Processed [%sth] app/collection/name = %s / %s / %s' % (
                            count_processed, app, collection_name, entity.get('uuid')))

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1
                if empty_count < 10:
                    keep_going = False


def create_new(app, collection_name, source_entity, attempts=0):
    attempts += 1

    if 'metadata' in source_entity: source_entity.pop('metadata')

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
    target_app = config.get('app_mapping', {}).get(app, app)
    target_collection = config.get('collection_mapping', {}).get(collection_name, collection_name)

    try:
        target_entity_url = put_entity_url_template.format(org=target_org,
                                                           app=target_app,
                                                           collection=target_collection,
                                                           uuid=source_entity.get('uuid'),
                                                           **config.get('target_endpoint'))

        millis_created = source_entity.get('created', 0)
        date_created = datetime.datetime.fromtimestamp(millis_created / 1000.0)

        millis_modified = source_entity.get('modified', 0)
        date_modified = datetime.datetime.fromtimestamp(millis_modified / 1000.0)

        should_write = False

        if date_modified > datetime.datetime.utcnow() - datetime.timedelta(
                days=1) or date_created > datetime.datetime.utcnow() - datetime.timedelta(days=1):
            should_write = True

        if not should_write:
            r = requests.get(target_entity_url)

            if r.status_code != 200:
                entities = r.json()

                if len(entities) > 0:
                    target_entity = entities[0]

                    if target_entity.get('modified') != source_entity.get('modified'):
                        should_write = True
                else:
                    should_write = True

        if should_write or config.get('force'):
            r = requests.put(target_entity_url, json.dumps(source_entity))

            if r.status_code in ['401', '404']:
                collection_url = collection_url_template.format(org=config.get('org'),
                                                                app=app,
                                                                collection=collection_name,
                                                                **config.get('target_endpoint'))
                r = requests.post(collection_url, json.dumps(source_entity))

                if r.status_code >= 500:

                    if attempts > 3:
                        logger.critical('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                            r.status_code, attempts, target_entity_url, json.dumps(source_entity), r.json()))
                        logger.critical('WILL NOT RETRY')
                        return False

            elif r.status_code >= 500:

                if attempts > 3:
                    logger.critical('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                        r.status_code, attempts, target_entity_url, json.dumps(source_entity), r.json()))
                    logger.critical('WILL NOT RETRY')
                    return False

                # logger.error('[%s] on attempt [%s] to POST url=[%s]' % (r.status_code, attempts, target_url))
                time.sleep(5)
                create_new(app, collection_name, source_entity, attempts)

            else:
                logger.info('PUT [%s] (%s): [%s]' % (r.status_code, r.elapsed, target_entity_url))
        else:
            logger.info('No Update on %s' % target_entity_url)

        return True

    except:
        print traceback.format_exc()
        print 'error on entity: \n %s' % json.dumps(source_entity, indent=2)

    return True


def parse_args():
    parser = argparse.ArgumentParser(description='Org/App Migrator')

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
            logger.warning('Skipping Collection mapping: [%s]' % mapping)

    for mapping in config.get('map_app', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['app_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping App mapping: [%s]' % mapping)

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
    org_mgmt_url = org_management_url_template.format(org=config.get('org'),
                                                      **config.get('source_endpoint'))
    logger.info('GET %s' % org_mgmt_url)
    r = requests.get(org_mgmt_url)

    print json.dumps(r.json(), indent=2)

    queue = Queue()
    logger.warning('Starting workers...')

    workers = [Worker(queue, create_new) for x in xrange(config.get('workers'))]
    [w.start() for w in workers]

    apps_to_process = config.get('app')
    collections_to_process = config.get('collection')
    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))

    for org_app, app_uuid in r.json().get('data').iteritems():
        parts = org_app.split('/')
        app = parts[1]

        if app not in apps_to_process and '*' not in apps_to_process:
            logger.warning('Skipping app=[%s]' % app)
            continue

        logger.warning('Processing app=[%s]' % app)

        target_app = config.get('app_mapping', {}).get(app, app)

        target_app_url = app_url_template.format(org=target_org,
                                                 app=target_app,
                                                 **config.get('target_endpoint'))
        logger.info('GET %s' % target_app_url)
        r_target_apps = requests.get(target_app_url)

        if r_target_apps.status_code != 200:
            logger.error('Target application does not exist at URL=%s' % target_app_url)
            continue

        app_url = app_url_template.format(org=config.get('org'),
                                          app=app,
                                          **config.get('source_endpoint'))
        logger.info('GET %s' % app_url)

        r_collections = requests.get(app_url)

        while r_collections.status_code != 200:
            logger.warning('GET (%s) [%s] URL: %s' % r_collections.elapsed, r_collections.status_code, app_url)
            time.sleep(5)
            r_collections = requests.get(app_url)

        app_response = r_collections.json()

        logger.info('App Response: ' + json.dumps(app_response))

        app_entities = app_response.get('entities')

        if len(app_entities) > 0:
            app_entity = app_entities[0]
            collections = app_entity.get('metadata', {}).get('collections', {})
            logger.warning('Collection List: %s' % collections)

            for collection_name, collection_data in collections.iteritems():

                if len(collections_to_process) > 0 and collection_name not in collections_to_process:
                    logger.warning('Skipping collection=[%s]' % collection_name)
                    continue

                logger.warning('Processing collection=%s' % collection_name)

                counter = 0

                top_flag = True

                url = collection_query_url_template.format(org=config.get('org'),
                                                           app=app,
                                                           collection=collection_name,
                                                           ql=config.get('ql'),
                                                           **config.get('source_endpoint'))
                logger.info('GET ' + url)

                r = requests.get(url, verify=False)

                logger.info('GET (%s) [%s] on URL=%s' % (r.elapsed, r.status_code, url))

                response = r.json()

                while top_flag:
                    try:
                        entities = response.get('entities', [])
                        counter += len(entities)

                        logger.info('Retrieved [%s] entities' % (len(entities)))

                        logger.info('Collection=[%s] counter=[%s]' % (collection_name, counter))

                        for entity in entities:
                            queue.put((app, collection_name, entity))

                        if 'cursor' not in response:
                            logger.warning('no cursor in response')
                            break

                        cursor = response.get('cursor')
                        logger.debug('cursor: %s' % cursor)

                        cursor_url = url + '&cursor=%s' % cursor

                        proceed = False

                        while not proceed:
                            try:
                                logger.info('GET ' + cursor_url)

                                r = requests.get(cursor_url, verify=False)

                                logger.info('GET (%s) [%s] on URL=%s' % (r.elapsed, r.status_code, url))

                                if r.status_code == 200:
                                    logger.info('[200]: GET %s' % cursor_url)
                                    response = r.json()
                                    proceed = True
                                else:
                                    logger.error('[%s]: GET %s' % (r.status_code, cursor_url))
                                    logger.error('Failed getting next page: [%s]: %s' % (r.status_code, r.text))
                                    logger.warn('Sleeping 10s...')
                                    time.sleep(10)

                            except KeyboardInterrupt, e:
                                raise e

                            except:
                                logger.error(traceback.format_exc())
                                print traceback.format_exc()

                        logger.debug('proceeding...')

                        logger.info('Counter: %s' % counter)

                    except KeyboardInterrupt:
                        top_flag = False
                        logger.warning('Keyboard Interrupt, aborting...')
                        [w.terminate() for w in workers]

        logger.info('DONE!')

    wait_for(workers)

    logger.info('workers DONE!')


main()
