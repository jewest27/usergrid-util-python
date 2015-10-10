from Queue import Empty
import argparse
import json
import time
import logging
import sys
from multiprocessing import Queue, Process
import datetime
import requests
import traceback
from logging.handlers import RotatingFileHandler

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('CollectionIterator')

org_management_url_template = "{protocol}://{host}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_url_template = "{protocol}://{host}/{org}?client_id={client_id}&client_secret={client_secret}"
app_url_template = "{protocol}://{host}/{org}/{app}?client_id={client_id}&client_secret={client_secret}"
collection_url_template = "{protocol}://{host}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
collection_query_url_template = "{protocol}://{host}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"
get_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}&connections=none"
put_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"

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
                org, app, collection, entity = self.queue.get(timeout=600)

                if self.handler_function is not None:
                    processed = self.handler_function(org, app, collection, entity)

                    if processed:
                        count_processed += 1
                        logger.info('Processed [%sth] uuid/name = %s / %s' % (
                            count_processed, entity.get('uuid'), entity.get('name')))

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                empty_count += 1
                if empty_count < 10:
                    keep_going = False


def init():
    global config

    with open('%s/%s' % (config.get('config_path'), config.get('source_config')), 'r') as f:
        config['source_config'] = json.load(f)

    with open('%s/%s' % (config.get('config_path'), config.get('target_config')), 'r') as f:
        config['target_config'] = json.load(f)

    config['source_endpoint'] = config['source_config'].get('endpoint').copy()
    config['source_endpoint'].update(config['source_config']['credentials'][config['org']])

    config['target_endpoint'] = config['target_config'].get('endpoint').copy()
    config['target_endpoint'].update(config['target_config']['credentials'][config['org']])


def wait_for(threads, sleep_time=3000):
    wait = True

    while wait:
        wait = False

        for t in threads:

            if t.is_alive():
                wait = True
                time.sleep(sleep_time)
                break


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid Collection Iterator')

    parser.add_argument('-o', '--org',
                        help='Name of the org to migrate',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='Multiple, name of apps to include',
                        required=True,
                        type=str)

    parser.add_argument('-c', '--collection',
                        help='Multiple, name of collections to include, include \'*\' to do all collections',
                        required=True,
                        type=str)

    parser.add_argument('-p', '--config_path',
                        help='The directory in which to find the config files',
                        type=str,
                        default='.')

    parser.add_argument('-s', '--source_config',
                        help='The configuration of the source endpoint/org',
                        type=str,
                        default='usergrid_source.json')

    parser.add_argument('-t', '--target_config',
                        help='The configuration of the target endpoint/org',
                        type=str,
                        default='usergrid_target.json')

    parser.add_argument('-w', '--workers',
                        help='The number of worker threads',
                        type=int,
                        default=8)

    parser.add_argument('--ql',
                        help='The QL to use in the filter',
                        type=str,
                        default='select *')

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


collection_mapping = {
    'providers': 'nearprod-providers'
}

MIGRATE = True


def put_new(org, app, collection_name, entity, attempts=0):
    attempts += 1

    if 'metadata' in entity: entity.pop('metadata')

    collection_name = collection_mapping.get(collection_name, collection_name)

    if MIGRATE:
        if 'uuid' in entity: entity.pop('uuid')

        try:
            logger.warn(put_entity_url_template)
            my_tmp = put_entity_url_template
            source_endpoint = config.get('source_endpoint')
            entity_name_uuid = entity.get('name')
            entity_url = put_entity_url_template.format(uuid=entity_name_uuid,
                                                        org=org,
                                                        app=app,
                                                        collection=collection_name,
                                                        **source_endpoint)

            r = requests.put(entity_url, json.dumps(entity))

            if r.status_code in ['401', '404']:
                collection_url = collection_url_template.format()
                r = requests.post(collection_url, json.dumps(entity))

                if r.status_code >= 500:

                    if attempts > 3:
                        logger.critical('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                            r.status_code, attempts, entity_url, json.dumps(entity), r.json()))
                        logger.critical('WILL NOT RETRY')
                        return False

            elif r.status_code >= 500:

                if attempts > 3:
                    logger.critical('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                        r.status_code, attempts, entity_url, json.dumps(entity), r.json()))
                    logger.critical('WILL NOT RETRY')
                    return False

                # logger.error('[%s] on attempt [%s] to POST url=[%s]' % (r.status_code, attempts, target_url))
                time.sleep(5)
                put_new(org, app, collection_name, entity, attempts)

            else:
                logger.info('[%s] on PUT to [%s]' % (r.status_code, entity_url))

            return True

        except:
            print traceback.format_exc()
            logger.error('error on entity: \n %s' % json.dumps(entity, indent=2))

    return True


def main():
    global config

    config = parse_args()
    init()

    init_logging()

    url = collection_url_template.format(org=config['org'],
                                         app=config['app'],
                                         collection=config['collection'],
                                         **config.get('source_endpoint'))
    print url

    start = datetime.datetime.now()

    r = requests.get(url, verify=False)

    print r.status_code
    response = r.json()
    character_count = len(r.text)
    counter = 0

    queue = Queue()
    logger.warning('Starting workers...')

    workers = [Worker(queue, put_new) for x in xrange(config.get('workers'))]
    [w.start() for w in workers]

    top_flag = True
    all_entities = []

    while top_flag:
        try:
            entities = response.get('entities', [])

            logger.info('Retrieved [%s] entities' % len(entities))
            counter += len(entities)

            all_entities += entities
            logger.info("Current length of all_entities: %s" % len(all_entities))

            for entity in entities:
                queue.put((config['org'], config['app'], config['collection'], entity))

            if 'cursor' not in response:
                logger.warning('no cursor in response, stopping iteration')
                top_flag = False
            else:
                cursor = response.get('cursor')
                logger.debug('cursor: %s' % cursor)

                cursor_url = url + '&cursor=%s' % cursor

                proceed = False

                while not proceed:
                    try:
                        logger.info('GET %s' % cursor_url)
                        r = requests.get(cursor_url, verify=False)

                        if r.status_code == 200:
                            logger.info('Response [200]: Time: ' + str(r.elapsed))
                            character_count += len(r.text)
                            logger.info('Total byte count: %s' % sys.getsizeof(all_entities))
                            response = r.json()
                            proceed = True
                        else:
                            logger.error('[%s]: GET %s' % (r.status_code, cursor_url))
                            logger.error('Failed getting next page: [%s]: %s' % (r.status_code, r.text))

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

    # with open('/Users/ApigeeCorporation/tmp/all_entities.json', 'w') as f:
    #     logger.warning('Dumping [%s] providers to file...' % len(all_entities))
    #     json.dump(all_entities, f)

    finish = datetime.datetime.now()

    logger.warning('Done!  Took: %s ' % (finish - start))


main()
