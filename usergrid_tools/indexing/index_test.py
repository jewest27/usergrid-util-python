import json
import logging
from multiprocessing import Pool
import datetime

import argparse
import requests
import time
from logging.handlers import RotatingFileHandler

import sys

entity_template = {
    "id": "replaced",
    "dataType": "entitlements",
    "mockData": [
        {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
        {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
         "property": "show"},
        {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
         "property": "hide"}
    ],
    "nullArray1": [None],
    "nullArray2": [None, None],
    "nullArray3": [None, None],
    "nest1": {
        "nest2": {
            "nest3": [None, None, 'foo']
        }
    }
}

url_template = '{api_url}/{org}/{app}/{collection}'
config = {}


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './usergrid_index_test.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(name)s | %(processName)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=2048576000,
                                                         backupCount=10)
    rotating_file.setFormatter(log_formatter)
    rotating_file.setLevel(logging.INFO)

    root_logger.addHandler(rotating_file)
    root_logger.setLevel(logging.INFO)

    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


def create_entity(entity):
    global config
    r = requests.post(config['url'], data=json.dumps(entity))
    entities = r.json().get('entities', [])
    uuid = entities[0].get('uuid')

    if r.status_code != 200:
        print '%s: %s' % (r.status_code, uuid)

    return uuid, entity


def test_multiple(number_of_entities=10):
    global processes, config

    start = datetime.datetime.now()

    print 'Creating %s entities w/ url=%s' % (number_of_entities, config['url'])
    created_map = {}
    entities = []

    for x in xrange(1, number_of_entities + 1):
        entity = entity_template.copy()
        entity['id'] = str(x)
        entities.append(entity)

    responses = processes.map(create_entity, entities)

    for res in responses:
        if len(res) > 0:
            created_map[res[0]] = res[1]

    stop = datetime.datetime.now()

    print 'Created [%s] entities in %s' % (number_of_entities, (stop - start))

    return created_map


def nested_null_test():
    pass


def test_created(created_map, q_url, sleep_time=0.0):
    print 'checking number created, expecting %s....' % len(created_map)

    nested_null_test()

    count_missing = 100
    start = datetime.datetime.now()

    while count_missing > 0:
        count_missing = 0

        entity_map = {}
        r = requests.get(q_url)
        entities = r.json().get('entities', [])

        print 'Found [%s] entities at url: %s' % (len(entities), q_url)

        for entity in entities:
            entity_map[entity.get('uuid')] = entity

        for uuid, created_entity in created_map.iteritems():
            if uuid not in entity_map:
                count_missing += 1
                # print 'Missing uuid=[%s] Id=[%s] total missing=[%s]' % (uuid, created_entity.get('id'), count_missing)

        print 'total missing=[%s], url=[%s]' % (count_missing, q_url)

        if count_missing > 0:
            print 'Waiting for indexing, Sleeping for %s' % sleep_time
            time.sleep(sleep_time)

    stop = datetime.datetime.now()
    print 'All entities found after %s' % (stop - start)


def clear(clear_url):
    print 'deleting.... ' + clear_url

    r = requests.delete(clear_url)

    if r.status_code != 200:
        print 'error deleting url=' + clear_url
        print json.dumps(r.json())

    else:
        res = r.json()
        len_entities = len(res.get('entities', []))

        if len_entities > 0:
            clear(clear_url)


def test_cleared(q_url):
    r = requests.get(q_url)

    if r.status_code != 200:
        print json.dumps(r.json())
    else:
        res = r.json()

        if len(res.get('entities', [])) != 0:
            print 'DID NOT CLEAR'


processes = Pool(32)


def test_url(q_url, sleep_time=0.25):
    test_var = False

    while not test_var:
        r = requests.get(q_url)

        if r.status_code == 200:

            if len(r.json().get('entities')) >= 1:
                test_var = True
        else:
            print 'non 200'

        if test_var:
            print 'Test of URL [%s] Passes'
        else:
            print 'Test of URL [%s] Passes'
            time.sleep(sleep_time)


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid Indexing Latency Test')

    parser.add_argument('-o', '--org',
                        help='Name of the org to perform the test in',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='Name of the app to perform the test in',
                        type=str,
                        required=True)

    parser.add_argument('--base_url',
                        help='The URL of the Usergrid Instance',
                        type=str,
                        required=True)

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


def init():
    global config

    url_data = {
        'api_url': config.get('base_url'),
        'org': config.get('org'),
        'app': config.get('app'),
        'collection': datetime.datetime.now().strftime('index-test-%yx%mx%dx%Hx%Mx%S')
    }

    config['url'] = url_template.format(**url_data)


def main():
    global config

    config = parse_args()

    init_logging()

    init()

    try:

        created_map = test_multiple(999)

        q_url = config.get('url') + "?ql=select * where dataType='entitlements'&limit=1000"

        test_created(created_map=created_map,
                     q_url=q_url,
                     sleep_time=.25)

        delete_q_url = config.get('url') + "?ql=select * where dataType='entitlements'&limit=1000"

        clear(clear_url=delete_q_url)

    except KeyboardInterrupt:
        processes.terminate()

    processes.terminate()


main()
