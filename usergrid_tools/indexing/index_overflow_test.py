import json
from multiprocessing import Pool
import datetime
import requests
import time

data = [
    {
        "id": "001",
        "dataType": "entitlements",
        "mockData": [
            {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
            {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
             "property": "show"},
            {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
             "property": "hide"}
        ]
    },
    {
        "id": "002",
        "dataType": "entitlements",
        "mockData": [
            {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
            {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
             "property": "show"},
            {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
             "property": "hide"}
        ]
    },
    {
        "id": "003",
        "dataType": "entitlements",
        "mockData": [
            {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
            {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
             "property": "show"},
            {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
             "property": "hide"}
        ]
    },
    {
        "id": "004",
        "dataType": "entitlements",
        "mockData": [
            {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
            {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
             "property": "show"},
            {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
             "property": "hide"}
        ]
    },
    {
        "id": "005",
        "dataType": "entitlements",
        "mockData": [
            {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
            {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
             "property": "show"},
            {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
             "property": "hide"}
        ]
    }
]

entity_template = {
    "id": "005",
    "dataType": "entitlements",
    "mockData": [
        {"importDate": "2015-08-25T23:33:57.124Z", "rowsImported": 2},
        {"role": "line-owner", "route": "/master", "element": "element1", "entitlementId": "entitlement4",
         "property": "show"},
        {"role": "line-owner", "route": "/master", "element": "element2", "entitlementId": "entitlement8",
         "property": "hide"}
    ]
}

# url_template = 'https://usergrid-e2e-prod.e2e.apigee.net/appservices-2-1/{org}/{app}/{collection}'
url_template = 'https://usergrid-e2e-prod.e2e.apigee.net/appservices-2-1/{org}/{app}/{collection}'

start_string = datetime.datetime.now().strftime('%yx%mx%dx%Hx%Mx%S')

url_data = {
    'org': 'jwest27',
    'app': 'sandbox',
    'collection': start_string
}

url = url_template.format(**url_data)

session = requests.session()


def create_entity(entity):
    global url
    r = session.post(url, data=json.dumps(entity))

    if r.status_code != 200:
        print 'HTTP [%s]: %s | %s' % (r.status_code, url, r.text)
        return None, None

    else:
        entities = r.json().get('entities', [])
        uuid = entities[0].get('uuid')

        return uuid, entity


def test_multiple(processes=None, count=10):

    if processes is None:
        my_process_pool = True
        processes = Pool(64)
    else:
        my_process_pool = True

    start = datetime.datetime.now()

    print 'Creating %s entities w/ url=%s' % (count, url)
    created_map = {}
    entities = []

    for count in xrange(1, count + 1):
        entity = entity_template.copy()
        entity['id'] = str(count)
        entities.append(entity)

    responses = processes.map(create_entity, entities)

    if my_process_pool:
        processes.terminate()

    for res in responses:
        if len(res) > 0 and res[0] is not None:
            created_map[res[0]] = res[1]

    stop = datetime.datetime.now()

    print 'Created [%s] entities in %s' % (len(res), (stop - start))
    processes.terminate()

    return created_map


def test_array():
    created_map = {}

    for datum in data:
        r = requests.post(url, data=json.dumps(datum))

        entities = r.json().get('entities', [])
        uuid = entities[0].get('uuid')
        print '%s: %s' % (r.status_code, uuid)
        created_map[uuid] = entities[0]

    return created_map


def test_created(created_map, q_url, sleep_time=0.0):
    print 'checking number created, expecting %s....' % len(created_map)

    count_missing = 100
    start = datetime.datetime.now()

    while count_missing > 0:
        count_missing = 0

        entity_map = {}
        try:
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
        except:
            print 'Error connecting, Sleeping for %s' % sleep_time
            time.sleep(sleep_time)

    stop = datetime.datetime.now()
    print 'All entities found after %s' % (stop - start)


def clear(url):
    print 'deleting.... ' + url
    r = requests.delete(url)

    if r.status_code != 200:
        print 'error deleting!'
        print json.dumps(r.json())

    else:
        res = r.json()
        len_entities = len(res.get('entities', []))

        if len_entities > 0:
            clear(url)


def test_cleared(q_url):
    r = requests.get(q_url)

    if r.status_code != 200:
        print json.dumps(r.json())
    else:
        res = r.json()

        if len(res.get('entities', [])) != 0:
            print 'DID NOT CLEAR'


_processes = Pool(8)

for x in xrange(10):

    try:
        url_data = {
            'org': 'jwest27',
            'app': 'sandbox',
            'collection': start_string
        }

        url = url_template.format(**url_data)

        q_url = url + "?ql=select * where dataType='entitlements'&limit=1000"

        created_map = test_multiple(count=999)

        test_created(created_map, q_url, 1)

        clear(q_url)

        start_string = datetime.datetime.now().strftime('%yx%mx%dx%Hx%Mx%S')

    except KeyboardInterrupt:
        _processes.terminate()

_processes.terminate()
