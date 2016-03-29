import os
import uuid
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

import signal
from usergrid import UsergridQueryIterator
import urllib3

__author__ = 'Jeff West @ ApigeeCorporation'

ECID = str(uuid.uuid4())

logger = logging.getLogger('Main')
worker_logger = logging.getLogger('Worker')
collection_worker_logger = logging.getLogger('CollectionWorker')
data_logger = logging.getLogger('DataMigrator')
audit_logger = logging.getLogger('AuditLogger')
status_logger = logging.getLogger('StatusLogger')
connection_logger = logging.getLogger('ConnectionMigrator')
credential_logger = logging.getLogger('CredentialMigrator')

urllib3.disable_warnings()

DEFAULT_CREATE_APPS = False
DEFAULT_RETRY_SLEEP = 2
DEFAULT_PROCESSING_SLEEP = 5

queue = Queue()
QSIZE_OK = False

try:
    queue.qsize()
    QSIZE_OK = True
except:
    pass

session_source = requests.Session()
session_target = requests.Session()

USE_CACHE = True

if USE_CACHE:
    import redis

    cache = redis.StrictRedis(host='localhost', port=6379, db=0)
    # cache.flushall()

else:
    cache = None


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # root_logger.setLevel(logging.WARN)

    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)

    log_formatter = logging.Formatter(
            fmt='%(asctime)s | ' + ECID + ' | %(name)s | %(processName)s | %(levelname)s | %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p')

    stdout_logger = logging.StreamHandler(sys.stdout)
    stdout_logger.setFormatter(log_formatter)
    stdout_logger.setLevel(logging.CRITICAL)
    root_logger.addHandler(stdout_logger)

    if stdout_enabled:
        stdout_logger.setLevel(logging.INFO)

    # base log file

    log_dir = '/var/log/tomcat7'
    log_dir = '/mnt/raid/logs'

    log_file_name = '%s/migrator.log' % log_dir
    # ConcurrentLogHandler
    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=404857600,
                                                         backupCount=0)
    rotating_file.setFormatter(log_formatter)
    rotating_file.setLevel(logging.INFO)

    root_logger.addHandler(rotating_file)

    # ERROR LOG

    error_log_file_name = '%s/migrator_errors.log' % log_dir
    error_rotating_file = logging.handlers.RotatingFileHandler(filename=error_log_file_name,
                                                               mode='a',
                                                               maxBytes=404857600,
                                                               backupCount=0)
    error_rotating_file.setFormatter(log_formatter)
    error_rotating_file.setLevel(logging.ERROR)

    root_logger.addHandler(error_rotating_file)

    # AUDIT LOG

    audit_log_file_name = '%s/migrator_audit.log' % log_dir
    audit_rotating_file = logging.handlers.RotatingFileHandler(filename=audit_log_file_name,
                                                               mode='a',
                                                               maxBytes=404857600,
                                                               backupCount=10)
    audit_rotating_file.setFormatter(log_formatter)
    audit_rotating_file.setLevel(logging.WARNING)

    audit_logger.addHandler(audit_rotating_file)

    # DATA LOG

    data_log_file_name = '%s/migrator_data.log' % log_dir
    data_rotating_file = logging.handlers.RotatingFileHandler(filename=data_log_file_name,
                                                              mode='a',
                                                              maxBytes=404857600,
                                                              backupCount=10)
    data_rotating_file.setFormatter(log_formatter)
    data_rotating_file.setLevel(logging.WARNING)

    data_logger.addHandler(data_rotating_file)

    # CONNECTION LOG

    connections_log_file_name = '%s/migrator_connections.log' % log_dir
    connections_rotating_file = logging.handlers.RotatingFileHandler(filename=connections_log_file_name,
                                                                     mode='a',
                                                                     maxBytes=404857600,
                                                                     backupCount=10)
    connections_rotating_file.setFormatter(log_formatter)
    connections_rotating_file.setLevel(logging.WARNING)

    connection_logger.addHandler(connections_rotating_file)

    # CREDENTIALS LOG

    credential_log_file_name = '%s/migrator_credentials.log' % log_dir
    credentials_rotating_file = logging.handlers.RotatingFileHandler(filename=credential_log_file_name,
                                                                     mode='a',
                                                                     maxBytes=404857600,
                                                                     backupCount=10)
    credentials_rotating_file.setFormatter(log_formatter)
    credentials_rotating_file.setLevel(logging.WARNING)

    credential_logger.addHandler(credentials_rotating_file)


entity_name_map = {
    'users': 'username'
}

config = {}

# URL Templates for Usergrid
org_management_app_url_template = "{api_url}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_management_url_template = "{api_url}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_url_template = "{api_url}/{org}?client_id={client_id}&client_secret={client_secret}"
app_url_template = "{api_url}/{org}/{app}?client_id={client_id}&client_secret={client_secret}"
collection_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}"
collection_query_url_template = "{api_url}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"
collection_graph_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
connection_query_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}?client_id={client_id}&client_secret={client_secret}&limit={limit}"

connecting_query_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/connecting/{verb}?client_id={client_id}&client_secret={client_secret}&limit={limit}"

connection_create_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}/{target_uuid}?client_id={client_id}&client_secret={client_secret}"
connection_create_by_name_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}/{target_type}/{target_name}?client_id={client_id}&client_secret={client_secret}"
get_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}&connections=none"

get_entity_url_with_connections_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"
put_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"

user_credentials_url_template = "{api_url}/{org}/{app}/users/{uuid}/credentials"

ignore_collections = ['activities', 'queues', 'events', 'notifications']


class StatusListener(Process):
    def __init__(self, status_queue, worker_queue):
        super(StatusListener, self).__init__()
        self.status_queue = status_queue
        self.worker_queue = worker_queue

    def run(self):
        keep_going = True

        org_results = {
            'name': config.get('org'),
            'apps': {},
        }

        empty_count = 0

        while keep_going:

            try:
                app, collection, status_map = self.status_queue.get(timeout=30)
                status_logger.info('Received status update for app/collection: [%s / %s]' % (app, collection))
                empty_count = 0
                org_results['summary'] = {
                    'max_created': -1,
                    'max_modified': -1,
                    'min_created': 1584946416000,
                    'min_modified': 1584946416000,
                    'count': 0,
                    'bytes': 0
                }

                if app not in org_results['apps']:
                    org_results['apps'][app] = {
                        'collections': {}
                    }

                org_results['apps'][app]['collections'].update(status_map)

                try:
                    for app, app_data in org_results['apps'].iteritems():
                        app_data['summary'] = {
                            'max_created': -1,
                            'max_modified': -1,
                            'min_created': 1584946416000,
                            'min_modified': 1584946416000,
                            'count': 0,
                            'bytes': 0
                        }

                        if 'collections' in app_data:
                            for collection, collection_data in app_data['collections'].iteritems():

                                app_data['summary']['count'] += collection_data['count']
                                app_data['summary']['bytes'] += collection_data['bytes']

                                org_results['summary']['count'] += collection_data['count']
                                org_results['summary']['bytes'] += collection_data['bytes']

                                # APP
                                if collection_data.get('max_modified') > app_data['summary']['max_modified']:
                                    app_data['summary']['max_modified'] = collection_data.get('max_modified')

                                if collection_data.get('min_modified') < app_data['summary']['min_modified']:
                                    app_data['summary']['min_modified'] = collection_data.get('min_modified')

                                if collection_data.get('max_created') > app_data['summary']['max_created']:
                                    app_data['summary']['max_created'] = collection_data.get('max_created')

                                if collection_data.get('min_created') < app_data['summary']['min_created']:
                                    app_data['summary']['min_created'] = collection_data.get('min_created')

                                # ORG
                                if collection_data.get('max_modified') > org_results['summary']['max_modified']:
                                    org_results['summary']['max_modified'] = collection_data.get('max_modified')

                                if collection_data.get('min_modified') < org_results['summary']['min_modified']:
                                    org_results['summary']['min_modified'] = collection_data.get('min_modified')

                                if collection_data.get('max_created') > org_results['summary']['max_created']:
                                    org_results['summary']['max_created'] = collection_data.get('max_created')

                                if collection_data.get('min_created') < org_results['summary']['min_created']:
                                    org_results['summary']['min_created'] = collection_data.get('min_created')

                        if QSIZE_OK:
                            status_logger.warn('CURRENT Queue Depth: %s' % self.worker_queue.qsize())

                        status_logger.warn('UPDATED status of org processed: %s' % json.dumps(org_results, indent=2))

                except KeyboardInterrupt, e:
                    raise e

                except:
                    print traceback.format_exc()

            except KeyboardInterrupt, e:
                status_logger.warn('FINAL status of org processed: %s' % json.dumps(org_results))
                raise e

            except Empty:
                if QSIZE_OK:
                    status_logger.warn('CURRENT Queue Depth: %s' % self.worker_queue.qsize())

                status_logger.warn('CURRENT status of org processed: %s' % json.dumps(org_results, indent=2))

                status_logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1

                if empty_count >= 20:
                    keep_going = False

            except:
                print traceback.format_exc()

        logger.warn('FINAL status of org processed: %s' % json.dumps(org_results))


class MessageWorker(Process):
    def __init__(self, queue, handler_function):
        super(MessageWorker, self).__init__()

        worker_logger.debug('Creating worker!')
        self.queue = queue
        self.handler_function = handler_function

    def run(self):

        worker_logger.info('starting run()...')
        keep_going = True

        count_processed = 0
        empty_count = 0

        while keep_going:

            try:
                app, collection_name, entity = self.queue.get(timeout=60)
                empty_count = 0

                if self.handler_function is not None:
                    try:
                        processed = self.handler_function(app, collection_name, entity)

                        if processed:
                            count_processed += 1

                            worker_logger.debug('Processed [%sth] entity = %s / %s / %s' % (
                                count_processed, app, collection_name, entity.get('uuid')))

                            if count_processed % 1000 == 1:
                                worker_logger.info('Processed [%sth] entity = %s / %s / %s' % (
                                    count_processed, app, collection_name, entity.get('uuid')))

                    except KeyboardInterrupt, e:
                        raise e

                    except Exception, e:
                        print traceback.format_exc()

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                worker_logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1

                if empty_count >= 2:
                    keep_going = False

            except Exception, e:
                print traceback.format_exc()


class CollectionWorker(Process):
    def __init__(self, work_queue, entity_queue, response_queue):
        super(CollectionWorker, self).__init__()
        collection_worker_logger.debug('Creating worker!')
        self.work_queue = work_queue
        self.response_queue = response_queue
        self.entity_queue = entity_queue

    def run(self):

        collection_worker_logger.info('starting run()...')
        keep_going = True

        counter = 0
        # max_created = 0
        empty_count = 0
        app = 'ERROR'
        collection_name = 'NOT SET'
        status_map = {}
        sleep_time = 10

        try:

            while keep_going:

                try:
                    app, collection_name = self.work_queue.get(timeout=30)

                    status_map = {
                        collection_name: {
                            'iteration_started': str(datetime.datetime.now()),
                            'max_created': -1,
                            'max_modified': -1,
                            'min_created': 1584946416000,
                            'min_modified': 1584946416000,
                            'count': 0,
                            'bytes': 0
                        }
                    }

                    empty_count = 0

                    # added a flag for using graph vs query/index
                    if config.get('graph', False):
                        source_collection_url = collection_graph_url_template.format(org=config.get('org'),
                                                                                     app=app,
                                                                                     collection=collection_name,
                                                                                     **config.get('source_endpoint'))
                    else:
                        source_collection_url = collection_query_url_template.format(org=config.get('org'),
                                                                                     app=app,
                                                                                     collection=collection_name,
                                                                                     ql=config.get('ql'),
                                                                                     **config.get('source_endpoint'))

                    # use the UsergridQuery from the Python SDK to iterate the collection
                    q = UsergridQueryIterator(source_collection_url, sleep_time=config.get('page_sleep_time'))

                    for entity in q:

                        # begin entity loop

                        self.entity_queue.put((app, collection_name, entity))
                        counter += 1

                        if 'created' in entity:

                            try:
                                entity_created = long(entity.get('created'))

                                if entity_created > status_map[collection_name]['max_created']:
                                    status_map[collection_name]['max_created'] = entity_created
                                    status_map[collection_name]['max_created_str'] = str(
                                            datetime.datetime.fromtimestamp(entity_created / 1000))

                                if entity_created < status_map[collection_name]['min_created']:
                                    status_map[collection_name]['min_created'] = entity_created
                                    status_map[collection_name]['min_created_str'] = str(
                                            datetime.datetime.fromtimestamp(entity_created / 1000))

                            except ValueError:
                                pass

                        if 'modified' in entity:

                            try:
                                entity_modified = long(entity.get('modified'))

                                if entity_modified > status_map[collection_name]['max_modified']:
                                    status_map[collection_name]['max_modified'] = entity_modified
                                    status_map[collection_name]['max_modified_str'] = str(
                                            datetime.datetime.fromtimestamp(entity_modified / 1000))

                                if entity_modified < status_map[collection_name]['min_modified']:
                                    status_map[collection_name]['min_modified'] = entity_modified
                                    status_map[collection_name]['min_modified_str'] = str(
                                            datetime.datetime.fromtimestamp(entity_modified / 1000))

                            except ValueError:
                                pass

                        status_map[collection_name]['bytes'] += count_bytes(entity)
                        status_map[collection_name]['count'] += 1

                        if counter % 5000 == 1:
                            try:
                                collection_worker_logger.warning(
                                        'Sending FINAL stats for app/collection [%s / %s]: %s' % (
                                            app, collection_name, status_map))

                                self.response_queue.put((app, collection_name, status_map))

                                if QSIZE_OK:
                                    collection_worker_logger.info(
                                            'Counter=%s, queue depth=%s' % (counter, self.work_queue.qsize()))
                            except:
                                pass

                            collection_worker_logger.warn(
                                    'Current status of collections processed: %s' % json.dumps(status_map, indent=2))

                        if config.get('entity_sleep_time') > 0:
                            time.sleep(config.get('entity_sleep_time'))

                    # end entity loop

                    status_map[collection_name]['iteration_finished'] = str(datetime.datetime.now())

                    collection_worker_logger.warning(
                            'Collection [%s / %s / %s] loop complete!  Max Created entity %s' % (
                                config.get('org'), app, collection_name, status_map[collection_name]['max_created']))

                    collection_worker_logger.warning(
                            'Sending FINAL stats for app/collection [%s / %s]: %s' % (app, collection_name, status_map))

                    self.response_queue.put((app, collection_name, status_map))

                    collection_worker_logger.info('Done! Finished app/collection: %s / %s' % (app, collection_name))

                except KeyboardInterrupt, e:
                    raise e

                except Empty:
                    collection_worker_logger.warning('EMPTY! Count=%s' % empty_count)

                    empty_count += 1

                    if empty_count >= 2:
                        keep_going = False

                except Exception, e:
                    print traceback.format_exc()

        finally:
            self.response_queue.put((app, collection_name, status_map))
            collection_worker_logger.info('FINISHED!')


def migrate_out_graph_edge_type(app, collection_name, source_entity, edge_name):
    source_identifier = source_entity.get('uuid')
    response = True

    if collection_name in config.get('use_name_for_collection', []):

        if collection_name in ['users', 'user']:
            source_identifier = source_entity.get('username')
        else:
            source_identifier = source_entity.get('name')

        if source_identifier is None:
            source_identifier = source_entity.get('uuid')
            connection_logger.warn('Using UUID for entity [%s / %s / %s]' % (app, collection_name, source_identifier))
    else:
        source_identifier = source_entity.get('uuid')

    count_edges = 0

    include_edges = config.get('include_edge', [])

    if include_edges is None:
        include_edges = []

    exclude_edges = config.get('exclude_edge', [])

    if exclude_edges is None:
        exclude_edges = []

    if len(include_edges) > 0 and edge_name not in include_edges:
        connection_logger.info(
                'Skipping edge [%s] since it is not in INCLUDED list: %s' % (edge_name, include_edges))
        return False

    if edge_name in exclude_edges:
        connection_logger.info(
                'Skipping edge [%s] since it is in EXCLUDED list: %s' % (edge_name, exclude_edges))
        return False

    if (collection_name == 'devices' and edge_name in ['receipts']) or \
            (collection_name == 'receipts' and edge_name in ['devices']) or \
            (collection_name in ['users', 'user'] and edge_name in ['roles', 'followers', 'groups',
                                                                    'feed', 'activities']):
        # feed and activities are not retrievable...
        # roles and groups will be more efficiently handled from the role/group -> user
        # followers will be handled by 'following'
        # do only this from user -> device
        return False

    connection_logger.debug(
            'Processing edge type=[%s] of entity [%s / %s / %s]' % (edge_name, app, collection_name, source_identifier))

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    connection_query_url = connection_query_url_template.format(
            org=config.get('org'),
            app=app,
            verb=edge_name,
            collection=collection_name,
            uuid=source_identifier,
            **config.get('source_endpoint'))

    connection_query = UsergridQueryIterator(connection_query_url)

    connection_stack = []

    for e_connection in connection_query:
        target_connection_collection = config.get('collection_mapping', {}).get(e_connection.get('type'),
                                                                                e_connection.get('type'))

        target_ok = migrate_graph(app, e_connection.get('type'), e_connection)

        if not target_ok:
            connection_logger.critical(
                    'Error migrating TARGET entity data for connection [%s / %s / %s] --[%s]--> [%s / %s / %s]' % (
                        app, collection_name, source_identifier, edge_name, app, target_connection_collection,
                        e_connection.get('name', e_connection.get('uuid'))))

        count_edges += 1
        connection_stack.append(e_connection)

    while len(connection_stack) > 0:

        e_connection = connection_stack.pop()

        create_connection_url = connection_create_by_name_url_template.format(
                org=target_org,
                app=target_app,
                collection=target_collection,
                uuid=source_identifier,
                verb=edge_name,
                target_type=e_connection.get('type'),
                target_name=e_connection.get('name'),
                **config.get('target_endpoint'))

        if USE_CACHE:
            processed = cache.get(create_connection_url)

            if processed is not None:
                connection_logger.info('Skipping Edge: [%s / %s / %s] --[%s]--> [%s / %s / %s]: %s ' % (
                    app, collection_name, source_identifier, edge_name, target_app, e_connection.get('type'),
                    e_connection.get('name'), create_connection_url))

                response = True and response
                continue

        connection_logger.info('Connecting entity [%s / %s / %s] --[%s]--> [%s / %s / %s]: %s ' % (
            app, collection_name, source_identifier, edge_name, target_app, e_connection.get('type'),
            e_connection.get('name'), create_connection_url))

        attempts = 0

        while attempts < 5:
            attempts += 1

            r_create = session_target.post(create_connection_url)

            if r_create.status_code == 200:

                if USE_CACHE:
                    cache.set(create_connection_url, create_connection_url)

                response = True and response
                break

            elif r_create.status_code >= 500:

                if attempts < 5:
                    connection_logger.warning('FAILED (will retry) to create connection at URL=[%s]: %s' % (
                        create_connection_url, r_create.text))
                    time.sleep(DEFAULT_RETRY_SLEEP)
                else:
                    response = False
                    connection_stack = []
                    connection_logger.critical(
                            'FAILED [%s] (WILL NOT RETRY - max attempts) to create connection at URL=[%s]: %s' % (
                                r_create.status_code, create_connection_url, r_create.text))

            elif r_create.status_code in [401, 404]:
                connection_logger.critical(
                        'FAILED [%s] (WILL NOT RETRY - 401/404) to create connection at URL=[%s]: %s' % (
                            r_create.status_code, create_connection_url, r_create.text))

                response = False
                connection_stack = []

    return response


def migrate_in_graph_edge_type(app, collection_name, source_entity, edge_name):
    source_uuid = source_entity.get('uuid')
    source_identifier = source_entity.get('uuid')

    if collection_name in config.get('use_name_for_collection', []):

        if collection_name in ['users', 'user']:
            source_identifier = source_entity.get('username')
        else:
            source_identifier = source_entity.get('name')

        if source_identifier is None:
            source_identifier = source_entity.get('uuid')
            connection_logger.warn('Using UUID for entity [%s / %s / %s]' % (app, collection_name, source_identifier))
    else:
        source_identifier = source_entity.get('uuid')

    include_edges = config.get('include_edge', [])

    if include_edges is None:
        include_edges = []

    exclude_edges = config.get('exclude_edge', [])

    if exclude_edges is None:
        exclude_edges = []

    if len(include_edges) > 0 and edge_name not in include_edges:
        connection_logger.info(
                'Skipping edge [%s] since it is not in INCLUDED list: %s' % (edge_name, include_edges))
        return False

    if edge_name in exclude_edges:
        connection_logger.info(
                'Skipping edge [%s] since it is in EXCLUDED list: %s' % (edge_name, exclude_edges))
        return False

    if (collection_name == 'devices' and edge_name in ['receipts']) or \
            (collection_name == 'receipts' and edge_name in ['devices']) or \
            (collection_name in ['users', 'user'] and edge_name in ['roles', 'followers', 'groups',
                                                                    'feed', 'activities']):
        # feed and activities are not retrievable...
        # roles and groups will be more efficiently handled from the role/group -> user
        # followers will be handled by 'following'
        # do only this from user -> device
        return False

    connection_logger.debug(
            'Processing edge type=[%s] of entity [%s / %s / %s]' % (edge_name, app, collection_name, source_identifier))

    connection_logger.info('Processing IN edges type=[%s] of entity [ %s / %s / %s]' % (
        edge_name, app, collection_name, source_uuid))

    connection_query_url = connecting_query_url_template.format(
            org=config.get('org'),
            app=app,
            collection=collection_name,
            uuid=source_uuid,
            verb=edge_name,
            **config.get('source_endpoint'))

    connection_query = UsergridQueryIterator(connection_query_url)

    response = True

    for e_connection in connection_query:
        connection_logger.info('Triggering IN->OUT edge migration on entity [%s / %s / %s] ' % (
            app, e_connection.get('type'), e_connection.get('uuid')))

        response = migrate_graph(app, e_connection.get('type'), e_connection) and response

    return response


def migrate_graph(app, collection_name, source_entity):
    response = True

    source_uuid = source_entity.get('uuid')

    key = '{execution_id}:{uuid}:visited'.format(execution_id=ECID, uuid=source_uuid)

    if USE_CACHE:
        date_visited = cache.get(key)

        if date_visited is not None:
            connection_logger.info(
                    'Skipping: Already visited [%s / %s / %s] at %s' % (
                        app, collection_name, source_uuid, date_visited))
            return True
        else:
            cache.set(name=key, value=str(datetime.datetime.utcnow()))

    # migrate data for current node
    response = migrate_data(app, collection_name, source_entity)

    # for each connection name, get the connections

    out_edge_names = [edge_name for edge_name in source_entity.get('metadata', {}).get('collections', [])]
    out_edge_names += [edge_name for edge_name in source_entity.get('metadata', {}).get('connections', [])]

    for edge_name in out_edge_names:
        response = migrate_out_graph_edge_type(app, collection_name, source_entity, edge_name) and response

    in_edge_names = [edge_name for edge_name in source_entity.get('metadata', {}).get('connecting', [])]

    for edge_name in in_edge_names:
        response = migrate_in_graph_edge_type(app, collection_name, source_entity, edge_name) and response

    # migrate data for target nodes
    # migrate connections from current node
    # migrate connections to current node

    return response


def confirm_user_entity(app, source_entity, attempts=0):
    attempts += 1

    source_entity_url = get_entity_url_template.format(org=config.get('org'),
                                                       app=app,
                                                       collection='users',
                                                       uuid=source_entity.get('username'),
                                                       **config.get('source_endpoint'))

    if attempts >= 5:
        logger.error('Punting after [%s] attempts to confirm user at URL [%s], will use the source entity...' % (
            attempts, source_entity_url))

        return source_entity

    r = session_source.get(url=source_entity_url)

    if r.status_code == 200:
        retrieved_entity = r.json().get('entities')[0]

        if retrieved_entity.get('uuid') != source_entity.get('uuid'):
            logger.info(
                    'UUID of Source Entity [%s] differs from uuid [%s] of retrieved entity at URL=[%s] and will be substituted' % (
                        source_entity.get('uuid'), retrieved_entity.get('uuid'), source_entity_url))

        return retrieved_entity

    elif 'service_resource_not_found' in r.text:

        logger.warn('Unable to retrieve user at URL [%s], and will use source entity.  status=[%s] response: %s...' % (
            source_entity_url, r.status_code, r.text))

        return source_entity

    else:
        logger.error('After [%s] attempts to confirm user at URL [%s], received status [%s] message: %s...' % (
            attempts, source_entity_url, r.status_code, r.text))

        time.sleep(DEFAULT_RETRY_SLEEP)

        return confirm_user_entity(app, source_entity, attempts)


def migrate_data(app, collection_name, source_entity, attempts=0):
    if USE_CACHE:

        str_modified = cache.get(source_entity.get('uuid'))

        if str_modified is not None:

            modified = long(str_modified)

            logger.debug('FOUND CACHE: %s = %s ' % (source_entity.get('uuid'), modified))

            if modified <= source_entity.get('modified'):
                logger.info('Skipping unchanged entity: %s / %s / %s / %s' % (
                    config.get('org'), app, collection_name, source_entity.get('uuid')))
                return True
            else:
                logger.debug('DELETING CACHE: %s ' % (source_entity.get('uuid')))
                cache.delete(source_entity.get('uuid'))

    # handle duplicate user case
    if collection_name in ['users', 'user']:
        source_entity = confirm_user_entity(app, source_entity)

    response = False

    attempts += 1

    if 'metadata' in source_entity:
        source_entity.pop('metadata')

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    entity_id = source_entity.get('uuid')

    if source_entity.get('type') in config.get('use_name_for_collection', []):
        entity_id = source_entity.get('name')

    try:
        target_entity_url_by_name = put_entity_url_template.format(org=target_org,
                                                                   app=target_app,
                                                                   collection=target_collection,
                                                                   uuid=entity_id,
                                                                   **config.get('target_endpoint'))

        if attempts >= 5:
            data_logger.critical(
                    'ABORT migrate_data | success=[%s] | attempts=[%s] | created=[%s] | modified=[%s] %s / %s / %s' % (
                        True, attempts, source_entity.get('created'), source_entity.get('modified'), app,
                        collection_name, entity_id))

            return False

        if attempts > 1:
            logger.warn('Attempt [%s] to migrate entity [%s / %s] at URL [%s]' % (
                attempts, collection_name, entity_id, target_entity_url_by_name))
        else:
            logger.debug('Attempt [%s] to migrate entity [%s / %s] at URL [%s]' % (
                attempts, collection_name, entity_id, target_entity_url_by_name))

        r = session_target.put(url=target_entity_url_by_name, data=json.dumps(source_entity))

        if r.status_code == 200:
            # Worked => WE ARE DONE
            data_logger.info(
                    'migrate_data | success=[%s] | attempts=[%s] | entity=[%s / %s / %s] | created=[%s] | modified=[%s]' % (
                        True, attempts, config.get('org'), app, entity_id, source_entity.get('created'),
                        source_entity.get('modified'),))

            if USE_CACHE:
                data_logger.debug('SETTING CACHE | uuid=[%s] | modified=[%s]' % (
                    entity_id, str(source_entity.get('modified'))))

                cache.set(entity_id, str(source_entity.get('modified')))

            return True

        else:
            data_logger.error('Failure [%s] on attempt [%s] to PUT url=[%s], entity=[%s] response=[%s]' % (
                r.status_code, attempts, target_entity_url_by_name, json.dumps(source_entity), r.text))

            if r.status_code == 400:

                if target_collection in ['roles', 'role']:
                    return repair_user_role(app, collection_name, source_entity)

                elif target_collection in ['users', 'user']:
                    return handle_user_migration_conflict(app, collection_name, source_entity)

                elif 'duplicate_unique_property_exists' in r.text:
                    data_logger.error('WILL NOT RETRY (duplicate) [%s] attempts to PUT url=[%s], entity=[%s] response=[%s]' % (
                        attempts, target_entity_url_by_name, json.dumps(source_entity), r.text))

                    return False

    except:
        data_logger.error(traceback.format_exc())
        data_logger.error('error in migrate_data on entity: %s' % json.dumps(source_entity))

    return migrate_data(app, collection_name, source_entity, attempts)


def audit_data(app, collection_name, source_entity, attempts=0):
    if collection_name in ['users', 'user']:
        return audit_user(app, collection_name, source_entity, attempts)

    # simple process - check the target org/app/collection/{uuid|name} to see if it exists.
    # If it exists, move on.  If not, and repair is enabled then attempt to migrate it now and reattempt audit

    attempts += 1

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    target_entity_url = get_entity_url_template.format(org=target_org,
                                                       app=target_app,
                                                       collection=target_collection,
                                                       uuid=source_entity.get('uuid'),
                                                       **config.get('target_endpoint'))

    r = session_target.get(url=target_entity_url)

    # short circuit after 5 attempts
    if attempts >= 5:
        logger.critical('ABORT after [%s] attempts to audit entity [%s / %s] at URL [%s]' % (
            attempts, collection_name, source_entity.get('uuid'), target_entity_url))
        return False

    if r.status_code == 200:
        # entity found, audit success
        return True

    else:
        audit_logger.warning('AUDIT: Repair=[%s] Entity not found on attempt [%s] at TARGET URL=[%s] - [%s]: %s' % (
            config.get('repair'), attempts, target_entity_url, r.status_code, r.text))

        # if repaid is enabled, attempt to migrate the data followed by an audit
        if config.get('repair', False):

            response = migrate_data(app, collection_name, source_entity, attempts=0)

            if response:
                # migration success, sleep for processing
                time.sleep(DEFAULT_PROCESSING_SLEEP)

                # attempt a reaudit after migration
                return audit_data(app, collection_name, source_entity, attempts)

            else:
                audit_logger.critical(
                        'AUDIT: ABORT after attempted migration and entity not found on attempt [%s] at TARGET URL=[%s] - [%s]: %s' % (
                            attempts, target_entity_url, r.status_code, r.text))

    return audit_data(app, collection_name, source_entity, attempts)


def audit_user(app, collection_name, source_entity, attempts=0):
    if collection_name in ['users', 'user']:
        return False

    attempts += 1
    username = source_entity.get('username')
    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    target_entity_url = get_entity_url_template.format(org=target_org,
                                                       app=target_app,
                                                       collection=target_collection,
                                                       uuid=username,
                                                       **config.get('target_endpoint'))

    # There is retry build in, here is the short circuit
    if attempts >= 5:
        logger.critical(
                'Aborting after [%s] attempts to audit user [%s] at URL [%s]' % (attempts, username, target_entity_url))

        return False

    r = session_target.get(url=target_entity_url)

    # check to see if the entity is at the target by username
    # if the entity is there, check that the UUID is the same
    # if the UUID is not the same, then check the created date
    # If the created date of the target is after the created date of the source, then overwrite it with the entity in
    # the parameters

    if r.status_code != 200:
        audit_logger.info(
                'Failed attempt [%s] to GET [%s] URL=[%s]: %s' % (attempts, r.status_code, target_entity_url, r.text))

    if r.status_code == 200:
        target_entity = r.json().get('entities')[0]

        best_source_entity = get_best_source_entity(app, collection_name, source_entity)

        # compare uuids to make sure they match

        if target_entity.get('uuid') == best_source_entity.get('uuid') or target_entity.get(
                'created') <= best_source_entity.get('created'):

            if target_entity.get('uuid') != best_source_entity.get('uuid'):
                # UUIDs match and/or created at target is less than best we know of, audit complete
                audit_logger.info('GOOD user [%s] best/source UUID/created=[%s / %s] target UUID/created [%s / %s]' % (
                    username, best_source_entity.get('uuid'), best_source_entity.get('created'),
                    target_entity.get('uuid'),
                    target_entity.get('created')))

            return True

        else:
            audit_logger.warn('Repairing user [%s] best/source UUID/created=[%s / %s] target UUID/created [%s / %s]' % (
                username, best_source_entity.get('uuid'), best_source_entity.get('created'), target_entity.get('uuid'),
                target_entity.get('created')))

            return repair_user_role(app, collection_name, source_entity)

    elif r.status_code / 100 == 4:
        audit_logger.info('AUDIT: HTTP [%s] on URL=[%s]: %s' % (r.status_code, target_entity_url, r.text))

        return repair_user_role(app, collection_name, source_entity)

    else:
        audit_logger.warning('AUDIT: Failed attempt [%s] GET [%s] on TARGET URL=[%s] - : %s' % (
            attempts, r.status_code, target_entity_url, r.text))

        time.sleep(DEFAULT_RETRY_SLEEP)

        return audit_data(app, collection_name, source_entity, attempts)


def handle_user_migration_conflict(app, collection_name, source_entity, attempts=0):
    if collection_name in ['users', 'user']:
        return False

    attempts += 1
    username = source_entity.get('username')
    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    target_entity_url = get_entity_url_template.format(org=target_org,
                                                       app=target_app,
                                                       collection=target_collection,
                                                       uuid=username,
                                                       **config.get('target_endpoint'))

    # There is retry build in, here is the short circuit
    if attempts >= 5:
        logger.critical(
                'Aborting after [%s] attempts to audit user [%s] at URL [%s]' % (attempts, username, target_entity_url))

        return False

    r = session_target.get(url=target_entity_url)

    if r.status_code == 200:
        target_entity = r.json().get('entities')[0]

        if source_entity.get('created') < target_entity.get('created'):
            return repair_user_role(app, collection_name, source_entity)

    elif r.status_code / 100 == 5:
        audit_logger.warning(
                'CONFLICT: handle_user_migration_conflict failed attempt [%s] GET [%s] on TARGET URL=[%s] - : %s' % (
                    attempts, r.status_code, target_entity_url, r.text))

        time.sleep(DEFAULT_RETRY_SLEEP)

        return handle_user_migration_conflict(app, collection_name, source_entity, attempts)

    else:
        audit_logger.error(
                'CONFLICT: Failed handle_user_migration_conflict attempt [%s] GET [%s] on TARGET URL=[%s] - : %s' % (
                    attempts, r.status_code, target_entity_url, r.text))

        return False


def get_best_source_entity(app, collection_name, source_entity):
    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    target_pk = 'uuid'

    if target_collection in ['users', 'user']:
        target_pk = 'username'
    elif target_collection in ['roles', 'role']:
        target_pk = 'name'

    target_name = source_entity.get(target_pk)

    # there should be no target entity now, we just need to decide which one from the source to use
    source_entity_url_by_name = get_entity_url_template.format(org=config.get('org'),
                                                               app=app,
                                                               collection=collection_name,
                                                               uuid=target_name,
                                                               **config.get('source_endpoint'))

    r_get_source_entity = session_source.get(source_entity_url_by_name)

    # if we are able to get at the source by PK...
    if r_get_source_entity.status_code == 200:

        # extract the entity from the response
        entity_from_get = r_get_source_entity.json().get('entities')[0]

        return entity_from_get

    elif r_get_source_entity.status_code / 100 == 4:
        # wasn't found, get by QL and sort
        source_entity_query_url = collection_query_url_template.format(org=config.get('org'),
                                                                       app=app,
                                                                       collection=collection_name,
                                                                       ql='select * where %s=\'%s\' order by created asc' % (
                                                                           target_pk, target_name),
                                                                       **config.get('source_endpoint'))

        data_logger.info('Attempting to determine best entity from query on URL %s' % source_entity_query_url)

        q = UsergridQueryIterator(source_entity_query_url)

        desired_entity = None

        entity_counter = 0

        for e in q:
            entity_counter += 1

            if desired_entity is None:
                desired_entity = e

            elif e.get('created') < desired_entity.get('created'):
                desired_entity = e

        if desired_entity is None:
            data_logger.warn('Unable to determine best of [%s] entities from query on URL %s' % (
                entity_counter, source_entity_query_url))

            return source_entity

        else:
            return desired_entity

    else:
        return source_entity


def repair_user_role(app, collection_name, source_entity, attempts=0):

    if not config.get('repair', False):
        return False

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    # For the users collection, there seemed to be cases where a USERNAME was created/existing with the a
    # different UUID which caused a 'collision' - so the point is to delete the entity with the differing
    # UUID by UUID and then do a recursive call to migrate the data - now that the collision has been cleared

    target_pk = 'uuid'

    if target_collection in ['users', 'user']:
        target_pk = 'username'
    elif target_collection in ['roles', 'role']:
        target_pk = 'name'

    target_name = source_entity.get(target_pk)

    target_entity_url_by_name = get_entity_url_template.format(org=target_org,
                                                               app=target_app,
                                                               collection=target_collection,
                                                               uuid=target_name,
                                                               **config.get('target_endpoint'))

    data_logger.warning('Repairing: Deleting name=[%s] entity at URL=[%s]' % (target_name, target_entity_url_by_name))

    r = session_target.delete(target_entity_url_by_name)

    if r.status_code == 200 or (r.status_code in [404, 401] and 'service_resource_not_found' in r.text):
        data_logger.info('Deletion of entity at URL=[%s] was [%s]' % (target_entity_url_by_name, r.status_code))

        best_source_entity = get_best_source_entity(app, collection_name, source_entity)

        target_entity_url_by_uuid = get_entity_url_template.format(org=target_org,
                                                                   app=target_app,
                                                                   collection=target_collection,
                                                                   uuid=best_source_entity.get('uuid'),
                                                                   **config.get('target_endpoint'))

        r = session_target.put(target_entity_url_by_uuid, data=json.dumps(best_source_entity))

        if r.status_code == 200:
            data_logger.info('Successfully repaired user at URL=[%s]' % target_entity_url_by_uuid)
            return True

        else:
            data_logger.critical('Failed to PUT [%s] the desired entity  at URL=[%s]: %s' % (
                r.status_code, target_entity_url_by_name, r.text))
            return False

    else:
        # log an error and keep going if we cannot delete the entity at the specified URL.  Unlikely, but if so
        # then this entity is borked
        data_logger.critical(
                'Deletion of entity at URL=[%s] FAILED [%s]: %s' % (target_entity_url_by_name, r.status_code, r.text))
        return False


def get_target_mapping(app, collection_name):
    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
    target_app = config.get('app_mapping', {}).get(app, app)
    target_collection = config.get('collection_mapping', {}).get(collection_name, collection_name)
    return target_app, target_collection, target_org


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid Org/App Migrator')

    parser.add_argument('-o', '--org',
                        help='Name of the org to migrate',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='Name of one or more apps to include, specify none to include all apps',
                        required=False,
                        action='append')

    parser.add_argument('-e', '--include_edge',
                        help='Name of one or more edges/connection types to INCLUDE, specify none to include all edges',
                        required=False,
                        action='append')

    parser.add_argument('--exclude_edge',
                        help='Name of one or more edges/connection types to EXCLUDE, specify none to include all edges',
                        required=False,
                        action='append')

    parser.add_argument('--exclude_collection',
                        help='Name of one or more collections to EXCLUDE, specify none to include all collections',
                        required=False,
                        action='append')

    parser.add_argument('-c', '--collection',
                        help='Name of one or more collections to include, specify none to include all collections',
                        default=[],
                        action='append')

    parser.add_argument('--use_name_for_collection',
                        help='Name of one or more collections to use [name] instead of [uuid] for creating entities and edges',
                        default=[],
                        action='append')

    parser.add_argument('-m', '--migrate',
                        help='Specifies what to migrate: data, connections, credentials, audit or none (just iterate '
                             'the apps/collections)',
                        type=str,
                        choices=['data', 'none', 'graph'],
                        default='data')

    parser.add_argument('-s', '--source_config',
                        help='The path to the source endpoint/org configuration file',
                        type=str,
                        default='source.json')

    parser.add_argument('-d', '--target_config',
                        help='The path to the target endpoint/org configuration file',
                        type=str,
                        default='destination.json')

    parser.add_argument('-w', '--entity_workers',
                        help='The number of worker processes to do the migration',
                        type=int,
                        default=16)

    parser.add_argument('--page_sleep_time',
                        help='The number of seconds to wait between retrieving pages from the UsergridQueryIterator',
                        type=float,
                        default=.5)

    parser.add_argument('--entity_sleep_time',
                        help='The number of seconds to wait between retrieving pages from the UsergridQueryIterator',
                        type=float,
                        default=.5)

    parser.add_argument('--collection_workers',
                        help='The number of worker processes to do the migration',
                        type=int,
                        default=2)

    parser.add_argument('--queue_size_max',
                        help='The max size of entities to allow in the queue',
                        type=int,
                        default=100000)

    parser.add_argument('--queue_watermark_high',
                        help='The point at which publishing to the queue will PAUSE until it is at or below low watermark',
                        type=int,
                        default=25000)

    parser.add_argument('--min_modified',
                        help='Break when encountering a modified date before this, per collection',
                        type=int,
                        default=0)

    parser.add_argument('--max_modified',
                        help='Break when encountering a modified date after this, per collection',
                        type=long,
                        default=3793805526000)

    parser.add_argument('--queue_watermark_low',
                        help='The point at which publishing to the queue will RESUME after it has reached the high watermark',
                        type=int,
                        default=5000)

    parser.add_argument('--ql',
                        help='The QL to use in the filter for reading data from collections',
                        type=str,
                        default='select * order by created asc')
    # default='select * order by created asc')

    parser.add_argument('--create_apps',
                        dest='create_apps',
                        action='store_true')

    parser.add_argument('--with_data',
                        dest='with_data',
                        action='store_true')

    parser.add_argument('--nohup',
                        dest='specifies not to use stdout for logging',
                        action='store_true')

    parser.add_argument('--repair',
                        help='Attempt to migrate missing data',
                        dest='repair',
                        action='store_true')

    parser.add_argument('--graph',
                        help='Use GRAPH instead of Query',
                        dest='graph',
                        action='store_true')

    parser.add_argument('--su_username',
                        help='Superuser username',
                        required=False,
                        type=str)

    parser.add_argument('--su_password',
                        help='Superuser Password',
                        required=False,
                        type=str)

    parser.add_argument('--inbound_connections',
                        help='Name of the org to migrate',
                        action='store_true')

    parser.add_argument('--map_app',
                        help="Multiple allowed: A colon-separated string such as 'apples:oranges' which indicates to"
                             " put data from the app named 'apples' from the source endpoint into app named 'oranges' "
                             "in the target endpoint",
                        default=[],
                        action='append')

    parser.add_argument('--map_collection',
                        help="One or more colon-separated string such as 'cats:dogs' which indicates to put data from "
                             "collections named 'cats' from the source endpoint into a collection named 'dogs' in the "
                             "target endpoint, applicable globally to all apps",
                        default=[],
                        action='append')

    parser.add_argument('--map_org',
                        help="One or more colon-separated strings such as 'red:blue' which indicates to put data from "
                             "org named 'red' from the source endpoint into a collection named 'blue' in the target "
                             "endpoint",
                        default=[],
                        action='append')

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


def init():
    global config

    if config.get('migrate') == 'credentials':

        if config.get('su_password') is None or config.get('su_username') is None:
            message = 'In order to migrate credentials, Superuser parameters (su_password, su_username) are required'
            logger.critical(message)
            exit()

    config['collection_mapping'] = {}
    config['app_mapping'] = {}
    config['org_mapping'] = {}

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
            logger.info('Mapping Org [%s] to [%s] from mapping [%s]' % (parts[0], parts[1], mapping))
        else:
            logger.warning('Skipping Org mapping: [%s]' % mapping)

    with open(config.get('source_config'), 'r') as f:
        config['source_config'] = json.load(f)

    with open(config.get('target_config'), 'r') as f:
        config['target_config'] = json.load(f)

    config['source_endpoint'] = config['source_config'].get('endpoint').copy()
    config['source_endpoint'].update(config['source_config']['credentials'][config['org']])

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))

    config['target_endpoint'] = config['target_config'].get('endpoint').copy()
    config['target_endpoint'].update(config['target_config']['credentials'][target_org])


def wait_for(threads, sleep_time=60):
    wait = True

    logger.info('Starting to wait for [%s] threads with sleep time=[%s]' % (len(threads), sleep_time))

    while wait:
        wait = False
        alive_count = 0

        for t in threads:

            if t.is_alive():
                alive_count += 1
                logger.info('Thread [%s] is still alive' % t.name)

        if alive_count > 0:
            wait = True
            logger.info('Continuing to wait for [%s] threads with sleep time=[%s]' % (alive_count, sleep_time))
            time.sleep(sleep_time)


def get_token(endpoint_config):
    token_request = {}

    pass


def count_bytes(entity):
    entity_copy = entity.copy()

    if 'metadata' in entity_copy:
        del entity_copy['metadata']

    entity_str = json.dumps(entity_copy)

    return len(entity_str)


def main():
    global config

    config = parse_args()
    init()

    init_logging()

    status_map = {}

    org_apps = {
    }

    if len(org_apps) == 0:
        try:
            # get source token
            # token = get_token(config['source_config'])

            # list the apps for the SOURCE org
            source_org_mgmt_url = org_management_url_template.format(org=config.get('org'),
                                                                     **config.get('source_endpoint'))

            # logger.info('GET %s' % source_org_mgmt_url)
            r = session_target.get(source_org_mgmt_url)

            check_response_status(r, source_org_mgmt_url)

            logger.info(json.dumps(r.json()))

            org_apps = r.json().get('data')

        except:
            org_apps = {'foo/favourites': 'foo'}

    entity_queue = Queue(maxsize=config.get('queue_size_max'))
    collection_queue = Queue(maxsize=config.get('queue_size_max'))
    collection_response_queue = Queue(maxsize=config.get('queue_size_max'))

    # Check the specified configuration for what to migrate/audit
    if config.get('migrate') == 'graph':
        operation = migrate_graph
    elif config.get('migrate') == 'data':
        operation = migrate_data
    else:
        operation = None

    logger.info('Starting entity_workers...')

    status_listener = StatusListener(collection_response_queue, entity_queue)
    status_listener.start()

    # start the worker processes which will do the work of migrating
    entity_workers = [MessageWorker(entity_queue, operation) for x in xrange(config.get('entity_workers'))]
    [w.start() for w in entity_workers]

    # start the worker processes which will iterate the collections
    collection_workers = [CollectionWorker(collection_queue, entity_queue, collection_response_queue) for x in
                          xrange(config.get('collection_workers'))]
    [w.start() for w in collection_workers]

    try:
        apps_to_process = config.get('app')
        collections_to_process = config.get('collection')

        # iterate the apps retrieved from the org
        for org_app in sorted(org_apps.keys()):
            logger.info('Found SOURCE App: %s' % org_app)

        time.sleep(3)

        for org_app in sorted(org_apps.keys()):
            max_created = -1
            parts = org_app.split('/')
            app = parts[1]

            # if apps are specified and the current app is not in the list, skip it
            if apps_to_process and len(apps_to_process) > 0 and app not in apps_to_process:
                logger.warning('Skipping app [%s] not included in process list [%s]' % (app, apps_to_process))
                continue

            logger.info('Processing app=[%s]' % app)

            status_map[app] = {
                'iteration_started': str(datetime.datetime.now()),
                'max_created': -1,
                'max_modified': -1,
                'min_created': 1584946416000,
                'min_modified': 1584946416000,
                'count': 0,
                'bytes': 0,
                'collections': {}
            }

            # it is possible to map source orgs and apps to differently named targets.  This gets the
            # target names for each
            target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
            target_app = config.get('app_mapping', {}).get(app, app)

            # Check that the target Org/App exists.  If not, move on to the next
            target_app_url = app_url_template.format(org=target_org,
                                                     app=target_app,
                                                     **config.get('target_endpoint'))
            logger.info('GET %s' % target_app_url)
            r_target_apps = session_target.get(target_app_url)

            if r_target_apps.status_code != 200:

                if config.get('create_apps', DEFAULT_CREATE_APPS):
                    create_app_url = org_management_app_url_template.format(org=target_org,
                                                                            app=target_app,
                                                                            **config.get('target_endpoint'))
                    app_request = {'name': target_app}
                    r = session_target.post(create_app_url, data=json.dumps(app_request))

                    if r.status_code != 200:
                        logger.critical(
                                'Unable to create app [%s] at URL=[%s]: %s' % (target_app, create_app_url, r.text))
                        continue
                    else:
                        logger.warning('Created app=[%s] at URL=[%s]: %s' % (target_app, create_app_url, r.text))
                else:
                    logger.critical(
                            'Target application does not exist at [%s] URL=%s' % (
                                r_target_apps.status_code, target_app_url))
                    continue

            # get the list of collections from the source org/app
            source_app_url = app_url_template.format(org=config.get('org'),
                                                     app=app,
                                                     **config.get('source_endpoint'))
            logger.info('GET %s' % source_app_url)

            r_collections = session_source.get(source_app_url)

            collection_attempts = 0

            # sometimes this call was not working so I put it in a loop to force it...
            while r_collections.status_code != 200 and collection_attempts < 5:
                collection_attempts += 1
                logger.warning('FAILED: GET (%s) [%s] URL: %s' % (r_collections.elapsed, r_collections.status_code,
                                                                  source_app_url))
                time.sleep(DEFAULT_RETRY_SLEEP)
                r_collections = session_source.get(source_app_url)

            if collection_attempts >= 5:
                logger.critical('Unable to get collections at URL %s, skipping app' % source_app_url)
                continue

            app_response = r_collections.json()

            logger.info('App Response: ' + json.dumps(app_response))

            app_entities = app_response.get('entities', [])

            if len(app_entities) > 0:
                app_entity = app_entities[0]
                collections = app_entity.get('metadata', {}).get('collections', {})
                logger.info('Collection List: %s' % collections)

                # iterate the collections which are returned.
                for collection_name, collection_data in collections.iteritems():
                    exclude_collections = config.get('exclude_collection', [])

                    if exclude_collections is None:
                        exclude_collections = []

                    # filter out collections as configured...
                    if collection_name in ignore_collections \
                            or (len(collections_to_process) > 0 and collection_name not in collections_to_process) \
                            or (len(exclude_collections) > 0 and collection_name in exclude_collections) \
                            or (config.get('migrate') == 'credentials' and collection_name != 'users'):
                        logger.warning('Skipping collection=[%s]' % collection_name)

                        continue

                    logger.info('Publishing app / collection: %s / %s' % (app, collection_name))

                    collection_queue.put((app, collection_name))

            status_map[app]['iteration_finished'] = str(datetime.datetime.now())

            logger.info('Finished publishing collections for app [%s] !' % app)

        # allow collection workers to finish
        wait_for(collection_workers, sleep_time=30)

        # allow entity workers to finish
        wait_for(entity_workers, sleep_time=30)

    except KeyboardInterrupt:
        logger.warning('Keyboard Interrupt, aborting...')
        entity_queue.close()
        collection_queue.close()
        collection_response_queue.close()

        [os.kill(super(MessageWorker, p).pid, signal.SIGINT) for p in entity_workers]
        [os.kill(super(CollectionWorker, p).pid, signal.SIGINT) for p in collection_workers]
        os.kill(super(StatusListener, status_listener).pid, signal.SIGINT)

        [w.terminate() for w in entity_workers]
        [w.terminate() for w in collection_workers]
        status_listener.terminate()

    logger.info('entity_workers DONE!')

    # aggregate results from collection iteration


def check_response_status(r, url, exit_on_error=True):
    if r.status_code != 200:
        logger.critical('HTTP [%s] on URL=[%s]' % (r.status_code, url))
        logger.critical('Response: %s' % r.text)

        if exit_on_error:
            exit()


if __name__ == "__main__":
    main()
