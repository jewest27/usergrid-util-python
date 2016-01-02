from Queue import Empty
import argparse
import json
import logging
import sys
from multiprocessing import Queue, Process
import requests
import traceback
from logging.handlers import RotatingFileHandler
import time
from usergrid import UsergridQuery
from requests.auth import HTTPBasicAuth

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('DataMigrator')
error_logger = logging.getLogger('MigrationErrors')
audit_logger = logging.getLogger('AuditErrors')

import urllib3

urllib3.disable_warnings()

CREATE_APPS = False


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()

    # base log file

    log_file_name = './data_migrator.log'
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

    # ERROR LOG

    error_log_file_name = './data_migrator_errors.log'
    error_rotating_file = logging.handlers.RotatingFileHandler(filename=error_log_file_name,
                                                               mode='a',
                                                               maxBytes=2048576000,
                                                               backupCount=10)
    error_rotating_file.setFormatter(log_formatter)
    error_rotating_file.setLevel(logging.ERROR)

    root_logger.addHandler(error_rotating_file)

    # AUDIT LOG

    audit_log_file_name = './data_migrator_audit.log'
    audit_rotating_file = logging.handlers.RotatingFileHandler(filename=audit_log_file_name,
                                                               mode='a',
                                                               maxBytes=2048576000,
                                                               backupCount=10)
    audit_rotating_file.setFormatter(log_formatter)
    audit_rotating_file.setLevel(logging.WARNING)

    audit_logger.addHandler(audit_rotating_file)

    root_logger.setLevel(logging.INFO)

    # overrides

    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


entity_name_map = {
    'users': 'username'
}

config = {}

org_management_app_url_template = "{api_url}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_management_url_template = "{api_url}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_url_template = "{api_url}/{org}?client_id={client_id}&client_secret={client_secret}"
app_url_template = "{api_url}/{org}/{app}?client_id={client_id}&client_secret={client_secret}"
collection_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}"
collection_query_url_template = "{api_url}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"
collection_graph_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
connection_query_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
connection_create_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}/{target_uuid}?client_id={client_id}&client_secret={client_secret}"
get_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}&connections=none"

get_entity_url_with_connections_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"
put_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"

user_credentials_url_template = "{api_url}/{org}/{app}/users/{uuid}/credentials"

ignore_collections = ['activities', 'queues', 'events', 'notifications']


class Worker(Process):
    def __init__(self, queue, handler_function):
        super(Worker, self).__init__()
        logger.debug('Creating worker!')
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
                        logger.debug('Processed [%sth] entity = %s/%s/%s' % (
                            count_processed, app, collection_name, entity.get('uuid')))

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1
                if empty_count < 10:
                    keep_going = False


def migrate_connections(app, collection_name, source_entity, attempts=0):
    attempts += 1
    response = False

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    source_uuid = source_entity.get('uuid')
    count_edges = 0
    count_edge_names = 0

    try:
        connections = source_entity.get('metadata', {}).get('collections', {})

        count_edge_names = len(connections)

        logger.info(
                'Processing [%s] connections of entity [%s/%s/%s]' % (
                    count_edge_names, target_app, collection_name, source_uuid))

        for connection_name in connections:

            if connection_name in ['feed', 'activities']:
                continue

            logger.info(
                    'Processing connections [%s] of entity [%s/%s/%s]' % (
                        connection_name, app, collection_name, source_uuid))
            connection_query_url = connection_query_url_template.format(
                    org=config.get('org'),
                    app=app,
                    verb=connection_name,
                    collection=collection_name,
                    uuid=source_uuid,
                    **config.get('source_endpoint'))

            connection_query = UsergridQuery(connection_query_url)

            connection_stack = []

            # TODO: This log message does not take into account the target collection

            for e_connection in connection_query:
                logger.info('Connecting entity [%s/%s/%s] --[%s]--> [%s/%s/%s]' % (
                    app, collection_name, source_uuid, connection_name, target_app, e_connection.get('type'),
                    e_connection.get('uuid')))

                count_edges += 1
                connection_stack.append(e_connection)

            while len(connection_stack) > 0:

                e_connection = connection_stack.pop()

                create_connection_url = connection_create_url_template.format(
                        org=target_org,
                        app=target_app,
                        verb=connection_name,
                        collection=target_collection,
                        uuid=source_uuid,
                        target_uuid=e_connection.get('uuid'),
                        **config.get('target_endpoint'))

                logger.info('CREATE: ' + create_connection_url)
                attempts = 0

                while attempts < 5:
                    attempts += 1

                    r_create = requests.post(create_connection_url)

                    if r_create.status_code == 200:
                        response = True
                        break

                    elif r_create.status_code == 401:
                        logger.warning(
                                'FAILED to create connection at URL=[%s]: %s' % (create_connection_url, r_create.text))

                        if attempts < 5:
                            logger.warning('WILL Retry')
                            time.sleep(10)
                        else:
                            response = False
                            logger.critical(
                                    'WILL NOT RETRY: FAILED to create connection at URL=[%s]: %s' % (
                                        create_connection_url, r_create.text))

        response = True

    except:
        logger.error(traceback.format_exc())
        logger.error('error in migrate_connections on entity: \n %s' % json.dumps(source_entity, indent=2))

    if (count_edges + count_edge_names) > 0:
        logger.info('migrate_connections | success=[%s] | entity = %s/%s/%s | edge_types=[%s] | edges=[%s]' % (
            response, app, collection_name, source_entity.get('uuid'), count_edge_names, count_edges))

    return response


def migrate_connections_deep(app, collection_name, source_entity, attempts=0):
    attempts += 1
    response = False

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    source_uuid = source_entity.get('uuid')
    count_edges = 0
    count_edge_names = 0

    try:
        connections = source_entity.get('metadata', {}).get('collections', {})

        count_edge_names = len(connections)

        logger.info(
                'Processing [%s] connections of entity [%s/%s/%s]' % (
                    count_edge_names, target_app, collection_name, source_uuid))

        for connection_name in connections:

            if connection_name in ['feed', 'activities']:
                continue

            logger.info(
                    'Processing connections [%s] of entity [%s/%s]' % (connection_name, collection_name, source_uuid))
            connection_query_url = connection_query_url_template.format(
                    org=config.get('org'),
                    app=app,
                    verb=connection_name,
                    collection=collection_name,
                    uuid=source_uuid,
                    **config.get('source_endpoint'))

            connection_query = UsergridQuery(connection_query_url)

            connection_stack = []

            for e_connection in connection_query:
                logger.info('Connecting entity [%s/%s] --[%s]--> [%s/%s]' % (
                    collection_name, source_uuid, connection_name, e_connection.get('type'),
                    e_connection.get('uuid')))
                count_edges += 1
                connection_stack.append(e_connection)

            while len(connection_stack) > 0:

                e_connection = connection_stack.pop()

                create_connection_url = connection_create_url_template.format(
                        org=target_org,
                        app=target_app,
                        verb=connection_name,
                        collection=target_collection,
                        uuid=source_uuid,
                        target_uuid=e_connection.get('uuid'),
                        **config.get('target_endpoint'))

                logger.info('CREATE: ' + create_connection_url)
                attempts = 0

                while attempts < 5:
                    attempts += 1

                    r_create = requests.post(create_connection_url)

                    if r_create.status_code == 200:
                        response = True
                        break

                    elif r_create.status_code == 401:
                        logger.warning(
                                'FAILED to create connection at URL=[%s]: %s' % (create_connection_url, r_create.text))

                        if attempts < 5:
                            logger.warning('WILL Retry')
                            time.sleep(10)
                        else:
                            response = False
                            logger.critical(
                                    'WILL NOT RETRY: FAILED to create connection at URL=[%s]: %s' % (
                                        create_connection_url, r_create.text))

        response = True

    except:
        logger.error(traceback.format_exc())
        logger.error('error in migrate_connections_deep on entity: \n %s' % json.dumps(source_entity, indent=2))

    if (count_edges + count_edge_names) > 0:
        logger.info('migrate_connections_deep | success=[%s] | entity = %s/%s/%s | edge_types=[%s] | edges=[%s]' % (
            response, app, collection_name, source_entity.get('uuid'), count_edge_names, count_edges))

    return response


def migrate_user_credentials(app, collection_name, source_entity, attempts=0):
    if collection_name != 'users':
        return False

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    source_url = user_credentials_url_template.format(org=config.get('org'),
                                                      app=app,
                                                      uuid=source_entity.get('uuid'),
                                                      **config.get('source_endpoint'))

    target_url = user_credentials_url_template.format(org=target_org,
                                                      app=target_app,
                                                      uuid=source_entity.get('uuid'),
                                                      **config.get('target_endpoint'))

    r = requests.get(source_url, auth=HTTPBasicAuth(config.get('su_username'), config.get('su_password')))

    if r.status_code != 200:
        logger.critical(
                'Unable to migrate credentials due to HTTP [%s] on GET URL [%s]: %s' % (
                    r.status_code, source_url, r.text))
        return False

    source_credentials = r.json()

    logger.info('Putting credentials to [%s]...' % target_url)

    r = requests.put(target_url,
                     data=json.dumps(source_credentials),
                     auth=HTTPBasicAuth(config.get('su_username'), config.get('su_password')))

    if r.status_code != 200:
        logger.critical(
                'Unable to migrate credentials due to HTTP [%s] on PUT URL [%s]: %s' % (
                    r.status_code, target_url, r.text))
        return False

    logger.info('migrate_user_credentials | success=[%s] | app/collection/name = %s/%s/%s' % (
        True, app, collection_name, source_entity.get('uuid')))

    # logger.info(r.text)
    return True


def migrate_data(app, collection_name, source_entity, attempts=0):
    response = False

    attempts += 1

    if 'metadata' in source_entity: source_entity.pop('metadata')

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    # GET_FIRST = False
    #
    # if GET_FIRST:
    #     source_entity_url = get_entity_url_template.format(org=config.get('org'),
    #                                                        app=app,
    #                                                        collection=collection_name,
    #                                                        uuid=source_entity.get('uuid'),
    #                                                        **config.get('source_endpoint'))
    #
    #     r = requests.get(url=source_entity_url)
    #
    #     if r.status_code != 200:
    #         logger.warning('Entity at URL=[%s] is not retrievable!' % source_entity_url)
    #
    #         try:
    #             bad_response = r.json()
    #             if bad_response.get('error') != 'service_resource_not_found':
    #                 logger.warning(r.text)
    #         except:
    #             logger.warning(r.text)
    #
    #         return False

    try:
        target_entity_url = put_entity_url_template.format(org=target_org,
                                                           app=target_app,
                                                           collection=target_collection,
                                                           uuid=source_entity.get('uuid'),
                                                           **config.get('target_endpoint'))

        r = requests.put(url=target_entity_url, data=json.dumps(source_entity))

        if r.status_code == 200:
            # Worked == WE ARE DONE
            response = True

        elif r.status_code in [401, 404]:
            # NOT SURE THIS CODE PATH IS VALID as the array on line above used to have strings, and it would never match
            # r.status code which is an int.

            target_collection_url = collection_url_template.format(org=target_org,
                                                                   app=target_app,
                                                                   collection=target_collection,
                                                                   **config.get('target_endpoint'))

            r = requests.post(target_collection_url, json.dumps(source_entity))

            if r.status_code >= 500:

                if attempts <= 3:
                    logger.warn('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                        r.status_code, attempts, target_collection_url, json.dumps(source_entity), r.json()))
                    logger.warn('Sleeping before retry...')
                    time.sleep(5)
                    return migrate_data(app, collection_name, source_entity, attempts)

                else:
                    logger.critical(
                            'WILL NOT RETRY: [%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                                r.status_code, attempts, target_collection_url, json.dumps(source_entity), r.json()))

        elif r.status_code == 400 and target_collection in ['users', 'roles']:

            # For the users collection, there seemed to be cases where a USERNAME was created/existing with the a
            # different UUID which caused a 'collision' - so the point is to delete the entity with the differing
            # UUID bu UUID and then do a recursive call to migrate the data - now that the collision has been cleared

            target_entity_url = put_entity_url_template.format(org=target_org,
                                                               app=target_app,
                                                               collection=target_collection,
                                                               uuid=source_entity.get('username'),
                                                               **config.get('target_endpoint'))

            logger.warning('Deleting entity at URL=[%s] due to collision...' % target_entity_url)

            r = requests.delete(target_entity_url)

            if r.status_code == 200:
                time.sleep(5)
                return migrate_data(app, collection_name, source_entity, attempts)
            else:
                logger.warning('Deletion of entity at URL=[%s] FAILED!...' % target_entity_url)

        else:
            logger.critical('FAILURE [%s] on attempt [%s] to PUT url=[%s], entity=[%s] response=[%s]' % (
                r.status_code, attempts, target_entity_url, json.dumps(source_entity), r.text))

        if not response:
            logger.error('migrate_data | success=[%s] | created=[%s] %s/%s/%s' % (
                response, app, collection_name, source_entity.get('created'), source_entity.get('uuid')))
        else:
            logger.debug('migrate_data | success=[%s] | created=[%s] %s/%s/%s' % (
                response, app, collection_name, source_entity.get('created'), source_entity.get('uuid')))

    except:
        logger.error(traceback.format_exc())
        logger.error('error in migrate_data on entity: %s' % json.dumps(source_entity))

    if response and collection_name == 'users':
        # migrate credentials in the case of the users collection
        response = response and migrate_user_credentials(app, collection_name, source_entity, attempts=0) or response

    return response


def audit_target(app, collection_name, source_entity, attempts=0):

    attempts += 1

    target_app, target_collection, target_org = get_target_mapping(app, collection_name)

    target_entity_url = get_entity_url_template.format(org=target_org,
                                                       app=target_app,
                                                       collection=target_collection,
                                                       uuid=source_entity.get('uuid'),
                                                       **config.get('target_endpoint'))

    r = requests.get(url=target_entity_url)

    if r.status_code == 200:
        return True
    else:

        if attempts <= 2:
            audit_logger.warning('AUDIT: Did not find entity at TARGET URL=[%s] - [%s]: %s' % (
                target_entity_url, r.status_code, r.text))

            response = migrate_data(app, collection_name, source_entity, attempts=0)

            if response:
                return audit_target(app, collection_name, source_entity, attempts)
        else:
            audit_logger.critical('AUDIT: Failed permanently to migrate Entity at TARGET URL=[%s]! [%s]: %s' % (
                target_entity_url, r.status_code, r.text))


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

    parser.add_argument('-c', '--collection',
                        help='Name of one or more collections to include, specify none to include all collections',
                        default=[],
                        action='append')

    parser.add_argument('-m', '--migrate',
                        help='Specifies what to migrate: data, connections, credentials, audit or none (just iterate '
                             'the apps/collections)',
                        type=str,
                        choices=['data', 'connections', 'credentials', 'none', 'audit'],
                        default='data')

    parser.add_argument('-s', '--source_config',
                        help='The path to the source endpoint/org configuration file',
                        type=str,
                        default='source.json')

    parser.add_argument('-d', '--target_config',
                        help='The path to the target endpoint/org configuration file',
                        type=str,
                        default='destination.json')

    parser.add_argument('-w', '--workers',
                        help='The number of worker processes to do the migration',
                        type=int,
                        default=16)

    parser.add_argument('--ql',
                        help='The QL to use in the filter for reading data from collections',
                        type=str,
                        default='select * order by created asc')

    parser.add_argument('--su_username',
                        help='Superuser username',
                        required=False,
                        type=str)

    parser.add_argument('--su_password',
                        help='Superuser Password',
                        required=False,
                        type=str)

    parser.add_argument('--map_app',
                        help="Multiple allowed: A colon-separated string such as 'apples:oranges' which indicates to put data from the app named 'apples' from the source endpoint into app named 'oranges' in the target endpoint",
                        default=[],
                        action='append')

    parser.add_argument('--map_collection',
                        help="One or more colon-separated string such as 'cats:dogs' which indicates to put data from collections named 'cats' from the source endpoint into a collection named 'dogs' in the target endpoint, applicable globally to all apps",
                        default=[],
                        action='append')

    parser.add_argument('--map_org',
                        help="One or more colon-separated strings such as 'red:blue' which indicates to put data from org named 'red' from the source endpoint into a collection named 'blue' in the target endpoint",
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

    # list the apps for the SOURCE org
    source_org_mgmt_url = org_management_url_template.format(org=config.get('org'),
                                                             **config.get('source_endpoint'))
    logger.info('GET %s' % source_org_mgmt_url)
    r = requests.get(source_org_mgmt_url)

    check_response_status(r, source_org_mgmt_url)

    logger.info(json.dumps(r.json()))

    org_apps = r.json().get('data')

    queue = Queue()
    logger.info('Starting workers...')

    # Check the specified configuration for what to migrate - Data, Connections or App User Credentials
    if config.get('migrate') == 'connections':
        operation = migrate_connections
    elif config.get('migrate') == 'data':
        operation = migrate_data
    elif config.get('migrate') == 'credentials':
        operation = migrate_user_credentials
    elif config.get('migrate') == 'audit':
        operation = audit_target
    else:
        operation = None

    # start the worker processes which will do the work of migrating
    workers = [Worker(queue, operation) for x in xrange(config.get('workers'))]
    [w.start() for w in workers]

    apps_to_process = config.get('app')
    collections_to_process = config.get('collection')

    # iterate the apps retrieved from the org
    for org_app in sorted(org_apps.keys()):
        logger.warning('Found SOURCE App: %s' % org_app)

    time.sleep(3)

    for org_app in sorted(org_apps.keys()):
        max_created = -1
        app_uuid = org_apps[org_app]
        parts = org_app.split('/')
        app = parts[1]

        # if apps are specified and the current app is not in the list, skip it
        if apps_to_process and len(apps_to_process) > 0 and app not in apps_to_process:
            logger.warning('Skipping app=[%s]' % app)
            continue

        logger.info('Processing app=[%s]' % app)

        # it is possible to map source orgs and apps to differently named targets.  This gets the
        # target names for each
        target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
        target_app = config.get('app_mapping', {}).get(app, app)

        # Check that the target Org/App exists.  If not, move on to the next
        target_app_url = app_url_template.format(org=target_org,
                                                 app=target_app,
                                                 **config.get('target_endpoint'))
        logger.info('GET %s' % target_app_url)
        r_target_apps = requests.get(target_app_url)

        if r_target_apps.status_code != 200:

            if CREATE_APPS:
                create_app_url = org_management_app_url_template.format(org=target_org,
                                                                        app=target_app,
                                                                        **config.get('target_endpoint'))
                app_request = {'name': target_app}
                r = requests.post(create_app_url, data=json.dumps(app_request))

                if r.status_code != 200:
                    logger.error('Unable to create app [%s] at URL=[%s]: %s' % (target_app, create_app_url, r.text))
                    continue
                else:
                    logger.warning('Created app=[%s] at URL=[%s]: %s' % (target_app, create_app_url, r.text))
            else:
                logger.error(
                        'Target application does not exist at [%s] URL=%s' % (
                            r_target_apps.status_code, target_app_url))
                continue

        # get the list of collections from the source org/app
        source_app_url = app_url_template.format(org=config.get('org'),
                                                 app=app,
                                                 **config.get('source_endpoint'))
        logger.info('GET %s' % source_app_url)

        r_collections = requests.get(source_app_url)

        collection_attempts = 0

        # sometimes this call was not working so I put it in a loop to force it...
        while r_collections.status_code != 200 and collection_attempts < 5:
            collection_attempts += 1
            logger.warning('FAILED: GET (%s) [%s] URL: %s' % (r_collections.elapsed, r_collections.status_code,
                                                              source_app_url))
            time.sleep(5)
            r_collections = requests.get(source_app_url)

        if collection_attempts >= 5:
            logger.error('Unable to get collections at URL %s, skipping app' % source_app_url)
            continue

        app_response = r_collections.json()

        logger.info('App Response: ' + json.dumps(app_response))

        app_entities = app_response.get('entities')

        if len(app_entities) > 0:
            app_entity = app_entities[0]
            collections = app_entity.get('metadata', {}).get('collections', {})
            logger.info('Collection List: %s' % collections)

            # iterate the collections which are returned.
            for collection_name, collection_data in collections.iteritems():

                # filter out collections as configured...
                if collection_name in ignore_collections \
                        or (len(collections_to_process) > 0 and collection_name not in collections_to_process) \
                        or (config.get('migrate') == 'credentials' and collection_name != 'users'):
                    logger.warning('Skipping collection=[%s]' % collection_name)
                    continue

                logger.info('Processing collection=%s' % collection_name)
                counter = 0

                source_collection_url = collection_query_url_template.format(org=config.get('org'),
                                                                             app=app,
                                                                             collection=collection_name,
                                                                             ql=config.get('ql'),
                                                                             **config.get('source_endpoint'))

                # use the UsergridQuery from the Python SDK to iterate the collection
                q = UsergridQuery(source_collection_url)

                try:
                    for entity in q:
                        if 'created' in entity:
                            try:
                                entity_created = long(entity.get('created'))
                                if entity_created > max_created:
                                    max_created = entity_created
                            except:
                                pass

                        counter += 1
                        queue.put((app, collection_name, entity))

                except KeyboardInterrupt:
                    logger.warning('Keyboard Interrupt, aborting...')
                    [w.terminate() for w in workers]

                logger.warning('Max Created entity for org/app/collection= %s/%s/%s is %s' % (
                    config.get('org'), app, collection_name, max_created))

        logger.info('DONE!')

    wait_for(workers)

    logger.info('workers DONE!')


def check_response_status(r, url, exit_on_error=True):
    if r.status_code != 200:
        logger.critical('HTTP [%s] on URL=[%s]' % (r.status_code, url))
        logger.critical('Response: %s' % r.text)

        if exit_on_error:
            exit()


main()
