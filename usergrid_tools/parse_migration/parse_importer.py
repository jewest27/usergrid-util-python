import json
import logging
from logging.handlers import RotatingFileHandler
import os
from os import listdir
import zipfile

# parameters
from os.path import isfile

import sys

from usergrid import Usergrid
from usergrid.UsergridClient import UsergridEntity

logger = logging.getLogger('UsergridParseImporter')

parse_id_to_uuid_map = {}
global_connections = {}


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './usergrid_parse_importer.log'
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

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


def join_roles_to_roles(working_directory):
    join_file = os.path.join(working_directory, '_Join:roles:_Role.json')

    if os.path.isfile(join_file) and os.path.getsize(join_file) > 0:
        with open(join_file, 'r') as f:
            users_to_roles = json.load(f).get('results', [])
            logger.info('Loaded [%s] Roles->Roles' % len(users_to_roles))
    else:
        logger.info('No Roles->Roles to load')


def convert_parse_entity(collection, parse_entity):
    parse_entity['type'] = collection

    if 'name' not in parse_entity and collection.lower() != 'users':
        parse_entity['name'] = parse_entity['objectId']

    connections = {}

    for name, value in parse_entity.iteritems():

        if isinstance(value, dict):
            if value.get('__type') == 'Pointer':
                logger.info('connection found: %s' % value)

                connections[value.get('objectId')] = value.get('className') if value.get('className')[
                                                                                   0] != '_' else value.get(
                        'className')[1:]

    return UsergridEntity(parse_entity), connections


def build_usergrid_entity(collection, entity_uuid, data=None):
    identifier = {'type': collection, 'uuid': entity_uuid}

    if data is None:
        data = {}

    data.update(identifier)

    return UsergridEntity(data)


def load_users_and_roles(working_directory):
    with open(os.path.join(working_directory, '_User.json'), 'r') as f:
        users = json.load(f).get('results', [])
        logger.info('Loaded [%s] Users' % len(users))

    for i, parse_user in enumerate(users):
        logger.info('Loading user [%s]: [%s / %s]' % (i, parse_user['username'], parse_user['objectId']))
        usergrid_user, connections = convert_parse_entity('users', parse_user)
        response = usergrid_user.save()

        if 'uuid' in usergrid_user.entity_data:
            parse_id_to_uuid_map[parse_user['objectId']] = usergrid_user.get('uuid')

    with open(os.path.join(working_directory, '_Role.json'), 'r') as f:
        roles = json.load(f).get('results', [])
        logger.info('Loaded [%s] Roles' % len(roles))

    for i, parse_role in enumerate(roles):
        logger.info('Loading role [%s]: [%s / %s]' % (i, parse_role['name'], parse_role['objectId']))
        usergrid_role, connections = convert_parse_entity('roles', parse_role)
        usergrid_role.save()

        if 'uuid' in usergrid_role.entity_data:
            parse_id_to_uuid_map[parse_role['objectId']] = usergrid_role.get('uuid')

    join_file = os.path.join(working_directory, '_Join:users:_Role.json')

    if os.path.isfile(join_file) and os.path.getsize(join_file) > 0:
        with open(join_file, 'r') as f:
            users_to_roles = json.load(f).get('results', [])
            logger.info('Loaded [%s] User->Roles' % len(users_to_roles))

            for user_to_role in users_to_roles:
                role_id = user_to_role['owningId']
                role_uuid = parse_id_to_uuid_map.get(role_id)

                user_id = user_to_role['relatedId']
                user_uuid = parse_id_to_uuid_map.get(user_id)

                role_entity = build_usergrid_entity('role', role_uuid)
                user_entity = build_usergrid_entity('user', user_uuid)

                res = Usergrid.assign_role(role_uuid, user_entity)

                if res.ok:
                    logger.info('Assigned role [%s] to user [%s]' % (role_uuid, user_uuid))
                else:
                    logger.error('Failed on assigning role [%s] to user [%s]' % (role_uuid, user_uuid))

    else:
        logger.info('No Users->Roles to load')


def load_entities(working_directory):
    files = [f for f in listdir(working_directory)
             if isfile(os.path.join(working_directory, f)) and f[0] != '_']

    for file in files:
        file_path = os.path.join(working_directory, file)
        collection = file.split('.')[0]

        if collection not in global_connections:
            global_connections[collection] = {}

        entities = []

        with open(file_path, 'r') as f:
            entities = json.load(f).get('results')

            logger.info('Found [%s] entities of type [%s]' % (len(entities), collection))

            for parse_entity in entities:
                usergrid_entity, connections = convert_parse_entity(collection, parse_entity)
                response = usergrid_entity.save()

                global_connections[collection][usergrid_entity.get('uuid')] = connections

                if response.ok:
                    logger.info('Saved Entity: %s' % parse_entity)
                else:
                    logger.info('Error saving entity %s: %s' % (parse_entity, response))


def connect_entities(from_entity, to_entity, connection_name):
    connect_response = from_entity.connect(connection_name, to_entity)

    if connect_response.ok:
        logger.info('Successfully connected [%s / %s]--[%s]-->[%s / %s]' % (
            from_entity.get('type'), from_entity.get('uuid'), connection_name, to_entity.get('type'),
            to_entity.get('uuid')))
    else:
        logger.error('Unable to connect [%s / %s]--[%s]-->[%s / %s]: %s' % (
            from_entity.get('type'), from_entity.get('uuid'), connection_name, to_entity.get('type'),
            to_entity.get('uuid'), connect_response))


def create_connections():
    for from_collection, entity_map in global_connections.iteritems():

        for from_entity_uuid, entity_connections in entity_map.iteritems():
            from_entity = UsergridEntity({
                'uuid': from_entity_uuid,
                'type': from_collection
            })

            for to_entity_id, to_entity_collection in entity_connections.iteritems():
                to_entity = UsergridEntity({
                    'uuid': parse_id_to_uuid_map.get(to_entity_id),
                    'type': to_entity_collection
                })

                connect_entities(from_entity, to_entity, 'pointers')

                connect_entities(to_entity, from_entity, 'pointers')


def main():
    init_logging()
    Usergrid.init(org_id='jwest1',
                  app_id='sandbox')

    tmp_dir = '/Users/ApigeeCorporation/tmp'
    file_path = '/Users/ApigeeCorporation/code/usergrid/usergrid-util-python/usergrid_tools/parse_migration/0e4b82c5-9aad-45de-810a-ff07c281ed2d_1454177649_export.zip'

    file_name = os.path.basename(file_path).split('.')[0]

    working_directory = os.path.join(tmp_dir, file_name)

    with zipfile.ZipFile(file_path, "r") as z:
        logger.info('Extracting files to directory: %s' % working_directory)
        z.extractall(working_directory)
        logger.info('Extraction complete')

    load_users_and_roles(working_directory)

    load_entities(working_directory)

    create_connections()


# count files
# iterate internal objects starting with _
# load users
# load roles
# connect roles -> Users
# connect roles -> roles
# iterate custom collections
# end

main()
