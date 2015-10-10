import json

import requests

__author__ = 'Jeff West'

SOURCE_INDEX = '5f20f423-f2a8-11e4-a478-12a5923b55dc__application_v7'
TARGET_INDEX = '5f20f423-f2a8-11e4-a478-12a5923b55dc__application_v8'
SAVE_MAPPINGS = True


def process_dynamic_template(dynamic_template):
    new_template = dynamic_template.copy()

    for template_name, template_field_data in new_template.iteritems():

        for data_field_name, data_field_data in template_field_data.iteritems():
            if data_field_name == 'mapping':
                new_template[template_name]['mapping']['_all'] = {'enabled': False}

                # if data_field_data['type'] in ['string', 'long']:
                #     new_template[template_name]['mapping']['norms'] = {'enabled': False}
                #     new_template[template_name]['mapping']['doc_values'] = True

                # if data_field_data['type'] in ['string']:
                #     new_template[template_name]['mapping']['index'] = 'not_analyzed'

    return new_template


def process_dynamic_templates(arr_dynamic_templates):
    arr_new_template = []

    for dynamic_template in arr_dynamic_templates:
        arr_new_template.append(process_dynamic_template(dynamic_template))

    return arr_new_template


def process_mapping(type_name, mapping_detail):
    keep = True
    new_mapping = mapping_detail.copy()

    if mapping_detail.get('format') == 'dateOptionalTime':
        keep = False

    for field_name, fielddata in mapping_detail.iteritems():

        if field_name == '_routing':
            continue

        if 'properties' not in mapping_detail:
            # no properties at this level
            pass

        else:
            if field_name == 'dynamic_templates':
                new_mapping['dynamic_templates'] = process_dynamic_templates(fielddata)

            elif field_name in ['dynamic'] or field_name[0] == '_':
                pass

            elif field_name == 'date_detection':
                pass

            elif field_name == 'properties':

                data_copy = fielddata.copy()

                for prop_name, prop_data in data_copy.iteritems():
                    sub_update, keep_sub_1 = process_mapping(prop_name, prop_data)

                    if keep_sub_1:
                        new_mapping[field_name][prop_name] = sub_update
                    else:
                        new_mapping[field_name].pop(prop_name, None)
                        if prop_name in new_mapping[field_name]:
                            print 'wtf'
            else:
                sub_update, keep_sub_2 = process_mapping(field_name, fielddata)

                if keep_sub_2:
                    new_mapping[field_name] = sub_update
                else:
                    new_mapping.pop(field_name, None)
                    if field_name in new_mapping:
                        print 'wtf'

    return new_mapping, keep


url_base = 'http://localhost:9200'

r = requests.get(url_base + "/_stats")

indices = r.json()['indices']

mapping_url = '{url_base}/{index_name}/_mapping'.format(url_base=url_base, index_name=SOURCE_INDEX)

index_data = requests.get('{url_base}/{index_name}'.format(url_base=url_base, index_name=SOURCE_INDEX)).json()

mappings = index_data.get(SOURCE_INDEX, {}).get('mappings', {})

for type_name, mapping_detail in mappings.iteritems():

    print 'Index: %s | Type: %s: | Properties: %s' % (
        SOURCE_INDEX, type_name, len(mappings[type_name]['properties']))

    if SAVE_MAPPINGS:
        with open('/Users/ApigeeCorporation/tmp//%s_%s_source_mapping.json' % (
                SOURCE_INDEX, type_name), 'w') as f:
            json.dump({type_name: mapping_detail}, f, indent=2)

    updated_mapping, keep = process_mapping(type_name, mapping_detail)

    updated_filename = '/Users/ApigeeCorporation/tmp//%s_%s_updated_mapping.json' % (
        SOURCE_INDEX, type_name)

    print updated_filename

    with open(updated_filename, 'w') as f:
        json.dump({type_name: updated_mapping}, f, indent=2)

    payload = {type_name: updated_mapping}

    mapping_url = '{url_base}/{index_name}/_mapping'.format(url_base=url_base, index_name=TARGET_INDEX)
    type_mapping_url = '{mapping_url}/{type_name}'.format(mapping_url=mapping_url, type_name=type_name)

    # print 'PUT %s' % type_mapping_url
    #
    # r = requests.put(type_mapping_url, data=json.dumps(payload))
    #
    # if r.status_code != 200:
    #     print 'foo'
    #
    # print '%s: %s' % (r.status_code, r.text)
    # print '---'
