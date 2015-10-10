import traceback
import requests

__author__ = 'Jeff West @ ApigeeCorporation'


def total_milliseconds(td):
    return (td.microseconds + td.seconds * 1000000) / 1000


data_map = {
    "<<ORG NAME>>": {
        "apps": {
            "<<APP 1>>": {
                "collections": [
                    '<<COLLECTION 1>>',
                    '<<COLLECTION 2>>',
                ]
            }
        },
        "client_id": '<<client_id>>',
        "client_secret": '<<client_secret>>'
    }
}

keep_going = True
count_with_zero = 0

url_template = 'http://localhost:8080/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&ql=select *&limit={limit}'

for org, org_data in data_map.iteritems():
    template_data = {
        'limit': 500,
        'org': org,
        'client_id': org_data.get('client_id'),
        'client_secret': org_data.get('client_secret')}

    for app, app_data in org_data.get('apps', {}).iteritems():
        template_data['app'] = app

        for collection in app_data.get('collections', []):
            template_data['collection'] = collection
            keep_going = True

            count_with_zero = 0

            while keep_going:

                url_template = url_template.format(**template_data)
                print url_template
                try:
                    r = requests.get(url_template)

                    response = requests.get(url_template)
                    count = len(response.json().get('entities'))
                    total_ms = total_milliseconds(response.elapsed)

                    print 'GET %s from collection %s in %s' % (count, collection, total_ms)

                    response = requests.delete(url_template)

                    try:
                        count = len(response.json().get('entities'))
                        total_ms = total_milliseconds(response.elapsed)

                        print 'Deleted %s from collection %s in %s' % (count, collection, total_ms)

                        if count == 0:
                            count_with_zero += 1
                            print 'Count with ZERO: %s' % count_with_zero
                            if count_with_zero >= 100:
                                keep_going = False
                        else:
                            count_with_zero = 0

                    except:
                        print 'Error! HTTP Status: %s response: %s' % (response.status_code, response.text)
                except KeyboardInterrupt:
                    exit()
                except:
                    print traceback.format_exc()
