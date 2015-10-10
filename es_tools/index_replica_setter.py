import requests
import time

__author__ = 'ApigeeCorporation'

url_base = 'http://localhost:9211'

# r = requests.get(url_base + "/_cat/indices?v")
# print r.text

r = requests.get(url_base + "/_stats")

# print json.dumps(r.json(), indent=2)

indices = r.json()['indices']

print 'retrieved %s indices' % len(indices)

NUMBER_VALUE = 1

payload = {
    "index.number_of_replicas": NUMBER_VALUE,
}

# indices = ['usergrid__a34ad389-b626-11e4-848f-06b49118d7d0__application_manual']

includes = [
    # 'usergrid',
    # 'b6768a08-b5d5-11e3-a495-11ddb1de66c8',
    # 'b6768a08-b5d5-11e3-a495-10ddb1de66c3',
    # 'b6768a08-b5d5-11e3-a495-11ddb1de66c9',
    'a34ad389-b626-11e4-848f-06b49118d7d0'
]

excludes = [
    # 'b6768a08-b5d5-11e3-a495-11ddb1de66c8',
    # 'b6768a08-b5d5-11e3-a495-10ddb1de66c3',
    # 'b6768a08-b5d5-11e3-a495-11ddb1de66c9',
    # 'a34ad389-b626-11e4-848f-06b49118d7d0'
]

counter = 0
update = False
# print 'sleeping 1200s'
# time.sleep(1200)

for index in sorted(indices):
    update = False
    counter += 1

    print 'index %s of %s' % (counter, len(indices))

    if len(includes) == 0:
        update = True
    else:
        for include in includes:

            if include in index:
                update = True

    if len(excludes) > 0:
        for exclude in excludes:
            if exclude in index:
                update = False

    if update:
        print index

        # url = '%s/%s/_settings' % (url_base, index)
        # print url
        #
        # response = requests.get('%s/%s/_settings' % (url_base, index))
        # settings = response.json()
        #
        # index_settings = settings[index]['settings']['index']
        #
        # current_replicas = int(index_settings.get('number_of_replicas'))
        #
        # if current_replicas == NUMBER_VALUE:
        #     continue

        success = False

        while not success:

            response = requests.put('%s/%s/_settings' % (url_base, index), data=payload)

            if response.status_code == 200:
                success = True
                print '200: %s: %s' % (index, response.text)
            else:
                print '%s: %s: %s' % (response.status_code, index, response.text)






        # time.sleep(5)
