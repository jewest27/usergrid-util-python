import json
import requests

__author__ = 'ApigeeCorporation'

nodes = [
    'res005sy',
    'res006sy',
    # 'res007sy',
    'res008sy',
    'res009sy',
    'res010sy',
    # 'res011sy',
    # 'res012sy',
    'res013sy',
    'res014sy',
    'res015sy',
    'res016sy',
    'res017sy',
    'res018sy',
    'res020sy',
    'res021sy',
    'res022sy',
    'res023sy',
    'res024sy',
    'res025sy',
    'res026sy',
    'res027sy',
    'res028sy',
    'res029sy',
    'res030sy',
    'res031sy',
    'res032sy',
    'res033sy',
    'res034sy'
]

large_nodes = [
    'res035sy',
    'res036sy',
    'res037sy',
    'res038sy',
    'res039sy',
    'res040sy',
    'res041sy',
    'res042sy'
]

url_base = 'http://localhost:9211'

exclude_nodes = large_nodes

nodes_string = ",".join(exclude_nodes)
nodes_string = ""

payload = {
    "index.routing.allocation.exclude._host": nodes_string
}

r = requests.get(url_base + "/_stats")
indices = r.json()['indices']

print 'retrieved %s indices' % len(indices)

includes = [
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

for index in indices:
    update = False
    counter += 1

    print 'index %s of %s: %s' % (counter, len(indices), index)

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
        print 'Processing index %s' % index

        url_template = '%s/%s/_settings' % (url_base, index)
        print url_template

        success = False

        while not success:

            response = requests.put('%s/%s/_settings' % (url_base, index), data=json.dumps(payload))

            if response.status_code == 200:
                success = True
                print '200: %s: %s' % (index, response.text)
            else:
                print '%s: %s: %s' % (response.status_code, index, response.text)