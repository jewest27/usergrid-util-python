import json

import requests


__author__ = 'ApigeeCorporation'

url_template = 'http://localhost:9200/pea000ug_applications_2/_search'

request = {
    "query": {
        "term": {
            "entityId": "entityId(1a78d0a6-bffb-11e5-bc61-0af922a4f655,constratus)"
        }
    }
}

# url_template = 'http://localhost:9200/_search'
# r = requests.get(url)
r = requests.get(url_template, data=json.dumps(request))

print r.status_code
print json.dumps(r.json(), indent=2)

