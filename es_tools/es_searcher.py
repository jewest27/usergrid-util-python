import json

import requests


__author__ = 'ApigeeCorporation'

url_template = 'http://localhost:9200/stage-ex__949e79c9-a24e-11e4-8645-0220301a4821__application/_search'

request = {
    "query": {
        "filtered": {
            "filter": {
                "terms": {
                    "su_name": ['jwest2']
                }
            }
        }
    }
}

url_template = 'http://localhost:9200/_search'
# r = requests.get(url)
r = requests.get(url_template, data=json.dumps(request))

print r.status_code
print json.dumps(r.json(), indent=2)

