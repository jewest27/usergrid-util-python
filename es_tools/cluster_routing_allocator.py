import json

import requests


__author__ = 'ApigeeCorporation'

# curl -X PUT http://${HOSTNAME}:9200/_cluster/settings -d '{"transient" : {"cluster.routing.allocation.enable" : "none" } }'

url_template = 'http://localhost:9211/_cluster/settings'

data = {"transient": {"cluster.routing.allocation.enable": "all"}}

status_code = 500

while status_code >= 500:
    r = requests.delete('http://localhost:9211/usergrid__a34ad389-b626-11e4-848f-06b49118d7d0__application_revised')

    status_code = r.status_code

    print '%s: %s' % (status_code, r.text)
