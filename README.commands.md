# list files by date changed
git ls-files -z | xargs -0 -n1 -I{} -- git log -1 --format="%ai {}" {} | sort


FROM SQS
--------

import json
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData

das_query = requests.get('http://localhost:8003/api/proposal/1505/sqs_data.json').json()

masterlist_questions = das_query['masterlist_questions']
geojson = das_query['geojson']
proposal = das_query['proposal']
dlq = DisturbanceLayerQuery(das_query['masterlist_questions'], das_query['geojson'], das_query['proposal'])
dlq.query()

dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
dlq.query()

OR From DAS
--------

import requests
das_query = requests.get('http://localhost:8003/api/proposal/1505/sqs_data.json').json()
ret = requests.post(url='http://localhost:8002/api/layers/das/spatial_query.json', json=das_query)
ret.json()

----
import json
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData

with open('sqs/data/json/goldfields_curl_query_checkbox_full2.json', 'r') as f:
    query_json = json.load(f)

masterlist_questions = query_json['masterlist_questions']
geojson = query_json['geojson']
proposal = query_json['proposal']

dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
dlq.query()


From DAS
p=Proposal.objects.get(id=1505)

masterlist_gbl=requests.get('http://localhost:8003/api/spatial_query/grouped_by_layer.json').json()

geojson = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[124.12353515624999,-30.391830328088137],[124.03564453125,-31.672083485607377],[126.69433593749999,-31.615965936476076],[127.17773437499999,-29.688052749856787],[124.12353515624999,-30.391830328088137]]]}}]}


query_json = dict(masterlist_questions=masterlist_gbl, proposal=dict(schema=p.schema, data=p.data), geojson=geojson)
query_json = dict(masterlist_questions=masterlist_gbl, proposal=dict(schema=p.schema, data=[]), geojson=geojson)

ret = requests.post(url='http://localhost:8002/api/layers/das/spatial_query.json', json=query_json)

p.data=ret.json()['data']
p.save()

