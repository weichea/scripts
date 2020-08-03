import json
import ast
from elasticsearch import Elasticsearch, helpers
import time


es = Elasticsearch(
    ['localhost'],
    http_auth=('username', 'password'),
    scheme='https',
    verify_certs=False,
    port=9200          
)
#  195,600 hits 

def ingest_events(filename):
    events_dict = {}
    count = 0
    with open(file, 'r') as f:
         
        for line in f:
            count = count + 1
            data = ast.literal_eval(line)

            
            if not data['SourceName'] in events_dict:
                events_dict.update({data['SourceName']: []})
                events_dict[data['SourceName']].append(data)
            else:
                events_dict[data['SourceName']].append(data)

            if count >= 1000:
                es_bulk_insert(events_dict)
                count = 0
                events_dict = {}


        if events_dict:
            es_bulk_insert(events_dict)

            
        

def es_bulk_insert(events_dict):
    for k in events_dict.keys():
        if ' ' in k:
            new_k = '-'.join(k.split())
        else:
            new_k = k

        helpers.bulk(
            es, 
            events_dict[k],
            index='mordor-' + new_k.lower()
    )

for file in ['./apt29_evals_day1_manual_2020-05-01225525.json', './apt29_evals_day2_manual_2020-05-02035409.json']:
    events_dict = ingest_events(file)


