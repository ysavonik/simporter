from datetime import datetime
import time
from elasticsearch import helpers, Elasticsearch
from elasticsearch_dsl import Search, A
import csv
from flask import Flask, request
import json
import pandas as pd
import requests


es = Elasticsearch()

#  UPLOAD DATA TO ELASTICSEARCH
# df = pd.read_csv("pydev_test_task_data2.csv")
# result = df.to_json(orient="records")
# parsed = json.loads(result)
# for row in parsed:
#     row['timestamp'] = datetime.strftime(datetime.fromtimestamp(row['timestamp']), "%Y-%m-%d")
# helpers.bulk(es, parsed, index='simporter_clone_1', doc_type='simporter-type')
# exit()

app = Flask(__name__)

@app.route('/api/info', methods=['GET'])
def api_info():
    res_json = {}
    r = requests.get('http://localhost:9200/simporter_clone/_mapping').content
    j = json.loads(r)
    text_filters = []
    filters = []
    for el in j['simporter_clone']['mappings']['properties']:
        if j['simporter_clone']['mappings']['properties'][el]['type'] == 'text':
            text_filters.append(el)
        else:
            filters.append(el)
    values = {}
    query = {
        "size": 0,
        "aggs" : {
            "values" : {
                "terms" : {
                    'field': '',
                    "size": 10000
                }
            }
        }
    }

    for f in filters:
        values[f] = []
        query['aggs']['values']['terms']['field'] = f
        resp = es.search(index="simporter_clone", body=query)['aggregations']['values']['buckets']
        for el in resp:
            if 'key_as_string' in el:
                values[f].append(el['key_as_string'])
            else:
                values[f].append(el['key'])

    text_filters = [ x for x in text_filters if x != 'id' ]
    for f in text_filters:
        values[f] = []
        query['aggs']['values']['terms']['field'] = f + '.keyword'
        resp = es.search(index="simporter_clone", body=query)['aggregations']['values']['buckets']
        for el in resp:
            values[f].append(el['key'])

    if 'timestamp' in filters:
        filters.remove('timestamp')
    filters.append('startDate')
    filters.append('endDate')
    filters.append('grouping')
    filters.append('type')

    values['startDate'] = values['timestamp']
    values['endDate'] = values['timestamp']
    del values['timestamp']
    values['grouping'] = ['weekly', 'bi-weekly', 'monthly']
    values['type'] = ['cumulative', 'usual']

    for f in text_filters:
        filters.append(f)
    res_json['filters'] = filters
    res_json['values'] = values
    return res_json


@app.route('/api/timeline', methods=['GET'])
def api_timeline():
    args = request.args
    query = {
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "size": 0,
        "aggs": {
            "group": {
                "date_histogram": {
                    "field": "timestamp",
                    "interval": "month"
                }
            }
        }
    }

    # startDate filter DONE
    if 'startDate' in args:
        query['query']['bool']['filter'].append({
            "range": {
                "timestamp": {
                    "gte": args['startDate']
                }
            }
        })

    # endDate filter DONE
    if 'endDate' in args:
        query['query']['bool']['filter'].append({
            "range": {
                "timestamp": {
                    "lt": args['endDate']
                }
            }
        })

    # Asinfilter DONE
    if 'asin' in args:
        query['query']['bool']['must'].append({"match": { "asin": args['asin'] }})

    # Brandfilter DONE
    if 'brand' in args:
        query['query']['bool']['must'].append({"match": { "brand": args['brand'] }})

    # Sourcefilter DONE
    if 'source' in args:
        query['query']['bool']['must'].append({"match": { "source": args['source'] }})

    # Starsfilter DONE
    if 'stars' in args:
        query['query']['bool']['must'].append({"match": { "stars": args['stars'] }})

    # Groupingfilter DONE
    if 'grouping' in args:
        if args['grouping'] == 'bi-weekly':
            query['aggs']['group']['date_histogram']['interval'] = '14d'
        if args['grouping'] == 'monthly':
            query['aggs']['group']['date_histogram']['interval'] = '1M'
        if args['grouping'] == 'weekly':
            query['aggs']['group']['date_histogram']['interval'] = '1w'

    res = {'timeline': []}
    responce = es.search(index="simporter_clone", body=query)
    frames = responce["aggregations"]["group"]["buckets"]
    for frame in frames:
        el = {}
        res['timeline'].append({"date": frame["key_as_string"], "value": frame["doc_count"]})

    # Typefilter DONE
    if 'type' in args:
        if args['type'] == 'cumulative':
            for i in range(len(res['timeline']) - 1):
                res['timeline'][i + 1]['value'] += res['timeline'][i]['value']
    return(res)


if __name__ == '__main__':
    app.run(debug=True)