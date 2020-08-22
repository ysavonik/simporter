from datetime import datetime
import time
import csv
from flask import Flask, request
from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb+srv://<user>:<pwd>@cluster0.9h1pi.mongodb.net/simporter?retryWrites=true&w=majority')
db = client['simporter']
collection = db['amazon']

# upload data to mongo collection
# with open('/home/ysavonik/Downloads/pydev_test_task_data2.csv', 'r') as csvfile:
#     reader = csv.DictReader( csvfile )
# collection.drop()
# for each in reader:
#     collection.insert({
#         'asin': each['asin'],
#         'brand': each['brand'],
#         'id': each['id'],
#         'source': each['source'],
#         'stars': int(each['stars']),
#         'timestamp': datetime.fromtimestamp(int(each['timestamp'])),
#     })

app = Flask(__name__)

@app.route('/api/info', methods=['GET'])
def api_info():
    res_json = {}
    filters = []
    one = collection.find_one()
    one = [x for x in one if x != '_id' and x != 'id']
    for field in one:
        filters.append(field)
    values = {}
    for filter in filters:
        values[filter] = collection.distinct(filter)
    date_frames = []
    for frame in values["timestamp"]:
        date_frames.append(datetime.strftime(frame, '%Y-%m-%d'))
    if 'timestamp' in filters:
        filters.remove('timestamp')
    del values['timestamp']
    filters.append('startDate')
    filters.append('endDate')
    filters.append('grouping')
    filters.append('type')
    values['startDate'] = date_frames
    values['endDate'] = date_frames
    values['grouping'] = ['weekly', 'bi-weekly', 'monthly']
    values['type'] = ['cumulative', 'usual']
    res_json['filters'] = filters
    res_json['values'] = values
    return res_json


@app.route('/api/timeline', methods=['GET'])
def api_timeline():
    args = request.args
    agg = []
    # Asin filter DONE
    if 'asin' in args:
        asin = args['asin']
        agg.append({ '$match': {'asin': asin } })

    # Brand filter DONE
    if 'brand' in args:
        brand = args['brand']
        agg.append({ '$match': {'brand': brand } })

    # Source filter DONE
    if 'source' in args:
        source = args['source']
        agg.append({ '$match': {'source': source } })

    # Stars filter DONE
    if 'stars' in args:
        stars = int(args['stars'])
        agg.append({ '$match': {'stars': stars } })

    # startDate filter DONE
    if 'startDate' in args:
        startDate = args['startDate']
    else:
        startDate = '1970-01-01'

    # endDate filter DONE
    if 'endDate' in args:
        endDate = args['endDate']
    else:
        endDate = datetime.now().strftime("%Y-%m-%d")
    agg.append({ '$match' : { 'timestamp': { '$gte': datetime.strptime(startDate, "%Y-%m-%d"), '$lt': datetime.strptime(endDate, "%Y-%m-%d") } } })


    agg.append({
            '$group' : {
                '_id' : { '$dateToString': { 'format': "%Y-%m-%d", 'date': "$timestamp" } },
                'events' : { '$sum': 1 }
            }
        })
    agg.append({
            '$sort' : { '_id': -1 }
        })
    data = collection.aggregate(agg)
    timeline = []
    df = pd.DataFrame(data=data, columns=['_id', 'events'])
    df._id = pd.to_datetime(df['_id'],format='%Y-%m-%d')

    # Grouping filter DONE
    if 'grouping' in args:
        if args['grouping'] == 'bi-weekly':
            df = df.groupby(pd.Grouper(key="_id", freq="2W")).size().reset_index(name='events')
        if args['grouping'] == 'monthly':
            df = df.groupby(pd.Grouper(key="_id", freq="1M")).size().reset_index(name='events')
        if args['grouping'] == 'weekly':
            df = df.groupby(pd.Grouper(key="_id", freq="1W")).size().reset_index(name='events')
    else:
        df = df.groupby(pd.Grouper(key="_id", freq="1W")).size().reset_index(name='events')

    # Type filter DONE
    if 'type' in args:
        if args['type'] == 'cumulative':
            df["events"] = df["events"].cumsum()

    for i in range(len(df)):
        timeline.append({"date": datetime.strftime(df['_id'][i], '%Y-%m-%d'), "value": int(df['events'][i])}) #
    return {'timeline': timeline}


if __name__ == '__main__':
    app.run(debug=True)