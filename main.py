#!/usr/bin/env python

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import urllib.request, urllib.parse, urllib.error
from google.appengine.api import urlfetch
import json
import os
import yql
from flask import Flask
from flask import request
from flask import make_response


from google.cloud import bigquery
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.contrib.appengine import AppAssertionCredentials
#scopes = ['https://www.googleapis.com/auth/sqlservice.admin']
scopes = ['https://www.googleapis.com/auth/bigquery']
GOOGLE_APPLICATION_CREDENTIALS="/Users/jisiliang/Dropbox/qa/silver/job/SilverberryAI-ff09a9231e7c.json"
credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scopes)
#credentials = ServiceAccountCredentials.from_json_keyfile_name(StringIO(os.environ['GOOGLE_APPLICATION_CREDENTIALS']), scopes)
#print ("agbout to 1234")
#credentials = ServiceAccountCredentials.from_json_keyfile_name(filename, scopes)
#print ("333")
#credentials = GoogleCredentials.get_application_default()
#print ("444")

#GoogleCredentials.get_access_token()


# Flask app should start in global layout
app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)

    print("Request:")
    print(json.dumps(req, indent=4))

    res = processRequest(req)

    res = json.dumps(res, indent=4)
    # print(res)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'


    return r


def processRequest(req):
    if req.get("result").get("action") != "yahooWeatherForecast":
        return {}
    baseurl = "https://query.yahooapis.com/v1/public/yql?"
    yql_query = makeYqlQuery(req)
    if yql_query is None:
        return {}
    yql_url = baseurl + urllib.parse.urlencode({'q': yql_query}) + "&format=json"
    result = urllib.request.urlopen(yql_url).read()
    data = json.loads(result)
    res = makeWebhookResult(data)
    return res


def makeYqlQuery(req):
    result = req.get("result")
    parameters = result.get("parameters")
    city = parameters.get("geo-city")
    if city is None:
        return None

    return "select * from weather.forecast where woeid in (select woeid from geo.places(1) where text='" + city + "')"


def makeWebhookResult(data):
    query = data.get('query')
    if query is None:
        return {}

    result = query.get('results')
    if result is None:
        return {}

    channel = result.get('channel')
    if channel is None:
        return {}

    item = channel.get('item')
    location = channel.get('location')
    units = channel.get('units')
    if (location is None) or (item is None) or (units is None):
        return {}

    condition = item.get('condition')
    if condition is None:
        return {}

    # print(json.dumps(item, indent=4))

    speech = "Hello There , Today in " + location.get('city') + ": " + condition.get('text') + \
             ", the temperature is " + condition.get('temp') + " " + units.get('temperature')



    print("Response with big:")
    print(speech)
  
    return {
        "speech": speech,
        "displayText": speech,
        # "data": data,
        # "contextOut": [],
        "source": "apiai-weather-webhook-sample"
    }

'''
def big():
    print ("here I am in the module for BIGQUERY getappdefault o project iddddd")

    print ("variable")
    print (os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    # Instantiates a client
    bigquery_client = bigquery.Client(project='silverberry-ai')
    #bigquery_client = bigquery.Client()
    print ("bq 1")
    query_results = bigquery_client.run_sync_query("""
    print ("bq 2")
    SELECT
        APPROX_TOP_COUNT(corpus, 10) as title,
        COUNT(*) as unique_words
    FROM `publicdata.samples.shakespeare`;""")
    print ("bq 3")
    
    # https://cloud.google.com/bigquery/sql-reference/
    query_results.use_legacy_sql = False
    print ("bq 4")

    query_results.run()
    print ("bq 5")
    query_results.fetch_data()
    print ("bq 6")
    print  (query_results.project)
    print ("bq 7")
    page_token = None
    print ("bq 8")

    while True:
        rows, total_rows, page_token = query_results.fetch_data(
            max_results=10,
            page_token=page_token)

        for row in rows:
            print(row)

        if not page_token:
            break

    print ("completed")

'''

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))

    print("Starting app on port %d" % port)

    app.run(debug=False, port=port, host='0.0.0.0')
