#!/usr/bin/python3
from pprint import pprint as pp
import os, requests, time, json
from influxdb import InfluxDBClient
from config import *

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

client = InfluxDBClient(host=inf_cfg['host'], port=inf_cfg['port'], username=inf_cfg['user'], password=inf_cfg['pass'])
client.switch_database(inf_cfg['db'])

date = os.popen("date +%s").read().split('\n')
t = ((int(date[0])) * 1000000000 - 10000000000)

data = {}
def is_empty(my_dict):
    if my_dict == {}:
        return
    else:
        data.clear()
st_code = 0
codes = 'https://en.wikipedia.org/wiki/List_of_HTTP_status_codes'

def conn():
    global st_code,data
    url = 'https://%s/%s' % (twitch_cfg['fqdn'],api)
    headers = { 'Authorization': twitch_cfg['token'] }
    r = requests.get(url, verify=False, headers=headers)
    if r.status_code == 200:
#        print ((json.loads(r.text)))
        if type(json.loads(r.text)) == list:
            is_empty(data)
            data = {'base':(json.loads(r.text))}
            st_code = r.status_code
            requests.session().close()
        elif type(json.loads(r.text)) == dict:
            is_empty(data)
            data.update(json.loads(r.text))
            st_code = r.status_code
            requests.session().close()
    else:
        st_code = r.status_code

api = 'games/top?first=100'

conn()
feed = {}
for i in data['data']:
   feed.update({ i['id'] : i['name']})
streams = {}
for i in feed:
    api = '/streams?game_id=%s&first=100' % (i)
#    pp (api)
    conn ()
    streams.update({ feed[i]: data['data']})

#pp (streams)
total_viewers = 0
games_viewers = {}
for i in streams:
    influx_data = []
    viewers = 0
    for b in streams[i]:
        influx_data.append({ 
                            "measurement": inf_cfg['measurement'],
                            "tags": {
                                     "Game Name": i,
                                     "language": b['language'],
                                     "user_name": b['user_name'],
                                     "live": b['type']
                            },
                            "time" : t,
                            "fields": { "viewer_count" : b['viewer_count']}
                            })
        total_viewers += b['viewer_count']
        viewers += b['viewer_count']
    games_viewers.update({ i: viewers })
#   print (influx_data)
    client.write_points(influx_data)

#print (total_viewers)
#pp (games_viewers)
influx_data = []
for i in games_viewers.keys():
    influx_data.append({
                        "measurement": inf_cfg['measurement'],
                        "tags": {
                                 "Game Name Viewers": i,
                        },
                        "time" : t,
                        "fields": { "viewer_count" : games_viewers[i]}
                        })

influx_data.append({
		"measurement": "twitch",
		"tags": {
			 "Total Viewers": "Total Viewers",
		},
		"time" : t,
		"fields": { "viewer_count" : total_viewers}
		})
client.write_points(influx_data)
#pp (influx_data)
