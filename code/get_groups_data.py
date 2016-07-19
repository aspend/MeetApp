import numpy as np
import pandas as pd
from pymongo import MongoClient
import urllib
import json
from time import sleep
import requests
import logging 

secret_keys = json.load(open("secret_keys.txt"))
key = secret_keys['key']


db_cilent = MongoClient()
db = db_cilent['meetup']
groups_table = db['groups_topics']

count = 0
offset = 0
group_data = ['init']
failed_saves_offsets = []
group_dict = {}
while True:
    failed = True
    while failed:
        try:
            print "Requesting group data..."
            url_group= "https://api.meetup.com/find/groups?&sign=true&photo-host=public&lon=-122.4192703&radius=10&fields=plain_text_description,topics&lat=37.7792768&order=most_active&page=20&offset={offset}&key={key}".format(key=key, offset = offset)
            print url_group
            group_response = urllib.urlopen(url_group)
            group_data = json.loads(group_response.read())
            if int(group_response.info()['X-RateLimit-Remaining'])<5:
                sleep(int(group_response.info()['X-RateLimit-Remaining'])+0.5)
            failed = False
            offset +=1
        except:
            print "failed, retrying..."

    if len(group_data)==0:
        break
    print "Saving page " + str(count) + " of groups to MongoDB"
    try:
        groups_table.insert_many(group_data)
        count +=1
    except:
        logging.exception('') 
        failed_saves_offsets.append(offset)
        print "Save failed. Continuing..."

print "failed on offsets: " + str(failed_saves_offsets)
print "Done!"
#note that I saved a copy of the groups data in the repo for easy reference/amusement. There are 4538 meetup
#groups within 10 miles of San Francisco.


