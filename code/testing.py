import numpy as np
import pandas as pd
import urllib
import json
import requests


    
print "Requesting data..."
url_group= "http://iot.sb05.stations.graphenedb.com:24789/db/data/"
print "url_group:"
print url_group
group_response = urllib.urlopen(url_group)
group_data = json.loads(group_response.read())
print "group data:"
print group_data

print 'a'