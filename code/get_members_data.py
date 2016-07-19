import numpy as np
import urllib
import json
import pandas as pd
from time import sleep
import requests
from pymongo import MongoClient
import json
import itertools
import logging 

secret_keys = json.load(open("secret_keys.txt"))
key = secret_keys['key']

db_cilent = MongoClient()
db = db_cilent['meetup']
groups_table = db['groups']
#make new table to hold group membership data:
table_tech = db['members_data']



cursor = groups_table.find()
groups = [group for group in cursor]

#find groups who are in 'tech' category
groups = np.array(groups)
#insert value "None" to group category for uncategorized groups
group_categories = [groups[i]['category']['name'] if groups[i].get('category', False) else 'None' for i in range(len(groups))]

tech_groups = groups[np.array(group_categories) == 'Tech']
tech_groups_nmembers = [(group['id'], group['members']) for group in tech_groups]

groups_members = {}

recorded = []
count = 0
groups_count = 0
#request for members of a group, one group at a time. number of members in the group
#is used to determine the number of pages to loop through to get all members. 
for group_id, nmembers in tech_groups_nmembers:
    # list_members = []
    groups_count +=1
#each group can have many pages of members. Loop through values of offset to get all members.
    for offset in range(nmembers/200 +1):
        failed = True
#have while loop with try while making requests to api in case internet connection goes down
#or server fails
        while failed:
            try:
                url_group= "https://api.meetup.com/2/members?&sign=true&photo-host=public&fields=messaging_pref&group_id={group_id}&page={page}&offset={offset}&key={key}".format(key=key, group_id = group_id, page = 200, offset = offset)
                # print url_group
                # url_group= "https://api.meetup.com/2/members?group_id={group_id}&page={page}&offset={offset}&key={key}".format(key=key, group_id = group_id, page = 200, offset = offset)
                print url_group
                group_response = urllib.urlopen(url_group)
                members_data = json.loads(group_response.read())
                members_results = members_data['results']
                # list_members += members_results
                # print 'type', type(members_results)
                # print 'len', len(members_results)
                # print members_results[0]

                # break
                if int(group_response.info()['X-RateLimit-Remaining'])<5:
                    sleep(int(group_response.info()['X-RateLimit-Remaining'])+0.5)
                failed = False
            except:
                print "failed, retrying..."
#generate dictionary with (key) group and (value) a list of members of that group
        if len(members_results) ==0:
            groups_members[group_id]=[]
            print "no members. Continuing..."
        else:
            for member in members_results:
                try:
                    table_tech.insert_one({'member_id': str(member['id']), 'member_data' : member})
                    if groups_members.get(group_id, False):
                        groups_members[group_id].append(str(member['id']))
                    else:
                        groups_members[group_id]=[str(member['id'])]

                except:
                    print "Mongo save failed. Saving to local folder instead..."
                
                    json.dump({'member_id': member['id'], 'member_data' : str(member)}, open(str(member['id']) + ".txt",'w'))
                    if groups_members.get(group_id, False):
                        groups_members[group_id].append(str(member['id']))
                    else:
                        groups_members[group_id]=[str(member['id'])]
                count+=1

                print str(count) + " members saved" 
                print str(groups_count) + " group in progress"      

#some files are too big to save to mongo. try-except will catch these failures
#and save the file to folder instead.
#originally all data was saved to computer as well as mongo as a backup ( notice all members data
# is in groups_members_tech folder. I found that it was much faster to query
# mongo data than to load each group's data from files (each group had its own file because
# json.dumps dumped an empty list when I tried to save too many at once)
#so i modified the code to only save locally when necessary.


    
print "Finished collecting data!"
print "Now saving groups_members dictionary to project folder as groupMembersAbr.txt ..."
json.dump(groups_members, open("groupMembersAbr.txt",'w'))
print "Using groups_members to generate members_groups dictionary"

users_groups = {}
count = 0
failed = []
for key, value in groups_members.iteritems():

    try:
        print "looping through users of group " + key + " with " + str(len(value)) + ' members'  
        for user in value:
            if users_groups.get(user, False):
                users_groups[user].append(key)
            else:
                users_groups[user]= [key]
        count+=1
        print "finished assignments for group " + str(count)
    except:
        failed.append(key)
        print "assignments for group " + key + " failed. Continuing to next group..."
print "Finished assignments!!! Now saving dictionary to users_groups.txt..."

json.dump(users_groups, open("users_groups.txt",'w'))

print "Save complete."
print "Assignment failed for groups:", failed
print "Done!"
