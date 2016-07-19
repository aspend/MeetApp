import numpy as np
import urllib
import json
import pandas as pd
from time import sleep
import requests
from pymongo import MongoClient
from collections import Counter
from nltk.corpus import stopwords

users_groups = json.load(open('app_gensim/app_all_users_groups.txt'))

#members data

db_cilent = MongoClient()
db = db_cilent['meetup']
members_table = db['members_data']

cursor = members_table.find()
members = [x for x in cursor]

dmembers = pd.DataFrame([{'status':doc['member_data'].get('status'),'city':doc['member_data'].get('city'),\
                         'photo':doc['member_data'].get('photo'), 'name':doc['member_data'].get('name'), \
                         'other_services':doc['member_data'].get('other_services'), 'topics':doc['member_data'].get('topics'),\
                         'link':doc['member_data'].get('link'), 'messaging_pref':doc['member_data'].get('messaging_pref'), \
                         'hometown':doc['member_data'].get('hometown'), 'id':doc['member_data'].get('id')}\
                         for doc in members])
dmembers['id']= dmembers['id'].apply(lambda x: str(x))

dmembers_unique = dmembers.groupby('id').first()
dmembers_unique['topic_names']= dmembers_unique['topics'].apply(lambda x: map(lambda y: y['name'].encode('UTF-8'), x ) if x else [])
dmembers_unique['in_sf'] = dmembers_unique['city'].apply(lambda x: 'San Francisco' in x if x else False)
dmembers_unique['thumb_link'] = dmembers_unique['photo'].apply(lambda x: x.get('thumb_link','') if x else '')
dmembers_unique['photo_link'] = dmembers_unique['photo'].apply(lambda x: x.get('photo_link','') if x else '')
dmembers_unique['is_connected'] = dmembers_unique['other_services'].apply(lambda x: len(x.keys())>0)
dmembers_unique['n_connected'] = dmembers_unique['other_services'].apply(lambda x: len(x.keys()))
dmembers_unique['n_topics'] = dmembers_unique['topic_names'].apply(lambda x: len(x))
dmembers_unique['n_groups'] = dmembers_unique['topic_names'].apply(lambda x: len(x))
dmembers_unique['n_groups']= map(lambda x: len(users_groups[x]), dmembers_unique.index.values)

cities_data =[{"zip":"94101","country":"us","localized_country_name":"USA","distance":0.06379063524396955,"city":"San Francisco","lon":-122.41999816894531,"ranking":0,"id":94101,"state":"CA","member_count":60351,"lat":37.779998779296875},{"zip":"94601","country":"us","localized_country_name":"USA","distance":10.883503670099289,"city":"Oakland","lon":-122.22000122070312,"ranking":1,"id":94601,"state":"CA","member_count":13524,"lat":37.779998779296875},{"zip":"94701","country":"us","localized_country_name":"USA","distance":10.276455883472025,"city":"Berkeley","lon":-122.2699966430664,"ranking":2,"id":94701,"state":"CA","member_count":5844,"lat":37.869998931884766},{"zip":"94401","country":"us","localized_country_name":"USA","distance":15.449428937175858,"city":"San Mateo","lon":-122.31999969482422,"ranking":3,"id":94401,"state":"CA","member_count":3775,"lat":37.56999969482422},{"zip":"94901","country":"us","localized_country_name":"USA","distance":14.07543285576537,"city":"San Rafael","lon":-122.51000213623047,"ranking":4,"id":94901,"state":"CA","member_count":3192,"lat":37.970001220703125},{"zip":"94501","country":"us","localized_country_name":"USA","distance":8.802416096848393,"city":"Alameda","lon":-122.26000213623047,"ranking":5,"id":94501,"state":"CA","member_count":1808,"lat":37.7599983215332},{"zip":"94941","country":"us","localized_country_name":"USA","distance":11.229695296162893,"city":"Mill Valley","lon":-122.56999969482422,"ranking":6,"id":94941,"state":"CA","member_count":928,"lat":37.88999938964844},{"zip":"94010","country":"us","localized_country_name":"USA","distance":14.820939091467269,"city":"Burlingame","lon":-122.36000061035156,"ranking":7,"id":94010,"state":"CA","member_count":916,"lat":37.56999969482422},{"zip":"94577","country":"us","localized_country_name":"USA","distance":14.751771081054846,"city":"San Leandro","lon":-122.16000366210938,"ranking":8,"id":94577,"state":"CA","member_count":902,"lat":37.720001220703125},{"zip":"94608","country":"us","localized_country_name":"USA","distance":7.8781582270957236,"city":"Emeryville","lon":-122.29000091552734,"ranking":9,"id":94608,"state":"CA","member_count":873,"lat":37.83000183105469},{"zip":"94553","country":"us","localized_country_name":"USA","distance":20.28294315155349,"city":"Martinez","lon":-122.16000366210938,"ranking":10,"id":94553,"state":"CA","member_count":652,"lat":37.9900016784668},{"zip":"94013","country":"us","localized_country_name":"USA","distance":7.308190266041847,"city":"Daly City","lon":-122.52999877929688,"ranking":11,"id":94013,"state":"CA","member_count":644,"lat":37.720001220703125},{"zip":"94530","country":"us","localized_country_name":"USA","distance":11.697185006027171,"city":"El Cerrito","lon":-122.30000305175781,"ranking":12,"id":94530,"state":"CA","member_count":591,"lat":37.91999816894531},{"zip":"94044","country":"us","localized_country_name":"USA","distance":12.979228977255158,"city":"Pacifica","lon":-122.48999786376953,"ranking":13,"id":94044,"state":"CA","member_count":522,"lat":37.599998474121094},{"zip":"94801","country":"us","localized_country_name":"USA","distance":11.566070951958155,"city":"Richmond","lon":-122.36000061035156,"ranking":14,"id":94801,"state":"CA","member_count":511,"lat":37.939998626708984},{"zip":"94080","country":"us","localized_country_name":"USA","distance":8.952191382998148,"city":"South San Francisco","lon":-122.43000030517578,"ranking":15,"id":94080,"state":"CA","member_count":511,"lat":37.650001525878906},{"zip":"94706","country":"us","localized_country_name":"USA","distance":10.403626057211728,"city":"Albany","lon":-122.29000091552734,"ranking":16,"id":94706,"state":"CA","member_count":422,"lat":37.88999938964844},{"zip":"94066","country":"us","localized_country_name":"USA","distance":10.331654637988587,"city":"San Bruno","lon":-122.43000030517578,"ranking":17,"id":94066,"state":"CA","member_count":397,"lat":37.630001068115234},{"zip":"94965","country":"us","localized_country_name":"USA","distance":8.705376043196331,"city":"Sausalito","lon":-122.52999877929688,"ranking":18,"id":94965,"state":"CA","member_count":390,"lat":37.869998931884766},{"zip":"94806","country":"us","localized_country_name":"USA","distance":14.047637487231258,"city":"San Pablo","lon":-122.33000183105469,"ranking":19,"id":94806,"state":"CA","member_count":264,"lat":37.970001220703125},{"zip":"94960","country":"us","localized_country_name":"USA","distance":16.117672280189467,"city":"San Anselmo","lon":-122.56999969482422,"ranking":20,"id":94960,"state":"CA","member_count":251,"lat":37.97999954223633},{"zip":"94930","country":"us","localized_country_name":"USA","distance":16.59989130091295,"city":"Fairfax","lon":-122.62000274658203,"ranking":21,"id":94930,"state":"CA","member_count":247,"lat":37.959999084472656},{"zip":"94920","country":"us","localized_country_name":"USA","distance":8.135780936393965,"city":"Belvedere Tiburon","lon":-122.47000122070312,"ranking":22,"id":94920,"state":"CA","member_count":215,"lat":37.88999938964844},{"zip":"94030","country":"us","localized_country_name":"USA","distance":12.432970036843463,"city":"Millbrae","lon":-122.4000015258789,"ranking":23,"id":94030,"state":"CA","member_count":211,"lat":37.599998474121094},{"zip":"94803","country":"us","localized_country_name":"USA","distance":14.942497203179126,"city":"El Sobrante","lon":-122.29000091552734,"ranking":24,"id":94803,"state":"CA","member_count":204,"lat":37.970001220703125},{"zip":"94904","country":"us","localized_country_name":"USA","distance":13.507371621210579,"city":"Greenbrae","lon":-122.54000091552734,"ranking":25,"id":94904,"state":"CA","member_count":181,"lat":37.95000076293945},{"zip":"94939","country":"us","localized_country_name":"USA","distance":12.90858561131417,"city":"Larkspur","lon":-122.54000091552734,"ranking":26,"id":94939,"state":"CA","member_count":179,"lat":37.939998626708984},{"zip":"94563","country":"us","localized_country_name":"USA","distance":14.790379427486833,"city":"Orinda","lon":-122.18000030517578,"ranking":27,"id":94563,"state":"CA","member_count":171,"lat":37.880001068115234},{"zip":"94925","country":"us","localized_country_name":"USA","distance":11.773435848966063,"city":"Corte Madera","lon":-122.5199966430664,"ranking":28,"id":94925,"state":"CA","member_count":138,"lat":37.93000030517578},{"zip":"94005","country":"us","localized_country_name":"USA","distance":6.258457627183688,"city":"Brisbane","lon":-122.4000015258789,"ranking":29,"id":94005,"state":"CA","member_count":61,"lat":37.689998626708984},{"zip":"94620","country":"us","localized_country_name":"USA","distance":11.429780363320516,"city":"Piedmont","lon":-122.20999908447266,"ranking":30,"id":94620,"state":"CA","member_count":29,"lat":37.779998779296875},{"zip":"94957","country":"us","localized_country_name":"USA","distance":14.653844845810296,"city":"Ross","lon":-122.55999755859375,"ranking":31,"id":94957,"state":"CA","member_count":23,"lat":37.959999084472656},{"zip":"94516","country":"us","localized_country_name":"USA","distance":12.994981101353284,"city":"Canyon","lon":-122.19000244140625,"ranking":32,"id":94516,"state":"CA","member_count":6,"lat":37.83000183105469},{"zip":"94914","country":"us","localized_country_name":"USA","distance":13.507371621210579,"city":"Kentfield","lon":-122.54000091552734,"ranking":33,"id":94914,"state":"CA","member_count":4,"lat":37.95000076293945},{"zip":"94970","country":"us","localized_country_name":"USA","distance":14.644192018151147,"city":"Stinson Beach","lon":-122.63999938964844,"ranking":34,"id":94970,"state":"CA","member_count":4,"lat":37.900001525878906},{"zip":"94964","country":"us","localized_country_name":"USA","distance":11.588596184799856,"city":"San Quentin","lon":-122.4800033569336,"ranking":35,"id":94964,"state":"CA","member_count":3,"lat":37.939998626708984}]
cities_15_miles_sf = map(lambda x: x["city"], cities_data)
cities_string = ' '.join(cities_15_miles_sf)
uncities=dmembers_unique['city']
uncities_unique = uncities.unique()
cities_in_cities = [city.split(',')[0] in cities_string if city else 'maybe' for city in uncities]
dmembers_unique['near_sf'] = cities_in_cities

dg = dmembers_unique.copy()
dmembers_unique_filter = dg[(dg.messaging_pref=='all_members')&(dg.near_sf==True)\
                                &(dg.n_topics>0)&(dg.status=='active')&(dg.status=='active')&(dg.is_connected==True)]
dmembers_unique_filter_only = dmembers_unique_filter[dmembers_unique_filter.reset_index('id')['id'].isin(users_groups.keys()).values]
dmembers_unique_filter_only.to_pickle('memfiltclean1.pkl')


