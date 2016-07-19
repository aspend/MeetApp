from flask import Flask, render_template, request
import numpy as np
import pandas as pd
import graphlab
from graphlab.toolkits.recommender import ranking_factorization_recommender
from gensim import corpora, models, similarities
from gensim.models.ldamodel import LdaModel
import nltk
from nltk.corpus import stopwords
import json
import scipy

app = Flask(__name__)

all_users_groups = json.load(open('data/app_gensim/app_all_users_groups.txt'))
groups_users = json.load(open('data/app_gensim/groups_users_filt.txt'))
users_groups = json.load(open('data/app_gensim/users_groups_filt.txt'))
users_topics = json.load(open('data/app_gensim/users_topics_filt.txt'))
groups_topics = json.load(open('data/app_gensim/group_topics.txt'))

# member_data = pd.read_pickle('data/memfiltcleanfinal.pkl')
member_data = pd.read_pickle('data/app_final_filt_members_data.pkl')
# member_data['id'] = member_data['id'].apply(lambda x: str(x))
# member_data.set_index('id', inplace = True)

group_data = pd.read_pickle('data/app_final_groups_data.pkl')
loaded_model = graphlab.load_model('data/groups_model')
dictionary = corpora.Dictionary.load('data/app_gensim/dictionary.dict')
corpus = corpora.MmCorpus('data/app_gensim/corpus_tfidf.mm')
# loaded_model.get_similar_users(users = ['68157442'], k=10)
lda = LdaModel.load('data/app_gensim/model.lda')
#group index --> {group_ind: [groupid, grouptext]}
lda_dict = json.load(open('data/app_gensim/lda_dict.txt'))

users_wanted = list(member_data.index.values)
member_data['score']= member_data['n_connected']+ member_data['n_topics']
groups_topics = json.load(open('data/app_gensim/group_topics.txt'))
users_groups = json.load(open('data/app_gensim/users_groups_filt.txt'))
a

dfref = member_data.to_dict('dict')

users_sims = np.load("users_sims.npy")


def get_sim_score(user_id):
    sim_score = np.sum(users_sims*users_sims[users.index(user_id)], axis =1)/1215.
    return sim_score
#get gensim scores:
def get_group_scores(user_text):
    doc_in = user_text
    vec_bow = dictionary.doc2bow(doc_in.lower().split())
    vec_lsi = lda[vec_bow] 
    index = similarities.MatrixSimilarity(lda[corpus], num_features = 2374)
    sims = index[vec_lsi]
    sims = sorted(enumerate(sims), key=lambda item: -item[1])
    group_scores_dict = {lda_dict[str(thing[0])][0]:thing[1] for thing in sims}
    return group_scores_dict


def topics_in_text_score(text, topics, decode = False):
    clean_text = ' '.join([word for word in text.lower().split() if word not in (stopwords.words('english')+['and'])])
#     topic_present_score = sum([int(topic+' ' in clean_text)*len(topic.split()) for topic in topics])
    if decode:
        topics = [x.decode('UTF8') for x in topics]
    topic_peices = ' '.join([word for word in topics if word not in (stopwords.words('english')+['and'])]).split()
    topic_peices_set = set(topic_peices)
    topic_partial_score = sum([int(word in topic_peices_set) for word in clean_text.split()])
#     scale = (topic_partial_score*0.2+topic_present_score*0.8)/len(set(clean_text.split()))
    scale = (topic_partial_score*1.0)/(len(set(clean_text.split()))+1.0)
    return scale
def get_sims(user_text):
    doc_in = user_text
    vec_bow = dictionary.doc2bow(doc_in.lower().split())
    vec_lsi = lda[vec_bow] 
    index = similarities.MatrixSimilarity(lda[corpus], num_features = 9286)
    sims = index[vec_lsi]
#     sims = sorted(enumerate(sims), key=lambda item: -item[1])
#     group_scores_dict = {lda_dict[str(thing[0])][0]:thing[1] for thing in sims}
    return sims

def get_top_ten_users(text, user_id):
    #get similarities of groups to text
    group_scores = get_group_scores(text)
    #rank groups by sims
    groups_by_sim = sorted(group_scores.items(), key=lambda x: x[1], reverse=True)
    #find number of groups needed to get 1000 members
    #store set of users in those groups and list of groups
    #some of groups don't have members with at least one social link, at least one topic and public messaging pref on meetup
    #this is why we're also recording group ids
    users=set([])
    groups = []
    for pair in groups_by_sim:
        if len(users)>=1000:
            break
        group_id = pair[0].encode('utf-8')
        get_users = groups_users.get(group_id, False)
        if get_users:
            users.update(get_users)
            groups.append(group_id)

    #members in these groups were stored in "users"
    #for each user, compute topics score based on how many of their topics were in text
    users_list = list(users)
    users_topics_in_text_scores = map(lambda x: topics_in_text_score(text, topics = member_data.loc[x]['topic_names'], decode = True), users_list)
    #compare user topics to groups, get cosine sim of user (using their topics) with active user(using input text)
    users_sims_dict = dict(zip(users_groups.keys(),users_sims ))
    current_user_sim = get_sims(text)
    compare_user_sims_score = [1. - scipy.spatial.distance.cosine(current_user_sim, users_sims_dict[user]) for user in users_list]

    #check if we recognize id
    if user_id in all_users_groups.keys():
        # get graphlab sim scores for each user
        gl_rankings = loaded_model.get_similar_users(users = [users_list[0]], k=300000)
        gl_rankings = gl_rankings.filter_by(users_list, 'similar')
        results_ids = np.array(gl_rankings['similar'])
        results_scores = np.array(gl_rankings['score'])
        gl_user_scores_dict = dict(zip(results_ids, results_scores))

        #compute total score for each user 
        # final score for users will involve:
        # gl_user_scores_dict
        # compare_user_sims_score
        # users_topics_in_text_scores
        users_tot_score = [(1.+users_topics_in_text_scores[i])*(1.+compare_user_sims_score[i])*(1.+gl_user_scores_dict.get(user,1)) for i,user in enumerate(users_list)]
        users_tot_score = np.nan_to_num(np.array(users_tot_score))
        users_tot_score_zip = zip(users_list, users_tot_score)
        users_top_ten, throw_away = zip(*sorted(users_tot_score_zip,reverse=True, key=lambda x: x[1]))
        return users_top_ten[:10]
    else:
        #for cold case where don't have cosine sims from graphlab:
        users_cold_tot_score = [(1.+users_topics_in_text_scores[i])*(1.+compare_user_sims_score[i]) for i,user in enumerate(users_list)]
        users_cold_tot_score = np.nan_to_num(np.array(users_cold_tot_score))
        users_cold_tot_score_zip = zip(users_list, users_cold_tot_score)
        users_top_ten, throw_away = zip(*sorted(users_cold_tot_score_zip,reverse=True, key=lambda x: x[1]))
        return users_top_ten[:10]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/landing', methods=['POST'])
def landing():
    user_id = None
    test = None

    if request.method == 'POST':
        user_id = request.form['text']
        test = request.form['summary']
        test = test.encode('UTF8')
        if len(test)==0:
            return render_template('index.html', error = "Please enter a description above.")
        top = get_top_ten_users(test, user_id)
        top = [user_id_str.encode('UTF-8') for user_id_str in top]

        return render_template('success.html',test = test, user_id=user_id, results=top, df = dfref)

@app.route('/random') 
def random():    
    rand10 = np.random.choice(list(member_data.index.values),size = 10, replace = False)
    results = list(rand10)
    return render_template('success.html',results=results, df = dfref, error='Here are some randomly generated Meetup users!')

if __name__ == '__main__':
    app.debug = True
    app.run()
