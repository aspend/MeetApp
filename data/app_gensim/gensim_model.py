from gensim.models.ldamodel import LdaModel
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import numpy as np
import nltk
from nltk import tokenize
from nltk.corpus import stopwords
import pandas as pd
from gensim import corpora, models, similarities
from gensim.models import hdpmodel, ldamodel
from itertools import izip


#define docs array and ids array
stops = stopwords.words('english')
stops += map(unicode,['http', 'https', 'meetup', 'group'])

def tokenize_and_normalize(chunks):
    words = [ tokenize.word_tokenize(sent) for sent in tokenize.sent_tokenize(chunks) ]
    flatten = [ inner for sublist in words for inner in sublist ]
    stripped = [] 

    for word in flatten: 
        if word not in stops:
            try:
                stripped.append(word.encode('latin-1').decode('utf8').lower())
            except:
                #print "Cannot encode: " + word
                pass
            
    return [ word for word in stripped if len(word) > 1 ] 

parsed = [ tokenize_and_normalize(text) for text in docs_tech ]

dictionary = corpora.Dictionary(parsed)
corpus = [dictionary.doc2bow(text) for text in parsed]
# dictionary.save('dictionary.dict')
# corpora.MmCorpus.serialize('corpus.mm', corpus)

# dictionary = corpora.Dictionary.load('dictionary.dict')
# corpus = corpora.MmCorpus('corpus.mm')

tfidf = models.TfidfModel(corpus)
corpus_tfidf = tfidf[corpus]

# lda7=LdaModel(corpus_tfidf, id2word=dictionary, num_topics=100, update_every=0, passes=100)


doc_in = "Human computer interaction"
vec_bow = dictionary.doc2bow(doc_in.lower().split())
## convert the query to LSI space
vec_lsi = lsi[vec_bow] 

## transform corpus to LSI space and index it
index = similarities.MatrixSimilarity(lsi[corpus]) 

# index.save('index.index')
# index = similarities.MatrixSimilarity.load('index.index')

## perform a similarity query against the corpus
sims = index[vec_lsi]
## print (document_number, document_similarity) 2-tuples
# print(list(enumerate(sims))) 
# [(0, 0.99809301), (1, 0.93748635), (2, 0.99844527), (3, 0.9865886), (4, 0.90755945),
# (5, -0.12416792), (6, -0.1063926), (7, -0.098794639), (8, 0.05004178)]


sims = sorted(enumerate(sims), key=lambda item: -item[1])
## print sorted (document number, similarity score) 2-tuples
print(sims) 
## [(2, 0.99844527), # The EPS user interface management system
## (0, 0.99809301), # Human machine interface for lab abc computer applications
## (3, 0.9865886), # System and human system engineering testing of EPS
## (1, 0.93748635), # A survey of user opinion of computer system response time
## (4, 0.90755945), # Relation of user perceived response time to error measurement
## (8, 0.050041795), # Graph minors A survey
## (7, -0.098794639), # Graph minors IV Widths of trees and well quasi ordering
## (6, -0.1063926), # The intersection graph of paths in trees
## (5, -0.12416792)] # The generation of random binary unordered trees

##text of first thing in sorted : (can also use ind to get group id)
print docs_tech[sims[0][0]]


##useful: 
# lda7.show_topics(num_topics=200, num_words = 7, formatted=True)