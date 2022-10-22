import re
import pandas as pd
import nltk.stem as ns
from nltk.tokenize import word_tokenize
from collections import defaultdict
from math import log10, sqrt
import time

class SearchEngine:
    
    def __init__(self,engine):
        self.engine     = engine
        self.dict_query = defaultdict(int)
        self.df_DocUrl  = pd.DataFrame()
        self.DocNum     = 0
        
    def search(self,query):
        ##start timer
        timer1 = time.perf_counter()
        
        self.dict_query = defaultdict(int) 
        query = self.tokenize(query.lower())

        sql = '''SELECT token, docid,norm_tf_idf
                FROM index
                WHERE token in {};
            '''.format(query)
        

        score_df  = pd.DataFrame()
        
        try:
            reserves = pd.read_sql_query(sql,con=self.engine)

            ##calculate the normalized tf-idf for query
            query_df = reserves.groupby("token").count().reset_index()
            tf_idf_list = []
            for i in range(query_df.shape[0]):
                tf_idf_list.append( ##[1+log(tf)*log(N/df)]
                                   (1+log10(self.dict_query[query_df['token'][i]]))
                                   * log10(self.DocNum/query_df['docid'][i])
                                  )
            #print(self.dict_query)
            query_df['tf_idf_q'] = tf_idf_list
            query_df['square_tf_idf_q']= query_df['tf_idf_q']**2
            query_df['norm_tf_idf_q'] = query_df['tf_idf_q']/sqrt(sum(query_df['square_tf_idf_q']))
            query_df = query_df[['token','tf_idf_q','norm_tf_idf_q']]

            ##calculate the score using cosine similarity
            reserves = pd.merge(reserves,query_df,how='left',on='token')
            reserves['cosine'] = reserves['norm_tf_idf']*reserves['norm_tf_idf_q']
            score_df = reserves.groupby("docid").sum()
            score_df = score_df.sort_values(by=['cosine'], ascending=False).reset_index()

            #### Add doc info to the score_df
            score_df['docurl'] = score_df['docid'].apply(self.get_url)
            score_df['docid_raw'] = score_df['docid'].apply(self.get_docid)
            score_df['title'] = score_df['docid'].apply(self.get_title)
            score_df['excerpt'] = score_df['docid'].apply(self.get_excerpt)
            
            ##print the result
            rank= 20
            self.print_top_rank(score_df.head(rank))
            
        except KeyError:
                ## 如果没有result 就不用timer 直接返回result = 0
                return [score_df, 0, 0]

        ##end timer
        timer2 = time.perf_counter()

        search_time = timer2-timer1
        
        return [score_df.head(20), len(set(reserves['docid'])), search_time]
    
    #### Obtain raw docid,url, title, and excerpt from doc_url table
    def get_url(self,index):
        return self.df_DocUrl['url'][index]
    
    def get_docid(self, index):
        return self.df_DocUrl['docid'][index]

    def get_title(self, index):
        return self.df_DocUrl['title'][index]
    
    def get_excerpt(self, index):
        return self.df_DocUrl['excerpt'][index]
    
    
    def print_top_rank(self,score_df):
        for i in range(score_df.shape[0]):
            print('{:<10} {:<30} {:<30}'.format(i+1,score_df['cosine'][i],self.df_DocUrl['url'][score_df['docid'][i]]))
        
    def get_doc_info(self):
        
        sql = 'SELECT * FROM doc_url;'
        self.df_DocUrl = pd.read_sql_query(sql,con=self.engine).set_index('index',append=False)
        self.DocNum    = self.df_DocUrl.shape[0]
        
        
    def tokenize(self, raw_query):
        processed_text = re.sub(r'[^a-z^0-9^\'^\-]', ' ', raw_query)
        tokens = word_tokenize(processed_text)
        lemmatizer = ns.WordNetLemmatizer()
        stopwords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't",
		'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot',
		'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few',
		'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll",
		"he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll",
		"i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most',
		"mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our',
		'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't",
		'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's",
		'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up',
		'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where',
		"where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll",
		"you're", "you've", 'your', 'yours', 'yourself', 'yourselves']

        for token in tokens:
            if token not in stopwords:
                ## if a token starts or ends with - or ' then...
                if token.startswith(("'", "-")) or token.endswith(("'", "-")):
                    ## trim it and only keep the letter and number
                    trimed_token = re.sub(r'^[\'\-]+|[\'\-]+$', '', token)
                    if trimed_token != "":
                        self.dict_query[lemmatizer.lemmatize(trimed_token)] += 1
                else:
                    self.dict_query[lemmatizer.lemmatize(token)] += 1
        
        # Turn result to tuple-like string and keep quote mark
        tuplized_result = "(" + str(list(self.dict_query.keys()))[1:-1] + ")"
        return tuplized_result
