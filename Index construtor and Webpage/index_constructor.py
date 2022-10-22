import re
import json
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
from collections import defaultdict
import nltk.stem as ns
import pandas as pd
import numpy as np
import sqlalchemy
from math import log10, sqrt
import time

#import nltk
#nltk.download('punkt') # you may need to uncomment these two lines and run for only once, then comment out

class IndexConstructer:
    
    def __init__(self,base_path,url_path,engine):
        self.base_path      = base_path
        self.url_path       = url_path
        self.engine         = engine
        self.DocNum         = 0
        self.uniqueWord     = 0 
        self.dict_token_df  = defaultdict(int)
        self.df_DocUrl      = pd.DataFrame(columns = ('docid','url'))
        self.df_Index       = pd.DataFrame(columns = ('token','docid','tf'))

    def start_constructing(self):
        '''
        Thic function call the major fucntion of this class
        '''
        self.load_doc_url()
        timer = self.load_index_for_all()
        return timer
        
    
    def load_doc_url(self):
        '''
        This fucntion stores the DocId and it corresponding info into the database
        '''
        ##read doc with the url infomation
        DocUrl = pd.read_csv(self.url_path,sep='\t',header= None)
        DocUrl.columns = ['docid','url']

        #### Add title and excerpt to doc_url table
        ct = 0
        titles = []
        excerpts = []
        for doc_index in range(self.DocNum):
            if ct == 50000:
                break
            path = self.base_path + DocUrl['docid'][doc_index]
            soup = BeautifulSoup(open(path), features='lxml')
            titles.append(str(soup.find('title')).replace('<title>', '').replace('</title>', '').strip().replace('\n', ' '))
            ct += 1
            if (len(soup.get_text()) < 200):
                excerpts.append(soup.get_text().strip().replace('\n', ' '))
            else:
                excerpts.append(soup.get_text()[0:200].strip().replace('\n', ' '))
        
        DocUrl['title'] = pd.Series(titles)
        DocUrl['excerpt'] = pd.Series(excerpts)

        ##store the Doc info for further use
        DocUrl = DocUrl.reset_index()
        self.DocNum    = DocUrl.shape[0]
        self.df_DocUrl = DocUrl
        
        ##export to database
        DocUrl.to_sql("doc_url", con = self.engine, if_exists = 'replace', index = False)
        
        return 

    def load_index_for_all(self):
        '''
        This function construct all the index and store them into the database
        '''
        timer1 = time.perf_counter()
        ct = 0 # a counter for limiting the iteration
        for DocId_index in range(self.DocNum):
            if ct == 50000: # modify as needed
                break
            self.indexing_for_doc(DocId_index)
            ct += 1
            print('doc-'+str(ct))
            
        timer2 = time.perf_counter()
        
        self.uniqueWord = len(self.dict_token_df)
        
        timer3 = time.perf_counter()
        ##calculate the tf-idf 
        tf_idf_list=[]
        for i in range(self.df_Index.shape[0]):
            tf_idf_list.append( ##[1+log(tf)*log(N/df)]
                               (1+log10(self.df_Index['tf'][i]))
                               * log10(self.DocNum/self.dict_token_df[self.df_Index['token'][i]])
                               )
        self.df_Index['tf_idf'] = tf_idf_list
        
        timer4 = time.perf_counter()

        ##calculate the normalized tf-idf
        self.df_Index['square_tf_idf'] = self.df_Index['tf_idf']**2
        groupby = self.df_Index.groupby('docid')
        df_doc_len = groupby.sum()['square_tf_idf'].reset_index()
        df_doc_len['length'] = df_doc_len['square_tf_idf'].apply(sqrt)

        timer5 = time.perf_counter()

        self.df_Index = pd.merge(self.df_Index,df_doc_len,on='docid',how='left')
        self.df_Index['norm_tf_idf'] = self.df_Index['tf_idf']/self.df_Index['length']
        self.df_Index = self.df_Index[['token','docid','norm_tf_idf']]
        
        timer6 = time.perf_counter()

        self.df_Index.to_sql("index", con = self.engine, if_exists = 'replace', index = False)
        timer7 = time.perf_counter()
        
        return {'index':timer2-timer1,'tf-idf':timer4-timer3,'length':timer5-timer4,'norm':timer6-timer5,'export':timer7-timer6}


        
    def indexing_for_doc(self,DocId_index):
        '''
        This fucntion store the token and term frequency infomation in each doc into the data frame
        '''
        tokens = self.tokenize(self.base_path + self.df_DocUrl['docid'][DocId_index])
        
        tk_counter = defaultdict(int)
    
        for tk in tokens:
            tk_counter[tk]+=1

        newIndex = []
        for tk,tf in tk_counter.items():
            newIndex.append({'token':tk,'docid':DocId_index,'tf':tf})
            self.dict_token_df[tk] += 1 ##count the document frequency for this token
        
        self.df_Index = pd.concat([self.df_Index,pd.DataFrame(newIndex)],ignore_index=True)

        return
        

    def tokenize(self,path):
        '''
        This function tokenizes a document with given url and return a list of tokens
        '''
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
        
        result = []

        soup = BeautifulSoup(open(path), features='lxml')
        text = soup.get_text().lower()

        ## replace all non-word characters to space
        processed_text = re.sub(r'[^a-z^0-9^\'^\-]', ' ', text)
        tokens = word_tokenize(processed_text)
        lemmatizer = ns.WordNetLemmatizer()
        
        for token in tokens:
            if token not in stopwords:
                ## if a token starts or ends with - or ' then...
                if token.startswith(("'", "-")) or token.endswith(("'", "-")):
                    ## trim it and only keep the letter and number
                    trimed_token = re.sub(r'^[\'\-]+|[\'\-]+$', '', token)
                    if trimed_token != "":
                        result.append(lemmatizer.lemmatize(trimed_token))
                else:
                    result.append(lemmatizer.lemmatize(token))

        return result
   




