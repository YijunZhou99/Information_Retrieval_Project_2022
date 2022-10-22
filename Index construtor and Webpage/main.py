from index_constructor import *
from basic_query import *
from collections import defaultdict
import os
from sqlalchemy import create_engine,inspect
import time
import eel
import pandas as pd
from bs4 import BeautifulSoup

if __name__ == '__main__':
    base_path = "/Users/yijunzhou/Desktop/CS121/hw3/WEBPAGES_RAW/" 
    url_path = "/Users/yijunzhou/Desktop/CS121/hw3/WEBPAGES_RAW/bookkeeping.tsv"

    sql_connect = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format('postgres','2245','localhost','5432','cs121')
    sql_engine = create_engine(sql_connect)

    ##check if index exist:
    status = inspect(sql_engine).has_table('index')
    if status == True:
        pass
    else:
        ##construct index
        start =time.perf_counter()
        indexconstructer = IndexConstructer(base_path,url_path,sql_engine)
        time_list = indexconstructer.start_constructing()
        end = time.perf_counter()
        print('Total Running time: %s Seconds'%(end-start))
        print(time_list)

    ##search
    search = SearchEngine(sql_engine)
    search.get_doc_info()


    ## Initialize GUI
    eel.init('static')

        ## Expose python function to GUI 
    @eel.expose
    def start_search(q):
        resutl_list = search.search(q)
        return [resutl_list[0].to_json(), resutl_list[1], resutl_list[2]]

    ## Start GUI - run forever until window is closed
    eel.start('index.html')


