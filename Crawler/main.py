import atexit
import logging

import sys

from corpus import Corpus
from crawler import Crawler
from frontier import Frontier

if __name__ == "__main__":
    # Configures basic logging
    logging.basicConfig(format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    # Instantiates frontier and loads the last state if exists
    frontier = Frontier()
    frontier.load_frontier()

    # Instantiates corpus object with the given cmd arg
    corpus = Corpus(sys.argv[1])

    # Registers a shutdown hook to save frontier state upon unexpected shutdown
    atexit.register(frontier.save_frontier)

    # Instantiates a crawler object and starts crawling
    crawler = Crawler(frontier, corpus)
    crawler.start_crawling()

    ##Analytics Output
    f = open('Analytics.txt','w')
    
    f.write('【Analytics #1】\n')
    f.write('{:<35}{:<35}{:<30}\n'.format('URL','Subdomain','Num of URLs'))
    for k,v in crawler.subdomain.items():
        f.write('{:<35}{:<35}{:<30}\n'.format(k,k.lstrip('www.').rstrip('.uci.edu'),str(v-1)))

    f.write('\n【Analytics #2】\n')
    MostOutLinksPage = sorted(crawler.PageoutLinks.items(),key=lambda x:-x[1])[0]
    f.write('The page with the most valid out links is {}, with {} out links.\n'.format(MostOutLinksPage[0],MostOutLinksPage[1]))

    f.write('\n【Analytics #3】\n')
    f.write('-----Downloaded URLs: {} in total-----\n'.format(len(crawler.downloadedURLs)))
    for du in crawler.downloadedURLs:
        f.write(du+'\n')
    f.write('-----Traps: {} in total-----\n'.format(len(crawler.traps)))
    for t in crawler.traps:
        f.write(t+'\n')
    
    f.write('\n【Analytics #4】\n')
    f.write('The longest page in terms of number of words is:\n')
    f.write(crawler.longest_page + '\n')
    
    f.write('\n【Analytics #5】\n')
    ordered_freq = sorted(crawler.word_freq.items(), key=lambda x: x[1], reverse=True)[0:50]
    f.write('{:<5}{:<25}{}\n'.format('#', 'Word', 'Frequency'))
    counter = 1
    for freq in ordered_freq:
        f.write('{:<5}{:<25}{}\n'.format(counter, freq[0], freq[1]))
        counter += 1
    f.write('\n')

    f.close()
    
