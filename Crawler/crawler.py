import logging
import re
from urllib.parse import parse_qs, urlparse
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import defaultdict

logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier       = frontier
        self.corpus         = corpus
        self.most_word      = 0
        self.longest_page   = ""
        self.unique         = set()
        self.unique_noquery = defaultdict(int)
        self.subdomain      = defaultdict(int)
        self.PageoutLinks   = defaultdict(int)
        self.word_freq      = defaultdict(int)
        self.traps          = set()
        self.downloadedURLs = set()
        self.processedURL   = set()
        self.stopwords      = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off',
                          'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        index = 1
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s",
                        url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            if index == 1:
                self.subdomain[urlparse(url).netloc] += 1
                self.processedURL.add(url)
            else:
                if self.subdomain.get(urlparse(url).netloc):
                    self.subdomain[urlparse(url).netloc] = 2
                else:
                    self.subdomain[urlparse(url).netloc] = 1

            self.downloadedURLs.add(url)

            for next_link in self.extract_next_links(url_data):
                self.processedURL.add(next_link)
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        self.PageoutLinks[url] += 1
            index += 1

        for u in self.processedURL:
            if self.subdomain.get(urlparse(u).netloc):
                self.subdomain[urlparse(u).netloc] += 1

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []

        if url_data['content'] == None:
            return outputLinks

        else:

            # Parsing the html file into a BeautifulSoup object, with 'lxml' parser
            # lxml parser is faster than the builtin 'html.parser'
            soup = BeautifulSoup(url_data['content'], features='lxml')

            # Extracting all <a> tags whose href attribute has a value from the soup
            tags = soup.find_all('a', href=True)

            # Extract all words from the soup
            words = soup.get_text().split()

            word_count = 0

            # Add extracted words to word frequency dict
            for word in words:
                if re.match("[A-Za-z0-9\']{2,}", word):
                    onlyword = re.sub("[\;\,\:\.]", "", word.lower())
                    if onlyword not in self.stopwords:
                        self.word_freq[onlyword] += 1
                        
                    # Count the number of words in the page
                    word_count += 1
            
            # Determine if the current page is longest
            if (word_count > self.most_word):
                self.most_word = word_count
                self.longest_page = url_data['url']
                
            for tag in tags:

                base_url = url_data['url']

                # Extracting url from <a> tag
                extracted_url = tag.get("href").strip()

                # Transforming url into absolute form
                absolute_url = urljoin(base_url, extracted_url)

                # Adding the absolute url into outputLinks list
                outputLinks.append(absolute_url)

            return outputLinks

    def is_valid(self, url):

        # Check if url is too long
        if len(url) > 200:
            self.traps.add(url)
            return False

        # Parse the url
        parsed = urlparse(url)

        # Check if the url has fetched but with a different fragment
        if (parsed.netloc.lower()+parsed.path.lower()+parsed.params.lower()+parsed.query.lower()) in self.unique:
            self.traps.add(url)
            return False

        # Check if the scheme is http(s)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # Store parameters to a dictionary
        paras = parse_qs(parsed.query, keep_blank_values=True)

        # Check if there are too many parameters
        if len(paras) >= 4:
            self.traps.add(url)
            return False

        # Filter the calendar
        if re.match("^.*calendar.*$", parsed.path.lower()) and (parsed.netloc.lower()+parsed.path.lower()+parsed.params.lower()) in self.unique_noquery.keys():
            # Check if the same url without parameter has been fetched for 10 time
            if self.unique_noquery[parsed.netloc.lower()+parsed.path.lower()+parsed.params.lower()] > 10:
                self.traps.add(url)
                return False

        # Check for duplicated segments in path
        paths = parsed.path.split('/')
        frequency = {}
        for seg in paths:
            if seg in frequency.keys():
                frequency[seg] += 1
            else:
                frequency[seg] = 1
        if any(value > 2 for value in frequency.values()):
            self.traps.add(url)
            return False

        for key, value in paras.items():
            # If the URL has parameter...
            if len(value) > 0:
                # Check if the value of any parameter is too long, it could be a seesion id
                if value[0].isalnum() and len(value[0]) >= 25:
                    self.traps.add(url)
                    return False
                # Check for certain actions
                if key.lower() == 'action' and value[0] == ('download' or 'upload' or 'login'):
                    return False

        # Added ODF file extensions, .bam .bigwig .bw .lif
        try:
            if ".ics.uci.edu" in parsed.hostname \
                and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"
                                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|odt|odp|ods|odg|odb|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1"
                                 + "|thmx|mso|arff|rtf|jar|csv"
                                 + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|bam|bigwig|bw|lif)$", parsed.path.lower()) \
                and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"
                                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|odt|odp|ods|odg|odb|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1"
                                 + "|thmx|mso|arff|rtf|jar|csv"
                                 + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|bam|bigwig|bw|lif)$", parsed.query.lower()):

                # Add the url to the dictionary of unique urls
                self.unique.add(parsed.netloc.lower()+parsed.path.lower()+parsed.params.lower()+parsed.query.lower())

                self.unique_noquery[parsed.netloc.lower() +
                                    parsed.path.lower()+parsed.params.lower()] += 1

                return True

            return False

        except TypeError:
            print("TypeError for ", parsed)
            return False
