import os

from gensim.test.utils import datapath, get_tmpfile
from gensim.corpora import WikiCorpus, MmCorpus
from gensim.corpora.wikicorpus import extract_pages, filter_wiki
import pandas as pd
import bz2
import gensim
import requests
import multiprocessing

MAX_PROCESSES = max(1, multiprocessing.cpu_count() - 1)

class WikiParser(object):
    wiki_metadata_url = 'https://{}.wikipedia.org/w/api.php?action=query&prop=info&pageids={}&inprop=url&format=json'
    wiki_categories_url = 'https://{}.wikipedia.org/w/api.php?action=query&prop=categories&pageids={}&format=json'

    def __init__(self, language):
        self.language = language
        self.content_df = pd.DataFrame(columns=['id', 'title', 'Text'])
        self.categories_df = pd.DataFrame(columns=['id', 'category'])

    def get_extra_info(self, article_id):
        req = requests.get(self.wiki_metadata_url.format(self.language, article_id)).json()
        return req['query']['pages'][article_id]['fullurl'],\
               req['query']['pages'][article_id]['canonicalurl']

    def get_categories(self, article_id):
        req = requests.get(self.wiki_categories_url.format(self.language, article_id)).json()
        categories = []
        for category in req['query']['pages'][article_id].get('categories', []):
            categories.append({article_id, category['title']})
        return categories
    def process_article(self, title, text, article_id):
        print('Importing {}'.format(title))
        #full_url, canonical_url = self.get_extra_info(article_id)
        data = {"title": title,
                "pageid": article_id,
                "text": filter_wiki(text)}
        #        "full_url": full_url,
        #        "canonical_url": canonical_url}
        return data

    def process_file(self, path, processes=multiprocessing.cpu_count()):
        pool = multiprocessing.Pool(processes)
        data = extract_pages(bz2.BZ2File(path))
        for group in gensim.utils.chunkize(data, chunksize=10 * processes):
            for parsed_data in pool.imap(self.process_article, group):
                print(parsed_data['title'])
                yield parsed_data

            #self.content_df.append(data, ignore_index=True)
        # self.categories_df.append(self.get_categories(pageid), ignore_index=True)
            #self.content_df.to_pickle("./content.pkl")
        # self.categories_df.to_pickle("./categories.pkl")

        pool.terminate()

    def create_data_frames(self):
        for file in os.listdir(self.language):
            for article in self.process_file(os.path.join(self.language, file)):
                self.content_df.append(article, ignore_index=True)
        self.content_df.to_pickle("./content.pkl")


parser = WikiParser('es')
parser.create_data_frames()