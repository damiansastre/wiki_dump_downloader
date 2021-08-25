import os

from gensim.test.utils import datapath, get_tmpfile
from gensim.corpora import WikiCorpus, MmCorpus
from gensim.corpora.wikicorpus import extract_pages, filter_wiki
import pandas as pd
import bz2
import gensim
import requests



class WikiParser(object):
    wiki_metadata_url = 'https://{}.wikipedia.org/w/api.php?action=query&prop=info&pageids={}&inprop=url&format=json'
    wiki_categories_url = 'https://{}.wikipedia.org/w/api.php?action=query&prop=categories&pageids={}'

    def __init__(self, language):
        self.language = language

    def get_extra_info(self, article_id):
        req = requests.get(self.wiki_metadata_url.format(self.language, article_id)).json()
        payload = {"full_url": req['fullurl'],
                   "canonical_url": req['canonicalurl']}
        return payload

    def get_categories(self, article_id):
        req = requests.get(self.wiki_categories_url.format(self.language, article_id)).json()
        categories = []
        for category in req['query']['pages'][article_id]['categories']:
            categories.append(category['title'])
        return categories

    def create_data_frames(self):
        for file in os.listdir(self.language):
            path_to_wiki_dump = os.path.append(self.language, file)
            for title, text, pageid in extract_pages(bz2.BZ2File(path_to_wiki_dump)):
                data = {"title": title,
                        "pageid": pageid,
                        "text": filter_wiki(text),
                        "categories": self.get_categories(pageid),
                        "extra_info": self.get_extra_info(pageid)}
                print(data)


parser = WikiParser('es')
parser.create_data_frames()