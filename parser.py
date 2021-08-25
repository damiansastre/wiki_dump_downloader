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
    wiki_categories_url = 'https://{}.wikipedia.org/w/api.php?action=query&prop=categories&pageids={}&format=json'

    def __init__(self, language):
        self.language = language
        self.content_df = pd.DataFrame(columns=['id', 'title', 'full_url', 'canonical_url', 'Text'])
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

    def create_data_frames(self):
        for file in os.listdir(self.language):
            path_to_wiki_dump = os.path.join(self.language, file)
            for title, text, pageid in extract_pages(bz2.BZ2File(path_to_wiki_dump)):
                full_url, canonical_url = self.get_extra_info(pageid)

                data = {"title": title,
                        "pageid": pageid,
                        "text": filter_wiki(text),
                        "full_url": full_url,
                        "canonical_url": canonical_url}
                self.content_df.append(data)
                self.categories_df.append(self.get_categories(pageid))
            self.content_df.to_pickle("./content.pkl")
            self.categories_df.to_pickle("./categories.pkl")
            exit()



parser = WikiParser('es')
parser.create_data_frames()