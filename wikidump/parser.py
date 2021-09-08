from gensim.corpora.wikicorpus import extract_pages, filter_wiki
import pandas as pd
import os
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

    @staticmethod
    def get(path):
        try:
            os.mkdir(path)
            print('Dump Directory created')
        except OSError as error:
            print('Dump Directory already exists, skipping...')

    def get_extra_info(self, article_id):
        req = requests.get(self.wiki_metadata_url.format(self.language, article_id)).json()
        return req['query']['pages'][article_id]['fullurl'], \
               req['query']['pages'][article_id]['canonicalurl']

    def get_categories(self, article_id):
        req = requests.get(self.wiki_categories_url.format(self.language, article_id)).json()
        categories = []
        for category in req['query']['pages'][article_id].get('categories', []):
            categories.append({article_id, category['title']})
        return categories

    def process_article(self, payload):
        title, text, article_id = payload
        print('Importing {}'.format(title))
        return {"article_id": article_id, "title": title, "Text": filter_wiki(text)}

    def process_file(self, path, processes=multiprocessing.cpu_count()):
        pool = multiprocessing.Pool(processes)
        data = extract_pages(bz2.BZ2File(path))
        for group in gensim.utils.chunkize(data, chunksize=10 * processes):
            for parsed_data in pool.imap(self.process_article, group):
                yield parsed_data
        pool.terminate()

    def create_data_frames(self):
        for f in os.listdir(self.language):
            file_path = os.path.join(self.language, f)
            data_for_df = [article for article in self.process_file(file_path)]
            df = pd.DataFrame(data_for_df)
            df.to_pickle(os.path.join(self.language, "{}_df.pkl".format(f)))
            print('Removing file {}'.format(file_path))
            os.remove(file_path)

    def unify_data_frames(self):
        dfs = [pd.read_pickle(os.path.join(self.language, f)) for f in os.listdir(self.language) if f.endswith('pkl')]
        full_df = pd.concat(dfs)
        full_df.to_pickle(os.path.join(self.language, "full_df.pkl"))
