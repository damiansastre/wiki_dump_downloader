from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import re
import os


class WikiDumpCrawler(object):
    base_url = 'https://dumps.wikimedia.your.org/'
    index_url = base_url + 'backup-index.html'
    article_title = 'Articles, templates, media/file descriptions, and primary meta-pages.'
    size_regex = r"\d+\.\d+"

    @staticmethod
    def build_file_path(name, folder):
        return os.path.join(folder, name)

    @staticmethod
    def get_language_directory(language):
        try:
            os.mkdir(language)
            print('Language Directory created')
        except OSError as error:
            print('Language Directory already exists, skipping...')

    def build_url(self, uri):
        return self.base_url + uri

    def download_file(self, link, language):
        resp = requests.get(self.build_url(link['url']), stream=True)
        total = int(resp.headers.get('content-length', 0))
        with open(self.build_file_path(link['filename'], language), 'wb') as file, tqdm(
                desc=link['filename'],
                total=total,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)

    def get_links(self, page):
        soup = BeautifulSoup(page, 'html.parser')
        data = []
        for ul in soup.find('b', string=self.article_title).find_parent('li').find('ul').find_all('li'):
            a = ul.find('a')
            size = float(re.findall(self.size_regex, ul.text.strip(a.text))[0]) if \
                re.findall(self.size_regex, ul.text.strip(a.text)) else 0
            new_link = {"url": a['href'],
                        "filename": a.text,
                        "size": size}
            data.append(new_link)
        return data, sum(link['size'] for link in data)

    def get_language_dumps(self, language):
        print('Starting Download Process')
        req = requests.get(self.index_url)
        soup = BeautifulSoup(req.text, 'html.parser')
        language_page = soup.find('a', string='{}wiki'.format(language))
        req = requests.get(self.base_url + language_page['href'])
        self.get_language_directory(language)
        print('Getting Links')
        links, total_size = self.get_links(req.text)
        print('Starting download of {} files with a total size of {:.2f}MB'.format(str(len(links)), total_size))
        for link in links:
            self.download_file(link, language)

