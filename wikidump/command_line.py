import argparse
from wikidump.crawler import WikiDumpCrawler
from wikidump.parser import WikiParser


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--import_dump', type=str)
    p.add_argument('-p', '--parse_dump', type=str)
    p.add_argument('-a', '--all', type=str)
    p.add_argument('languages', metavar='N', type=str, nargs='+', help='languages')
    args = p.parse_args()
    if args.import_dump:
        crawler = WikiDumpCrawler()
        crawler.get_language_dumps(args.import_dump)
    if args.parse_dump:
        parser = WikiParser(args.parse_dump)
        parser.create_data_frames()
        parser.unify_data_frames()
    if args.all:
        for language in args.languages:
            print('Starting language {}'.format(language))
            crawler = WikiDumpCrawler()
            crawler.get_language_dumps(language)
            print('Parsing language {}'.format(language))
            parser = WikiParser(language)
            parser.create_data_frames()
            parser.unify_data_frames()