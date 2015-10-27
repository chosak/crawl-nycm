import argparse
import csv
import itertools
import logging
import requests
import socket
import time

from BeautifulSoup import BeautifulSoup
from datetime import datetime, timedelta
from memcache import Client



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class Crawler(object):
    YEAR = 2014
    SITE_ROOT = (
        'http://web2.nyrrc.org/cgi-bin/htmlos.cgi/'
        'mar-programs/archive/archive_search.html'
    )
    REQUEST_SPACING = timedelta(seconds=1)


    def __init__(self):
        self.session = requests.Session()
        self.last_url = None
        self.post_url = None
        self.last_request = None
        self.cache = self.get_cache()


    def crawl(self):
        self.crawl_root()
        return filter(None, self.generate_results())


    def generate_results(self):
        for abbrev, name in self.states[:1]:
            for result in self.crawl_state(abbrev, name):
                yield result

        for abbrev, name in self.countries[:1]:
            for result in self.crawl_country(abbrev, name):
                yield result


    def crawl_root(self):
        root = self.curl(self.SITE_ROOT)
        soup = BeautifulSoup(root)
        self.refresh_post_url(soup)

        state_select = soup.find('select', {'name': 'input.state'})
        state_options = state_select.findAll('option')[1:]
        self.states = [
            (c.get('value').strip(), c.text) for c in state_options
        ]

        country_select = soup.find('select', {'name': 'input.country'})
        country_options = country_select.findAll('option')[1:]
        self.countries = [
            tuple(c.get('value').strip().split(',')) for c in country_options
        ]


    def refresh_post_url(self, soup):
        forms = soup.findAll('form')
        
        if 1 != len(forms):
            raise RuntimeError('multiple forms')

        self.post_url = forms[0].get('action')
        logger.info('new post URL: {}'.format(self.post_url))


    def crawl_state(self, abbrev, name):
        logger.info('crawling {} ({})'.format(name, abbrev)) 
        return self.crawl_type('search.state', input_state=abbrev)


    def crawl_country(self, abbrev, name):
        logger.info('crawling {} ({})'.format(name, abbrev))
        return self.crawl_type('search.country', input_country=abbrev)


    def crawl_type(self, search_type, **kwargs):
        response = self.post_or_cache(search_type, **kwargs)
        return self.parse_crawl(response)


    def post_or_cache(self, search_type, **kwargs):
        cache_key = self.cache_key(search_type, **kwargs)

        if self.cache:
            response = self.cache.get(cache_key)

            if response is not None:
                return response

        response = self.post(search_type, **kwargs)

        if self.cache:
            self.cache.set(cache_key, response, time=0)

        return response


    def cache_key(self, *args, **kwargs):
        return '-'.join(itertools.chain(
            args,
            *((k, v) for k, v in kwargs.iteritems())
        ))


    def parse_crawl(self, response):
        soup = BeautifulSoup(response)
        self.refresh_post_url(soup)

        table = soup.find('table', {'width': 750})
        rows = table.findAll('tr', {'bgcolor': '#FFFFFF'})
        
        for row in rows:
            yield self.parse_row(row)


    def parse_row(self, row):
        keys = (
            'first_name', 'last_name', 'sex_age', 'bib', 'team', 'country',
            'country_abbrev', 'place', 'place_gender', 'place_age', 'gun_time',
            'net_time', '5km', '10km', '15km', '20km', '13.1mi', '25km',
            '30km', '35km', '40km', 'minutes_per_mile', 'age_graded_time',
            'age_graded_pct',
        )

        values = [self.no_unicode(td.text) for td in row.findAll('td')]
        return {k: v for k, v in zip(keys, values[:-1])}


    @staticmethod
    def no_unicode(x):
        return x.encode('utf-8') if isinstance(x, unicode) else x


    def curl(self, url, method='GET', referer=None, data=None):
        if self.last_request is not None:
            while datetime.now() - self.last_request < self.REQUEST_SPACING:
                time.sleep(1)

        self.last_request = datetime.now()

        headers = {
            'Origin': 'http://web2.nyrrc.org',
            'Referer': self.last_url,
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 '
                'Safari/537.36'
            ),
        }

        fn = getattr(self.session, method.lower())
        response = fn(url, data=data, headers=headers)

        if 200 != response.status_code:
            raise RuntimeError((
                response.status_code, response.text
            ))

        self.last_url = response.url
        logger.info('new last URL: {}'.format(self.last_url))

        return response.text


    def post(self, search_method, input_state=None, input_country=None):
        return self.curl(self.post_url, method='POST', data={
            'AESTIVACVNLIST': ','.join([
                'input.searchyear', 'input.top', 'input.agegroup', 'team_code',
                'input.state', 'input.country', 'input.top.wc',
            ]),
            'input.country': input_country,
            'input.searchyear': self.YEAR,
            'input.state': input_state,
            'input.top': 10,
            'input.top.wc': 10,
            'search.method': search_method,
            'top.type': 'B',
            'top.wc.type': 'P',
            'top.wc.gender': 'B',
        })


    def get_cache(self):
        try:
            socket.create_connection(('localhost', 11211))
            logger.info('using local memcached')

            return Client(['localhost:11211'])
        except socket.error:
            logger.info('no local memcached')
            return None


if '__main__' == __name__:
    parser = argparse.ArgumentParser(
        description='Crawl 2014 NYC Marathon results'
    )

    default_fn = 'crawl.csv'
    parser.add_argument('--filename', default=default_fn,
                        help='output filename (default {})'.format(default_fn))

    args = parser.parse_args()

    results = Crawler().crawl()

    if results:
        logger.info('writing {} results to {}'.format(
            len(results),
            args.filename
        ))

        with open(args.filename, 'wb') as f:
            writer = csv.DictWriter(f, results[0].keys())
            writer.writeheader()
            map(writer.writerow, results)
