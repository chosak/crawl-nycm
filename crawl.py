import argparse
import csv
import logging
import requests

from BeautifulSoup import BeautifulSoup



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class Crawler(object):
    SITE_ROOT = (
        'http://web2.nyrrc.org/cgi-bin/htmlos.cgi/'
        'mar-programs/archive/archive_search.html'
    )


    def __init__(self):
        self.session = requests.Session()


    def crawl(self):
        states, countries = self.crawl_root()


    def crawl_root(self):
        root = self.curl(self.SITE_ROOT)
        soup = BeautifulSoup(root)

        state_select = soup.find('select', {'name': 'input.state'})
        state_options = state_select.findAll('option')[1:]
        states = [
            (c.get('value').strip(), c.text) for c in state_options
        ]

        country_select = soup.find('select', {'name': 'input.country'})
        country_options = country_select.findAll('option')[1:]
        countries = [
            tuple(c.get('value').strip().split(',')) for c in country_options
        ]

        return states, countries


    def curl(self, url, method='GET', data=None):
        headers = {}

        method = method.lower()
        if 'post' == method:
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        fn = getattr(self.session, method)
        response = fn(url, data=data, headers=headers)

        if 200 != response.status_code:
            raise RuntimeError((
                response.status_code, response.text
            ))

        return response.text


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
