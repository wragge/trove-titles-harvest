import requests
import re
import time
from utilities import retry
from requests.exceptions import HTTPError
from pymongo import MongoClient


from credentials import TROVE_API_KEY, MONGO_URL


@retry(HTTPError, tries=10, delay=1)
def get_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def count_articles(link):
    obj_id = re.search(r'(nla\.obj-\d+)', link).group(1)
    url = 'http://api.trove.nla.gov.au/result?q="{}"&zone=article&encoding=json&l-format=Article&n=1&sortby=dateasc&key={}'.format(obj_id, TROVE_API_KEY)
    data = get_data(url)
    total = int(data['response']['zone'][0]['records']['total'])
    if total > 0:
        try:
            start = data['response']['zone'][0]['records']['work'][0]['issued']
        except KeyError:
            start = None
        url = 'http://api.trove.nla.gov.au/result?q="{}"&zone=article&encoding=json&l-format=Article&n=1&sortby=datedesc&key={}'.format(obj_id, TROVE_API_KEY)
        data = get_data(url)
        try:
            end = data['response']['zone'][0]['records']['work'][0]['issued']
        except KeyError:
            end = None
        return {'total': total, 'start': start, 'end': end}
    else:
        return None


def get_titles():
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    count = 100
    n = count
    s = 0
    while n == count:
        url = 'http://api.trove.nla.gov.au/result?q="nla.obj-"&zone=article&encoding=json&l-format=Periodical&include=links&n={}&s={}&key={}'.format(n, s, TROVE_API_KEY)
        print url
        data = get_data(url)
        n = int(data['response']['zone'][0]['records']['n'])
        s = int(data['response']['zone'][0]['records']['s']) + count
        for work in data['response']['zone'][0]['records']['work']:
            thumbnail_url = None
            browse_url = None
            articles = None
            try:
                links = work['identifier']
            except KeyError:
                links = []
            for link in links:
                if 'linktext' in link and 'National Library of Australia digitised item' in link['linktext'] and 'nla.obj-' in link['value']:
                    articles = count_articles(link['value'])
                    browse_url = link['value']
                elif link['linktype'] == 'thumbnail' and 'nla.obj-' in link['value']:
                    thumbnail_url = link['value']
            if articles:
                try:
                    contributor = work['contributor'][0]
                except IndexError:
                    contributor = work['contributor']
                except KeyError:
                    contributor = None
                print '{} ({} - {}): {}'.format(work['title'].encode('utf-8'), articles['start'], articles['end'], articles['total'])
                title = {'title': work['title'], 'contributor': contributor, 'browse_url': browse_url, 'trove_url': work['troveUrl'], 'articles': articles, 'thumbnail_url': thumbnail_url}
                db.titles.replace_one({'browse_url': browse_url}, title, upsert=True)
            time.sleep(0.5)


