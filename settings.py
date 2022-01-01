from database import insert_settings_db, insert_result_db
import sys
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def insert_settings():
    settings_type = int(sys.argv[1])
    url = sys.argv[2]
    is_sitemap = int(sys.argv[3])
    interval = int(sys.argv[4])

    settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)
    if is_sitemap:
        sitemap_urls = get_urls(url)
        for sitemap_url in sitemap_urls:
            add_to_queue(sitemap_url)
    else:
        content = get_content(url)
        insert_result_db(settings_id, content, 1)


def get_urls(url):
    pass


def add_to_queue():
    pass


def get_content(url):
    req = Request(url)
    try:
        req = Request(url)
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    except gzip.BadGzipFile:
        html = urlopen(req).read().decode('utf-8')
    soup = BeautifulSoup(html, features="html.parser")
    return soup.get_text()


insert_settings()
