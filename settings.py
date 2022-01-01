from database import insert_settings_db, insert_result_db, insert_to_queue, select_from_queue
import sys
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def insert_initial_settings():
    settings_type = int(sys.argv[1])
    url = sys.argv[2]
    is_sitemap = 1 if url.endswith(".xml") else 0
    interval = int(sys.argv[3])

    settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)
    if is_sitemap:
        get_urls(settings_type, url, interval)
    else:
        content = get_content(url)
        insert_result_db(settings_id, content, 1)


def insert_settings(settings_type, url, interval):
    is_sitemap = 1 if url.endswith(".xml") else 0
    settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)
    if is_sitemap:
        get_urls(settings_type, url, interval)
    else:
        insert_to_queue(settings_id, url)


def get_urls(settings_type, url, interval):
    req = Request(url)
    xml = urlopen(req).read()
    soup = BeautifulSoup(xml, features="html.parser")

    sitemap_urls = []
    urls = []

    sitemap_tags = soup.find_all("sitemap")
    url_tags = soup.find_all("url")

    for sitemap_tag in sitemap_tags:
        sitemap_urls.append(sitemap_tag.findNext("loc").text)
    for url_tag in url_tags:
        urls.append(url_tag.findNext("loc").text)

    for sitemap_url in sitemap_urls:
        insert_settings(settings_type, sitemap_url, interval)
    for url in urls:
        insert_settings(settings_type, url, interval)


def get_content(url):
    req = Request(url)
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    except gzip.BadGzipFile:
        html = urlopen(req).read().decode('utf-8')
    soup = BeautifulSoup(html, features="html.parser")
    return soup.get_text()


# insert_initial_settings()
