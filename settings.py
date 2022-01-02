from database import insert_settings_db, insert_many_settings_db, insert_result_db, insert_many_queues_db
import sys
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


settings_id = 0
settings_list = []
queue_list = []


def init_settings():
    global settings_id
    global settings_list
    global queue_list

    # get settings parameters
    settings_type = int(sys.argv[1])
    url = sys.argv[2]
    is_sitemap = 1 if url.endswith(".xml") else 0
    interval = int(sys.argv[3])

    # insert initial settings to db and get its id
    settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)
    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(settings_type, url, interval)
    else:
        # if not, get the content and save it to db
        content = get_content(url)
        insert_result_db(settings_id, content, 1)

    # insert remaining settings and urls to db
    if settings_list:
        insert_many_settings_db(settings_list)
    if queue_list:
        insert_many_queues_db(queue_list)


def append_settings(settings_type, url, interval):
    global settings_id
    global settings_list
    global queue_list

    # decide whether current url is sitemap or not
    is_sitemap = 1 if url.endswith(".xml") else 0

    # append settings to settings_list for it to be inserted to db later
    settings_list.append(tuple((settings_type, url, is_sitemap, interval)))
    # if settings_list has 10k or more items, insert them to db and clear the list
    if len(settings_list) >= 10000:
        insert_many_settings_db(settings_list)
        settings_list = []
    # increase global settings id
    settings_id += 1

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(settings_type, url, interval)
    else:
        # if not, add it to queue_list for it to be inserted to db later
        queue_list.append(tuple((settings_id, url)))
        # if queue_list has 10k or more items, insert them to db and clear the list
        if len(queue_list) >= 10000:
            # also insert settings currently saved in settings_list to db,
            # so we don't get foreign key error, and clear it
            insert_many_settings_db(settings_list)
            settings_list = []
            insert_many_queues_db(queue_list)
            queue_list = []


def parse_sitemap(settings_type, url, interval):
    # get sitemap as xml
    req = Request(url)
    xml = urlopen(req).read()
    soup = BeautifulSoup(xml, features="html.parser")

    # initialize lists for sitemaps and urls for current sitemap
    sitemap_urls = []
    urls = []

    # find all sitemaps and urls
    sitemap_tags = soup.find_all("sitemap")
    url_tags = soup.find_all("url")

    # append links to lists
    for sitemap_tag in sitemap_tags:
        sitemap_urls.append(sitemap_tag.findNext("loc").text)
    for url_tag in url_tags:
        urls.append(url_tag.findNext("loc").text)

    # append settings for all sitemaps and urls
    for sitemap_url in sitemap_urls:
        append_settings(settings_type, sitemap_url, interval)
    for url in urls:
        append_settings(settings_type, url, interval)


def get_content(url):
    req = Request(url)
    # some urls have their content zipped
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    except gzip.BadGzipFile:
        html = urlopen(req).read().decode('utf-8')
    # make BeautifulSoup from html
    soup = BeautifulSoup(html, features="html.parser")
    # return plain text
    return soup.get_text()


init_settings()
