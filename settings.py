import urllib.error
from database import select_from_settings, insert_settings_db, insert_many_settings_db, insert_result_db, \
    insert_many_queues_db
from queue_processing import ERROR_CODE, get_content
import sys
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from datetime import datetime


settings_id = 0
settings_list = []
settings_list_db = select_from_settings()
queue_list = []


def init_settings():
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list

    # get settings parameters
    settings_type = int(sys.argv[1])
    url = sys.argv[2]
    is_sitemap = 1 if url.endswith(".xml") else 0
    interval = int(sys.argv[3])

    # check if settings already is in db
    insert_initial = True
    for settings_db in settings_list_db:
        if settings_db[2] == url:
            insert_initial = False

    # insert initial settings to db and get its id
    if insert_initial:
        settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(settings_type, url, interval)
    elif insert_initial:
        # if not, get the content and save it to db
        response, title, description, robots, image, content = get_content(url)

        if ERROR_CODE in content:
            # if url wasn't parsed successfully
            exception = content.split('.', 1)[1]
            insert_result_db(settings_id, url, datetime.now(), response, None, None, None, None, None, 0, None, 0, exception)
        else:
            insert_result_db(settings_id, url, datetime.now(), response, title, description, robots, image, content, 0, None, 1, None)

    # insert remaining settings and urls to db
    if settings_list:
        insert_many_settings_db(settings_list)
    if queue_list:
        insert_many_queues_db(queue_list)


def append_settings(settings_type, url, interval):
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list

    for settings_db in settings_list_db:
        if settings_db[2] == url:
            return

    # decide whether current url is sitemap or not
    is_sitemap = 1 if url.endswith(".xml") else 0

    if settings_id == 0:
        # if no settings was inserted yet
        settings_id = insert_settings_db(settings_type, url, is_sitemap, interval)
    else:
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


def parse_sitemap(settings_type, base_url, interval):
    # get sitemap as xml
    req = Request(base_url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        xml = urlopen(req).read()
    except urllib.error.HTTPError:
        return
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


init_settings()
