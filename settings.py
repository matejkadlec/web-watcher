import urllib.error
from database import select_from_settings, insert_settings_db, insert_many_settings_db, insert_many_queues_db, \
    insert_config_db
import sys
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import json

config = {"response": None, "title": None, "description": None, "robots": None, "image": None, "content": None}
config_id = 0
settings_id = 0
settings_list = []
settings_list_db = select_from_settings()
queue_list = []


def init_settings():
    global config_id
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list

    # get settings parameters
    url = sys.argv[1]
    is_sitemap = 1 if url.endswith(".xml") else 0
    for i in range(0, len(config)):
        config[list(config)[i]] = sys.argv[i + 2]

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(url)
    else:
        # if it's not, append settings for given url
        append_settings(url)

    # insert remaining settings and urls to db
    if settings_list:
        insert_many_settings_db(settings_list)
    if queue_list:
        insert_many_queues_db(queue_list)


def append_settings(url):
    global config_id
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list

    # check if settings already exist
    for settings_db in settings_list_db:
        if settings_db[1] == url:
            return

    # insert config to db and get its id
    if config_id == 0:
        config_id = insert_config_db(json.dumps(config))

    # decide whether current url is sitemap or not
    is_sitemap = 1 if url.endswith(".xml") else 0

    if settings_id == 0:
        # insert settings to db and get its id
        settings_id = insert_settings_db(url, is_sitemap, config_id)
    else:
        # append settings to settings_list for it to be inserted to db later
        settings_list.append(tuple((url, is_sitemap, 1, config_id)))
        # if settings_list has 10k or more items, insert them to db and clear the list
        if len(settings_list) >= 1000:
            insert_many_settings_db(settings_list)
            settings_list = []
        # increase global settings id
        settings_id += 1

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(url)
    else:
        # if not, add it to queue_list for it to be inserted to db later
        queue_list.append(tuple((settings_id, url)))
        # if queue_list has 10k or more items, insert them to db and clear the list
        if len(queue_list) >= 1000:
            # also insert settings currently saved in settings_list to db,
            # so we don't get foreign key error, and clear it
            insert_many_settings_db(settings_list)
            settings_list = []
            insert_many_queues_db(queue_list)
            queue_list = []


def parse_sitemap(base_url):
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
        append_settings(sitemap_url)
    for url in urls:
        append_settings(url)


init_settings()
