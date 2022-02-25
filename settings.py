import urllib.error
from database import select_from_settings, insert_settings, insert_many_settings, insert_many_queues, \
    insert_config, insert_many_sitemap_results
from queue_processing import process_queue
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import json

config = {"response": None, "title": None, "description": None, "robots": None, "image": None, "content": None}
config_id = 0
settings_id = 0
settings_list = []
settings_list_db = select_from_settings()
queue_list = []
sitemap_results = []


def init_settings(url, config_parameters):
    global config_id
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list

    # get settings parameters
    is_sitemap = 1 if url.endswith(".xml") else 0
    for i in range(0, len(config)):
        config[list(config)[i]] = config_parameters[i]

    # check if settings already exists
    for settings_db in settings_list_db:
        if settings_db[1] == url:
            return

    # insert config to db and get its id
    if config_id == 0:
        config_id = insert_config(json.dumps(config))

    # get initial settings_id
    settings_id = insert_settings(url, is_sitemap, config_id)

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(url, settings_id)
    else:
        # if it's not, append settings for given url
        append_settings(url, None, None)

    # insert remaining settings and urls to db
    if settings_list:
        insert_many_settings(settings_list)
    if queue_list:
        insert_many_queues(queue_list)
    if sitemap_results:
        insert_many_sitemap_results(sitemap_results)

    process_queue()


def append_settings(url, base_url, base_url_settings_id):
    global config_id
    global settings_id
    global settings_list
    global settings_list_db
    global queue_list
    global sitemap_results

    # check if settings already exist
    for settings_db in settings_list_db:
        if settings_db[1] == url:
            return

    # insert config to db and get its id
    if config_id == 0:
        config_id = insert_config(json.dumps(config))

    # decide whether current url is sitemap or not
    is_sitemap = 1 if url.endswith(".xml") else 0

    if settings_id == 0:
        # insert settings to db and get its id
        settings_id = insert_settings(url, is_sitemap, config_id)
    else:
        # append settings to settings_list for it to be inserted to db later
        settings_list.append(tuple((url, is_sitemap, 1, config_id)))
        # if settings_list has 10k or more items, insert them to db and clear the list
        if len(settings_list) >= 1000:
            insert_many_settings(settings_list)
            settings_list = []
        # increase global settings id
        settings_id += 1

    # append current url with it's base url
    if base_url:
        sitemap_results.append(tuple((base_url_settings_id, base_url, url, datetime.now(), 0, 0)))
        if len(sitemap_results) >= 1000:
            insert_many_sitemap_results(sitemap_results)
            sitemap_results = []

    if is_sitemap:
        # if current url is sitemap, get urls on that sitemap
        parse_sitemap(url, settings_id)
    else:
        # if not, add it to queue_list for it to be inserted to db later
        queue_list.append(tuple((settings_id, url)))
        # if queue_list has 10k or more items, insert them to db and clear the list
        if len(queue_list) >= 1000:
            # also insert settings currently saved in settings_list to db,
            # so we don't get foreign key error, and clear it
            insert_many_settings(settings_list)
            settings_list = []
            insert_many_queues(queue_list)
            queue_list = []


def parse_sitemap(base_url, base_url_settings_id):
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
        append_settings(sitemap_url, base_url, base_url_settings_id)
    for url in urls:
        append_settings(url, base_url, base_url_settings_id)
