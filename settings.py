from database import select_from_settings, insert_settings, insert_many_settings, insert_many_queues, \
    insert_config, insert_many_sitemap_results
from queue_processing import Queue
from static_fields import initial_config
from utils import get_urls
import sys
from datetime import datetime
import json


class Settings:
    def __init__(self):
        self.config = initial_config
        self.config_id = 0
        self.settings_id = 0
        self.settings_list = []
        self.settings_list_db = select_from_settings()
        self.queue_list = []
        self.sitemap_results = []

    def init_settings(self):
        # get settings parameters
        url = sys.argv[1]

        for i in range(0, len(self.config)):
            self.config[list(self.config)[i]] = sys.argv[i + 2]

        # check if settings already exists
        for settings_db in self.settings_list_db:
            if settings_db[1] == url:
                return

        # insert config to db and get its id
        self.config_id = insert_config(json.dumps(self.config))

        is_sitemap = 1 if url.endswith(".xml") else 0
        if is_sitemap:
            # get initial settings id
            initial_id = insert_settings(url, is_sitemap, self.config_id)
            # if current url is sitemap, get urls on that sitemap
            self.parse_sitemap(url, initial_id)
        else:
            # if it's not, append settings for given url
            self.append_settings(url, None, None)

        # insert remaining settings, queues and sitemap results to db
        if self.settings_list:
            insert_many_settings(self.settings_list)
        if self.queue_list:
            insert_many_queues(self.queue_list)
        if self.sitemap_results:
            insert_many_sitemap_results(self.sitemap_results)

        # immediately start processing queue
        qu = Queue()
        qu.process_queue()

    def append_settings(self, url, base_url, base_url_settings_id):
        # check if settings already exist
        for settings_db in self.settings_list_db:
            if settings_db[1] == url:
                return

        # insert config to db and get its id
        if self.config_id == 0:
            self.config_id = insert_config(json.dumps(self.config))

        # decide whether current url is sitemap or not
        is_sitemap = 1 if url.endswith(".xml") else 0

        if self.settings_id == 0:
            # insert settings to db and get its id
            self.settings_id = insert_settings(url, is_sitemap, self.config_id)
        else:
            # append settings to settings_list for it to be inserted to db later
            self.settings_list.append(tuple((url, is_sitemap, 1, self.config_id)))
            # if settings_list has 1k or more items, insert them to db and clear the list
            if len(self.settings_list) >= 1000:
                insert_many_settings(self.settings_list)
                self.settings_list = []
            # increase global settings id
            self.settings_id += 1

        # append current url with its base url
        if base_url:
            self.sitemap_results.append(tuple((base_url_settings_id, base_url, url, datetime.now(), 0, 0)))
            if len(self.sitemap_results) >= 1000:
                insert_many_sitemap_results(self.sitemap_results)
                self.sitemap_results = []

        if is_sitemap:
            # if current url is sitemap, get urls on that sitemap
            self.parse_sitemap(url, self.settings_id)
        else:
            # if not, add it to queue_list for it to be inserted to db later
            self.queue_list.append(tuple((self.settings_id, url)))
            # if queue_list has 1k or more items, insert them to db and clear the list
            if len(self.queue_list) >= 1000:
                # also insert settings currently saved in settings_list to db,
                # so we don't get foreign key error, and clear it
                insert_many_settings(self.settings_list)
                self.settings_list = []
                insert_many_queues(self.queue_list)
                self.queue_list = []

    def parse_sitemap(self, base_url, base_url_settings_id):
        # get urls
        sitemap_urls, urls = get_urls(base_url)

        # append settings for all sitemaps and urls
        for sitemap_url in sitemap_urls:
            self.append_settings(sitemap_url, base_url, base_url_settings_id)
        for url in urls:
            self.append_settings(url, base_url, base_url_settings_id)


st = Settings()
st.init_settings()
