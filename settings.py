from database import select_from_settings, insert_settings, insert_many_settings, insert_config, \
    insert_many_sitemap_results
from urls.url_queue_adding import add_urls_to_queue
from utils.static_fields import initial_config
from utils.utils import get_urls
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
        self.sitemap_results = []

    def init_settings(self):
        # Get settings parameters
        url = sys.argv[1]
        chat_id = sys.argv[2]

        for i in range(0, len(self.config)):
            self.config[list(self.config)[i]] = sys.argv[i + 3]

        # Check if settings already exists
        for settings_db in self.settings_list_db:
            if settings_db[1] == url:
                return

        # Insert config to db and get its id
        self.config_id = insert_config(json.dumps(self.config), chat_id)

        is_sitemap = 1 if url.endswith(".xml") else 0
        if is_sitemap:
            # Get initial settings id
            initial_id = insert_settings(url, is_sitemap, self.config_id)
            # If current url is sitemap, get urls on that sitemap
            self.parse_sitemap(url, initial_id)
        else:
            # If it's not, append settings for given url
            self.append_settings(url, None, None)

        # Insert remaining settings and sitemap results to db
        if self.settings_list:
            insert_many_settings(self.settings_list)
        if self.sitemap_results:
            insert_many_sitemap_results(self.sitemap_results)

        # Immediately add new urls to processing queue
        add_urls_to_queue(is_new=True)

    def append_settings(self, url, base_url, base_url_settings_id):
        # Check if settings already exist
        for settings_db in self.settings_list_db:
            if settings_db[1] == url:
                return

        # Insert config to db and get its id
        if self.config_id == 0:
            self.config_id = insert_config(json.dumps(self.config))

        # Decide whether current url is sitemap or not
        is_sitemap = 1 if url.endswith(".xml") else 0

        if self.settings_id == 0:
            # Insert settings to db and get its id
            self.settings_id = insert_settings(url, is_sitemap, self.config_id)
        else:
            # Append settings to settings_list for it to be inserted to db later
            self.settings_list.append(tuple((url, is_sitemap, 1, self.config_id)))
            # If settings_list has 1k or more items, insert them to db and clear the list
            if len(self.settings_list) >= 100000:
                insert_many_settings(self.settings_list)
                self.settings_list = []
            # Increase global settings id
            self.settings_id += 1

        # Append current url with its base url
        if base_url:
            self.sitemap_results.append(tuple((base_url_settings_id, base_url, url, datetime.now(), 0, 0)))
            if len(self.sitemap_results) >= 100000:
                insert_many_sitemap_results(self.sitemap_results)
                self.sitemap_results = []

        # If current url is sitemap, get urls on that sitemap
        if is_sitemap:
            self.parse_sitemap(url, self.settings_id)

    def parse_sitemap(self, base_url, base_url_settings_id):
        # Get urls
        sitemap_urls, urls = get_urls(base_url)

        # Append settings for all sitemaps and urls
        for sitemap_url in sitemap_urls:
            self.append_settings(sitemap_url, base_url, base_url_settings_id)
        for url in urls:
            self.append_settings(url, base_url, base_url_settings_id)


st = Settings()
st.init_settings()
