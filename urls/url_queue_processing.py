#!/usr/bin/python3
from database import select_distinct_configs, select_from_url_queue, delete_from_url_queue, insert_url_result
from utils.telegram_bot import TelegramBot
from utils.static_fields import initial_config
from utils.utils import get_soup
from urllib.request import Request, urlopen
import urllib.error
from datetime import datetime
import ssl
import json

ssl._create_default_https_context = ssl._create_unverified_context


class URLQueue:
    def __init__(self):
        self.attributes = initial_config

    def process_url_queue(self):
        configs_db = select_distinct_configs()

        for config_db in configs_db:
            config = json.loads(config_db[1])
            url_queue_record = select_from_url_queue(config_db[0], True)

            if not url_queue_record:
                continue

            settings_id = url_queue_record[0][0]
            url = url_queue_record[0][1]

            result, response_valid = self.parse_url(url, config)

            # Append result to the url_results_list for it to be inserted to db later
            if result != 0:
                tb = TelegramBot(config_db[2])
                tb.send_error_message(url, result, 1)
                for key in config:
                    if config[key] != '0':
                        if key == "response":
                            if response_valid:
                                insert_url_result(settings_id, url, datetime.now(), key, self.attributes[key], None,
                                                  None, 1)
                            else:
                                insert_url_result(settings_id, url, datetime.now(), key, self.attributes[key], None,
                                                  result, 1)
                        else:
                            insert_url_result(settings_id, url, datetime.now(), key, None, None, result, 1)
            else:
                for key in config:
                    if config[key] != '0':
                        insert_url_result(settings_id, url, datetime.now(), key, self.attributes[key], None, None, 1)

            # Reset attributes
            for k in self.attributes:
                self.attributes[k] = None

            delete_from_url_queue(settings_id, True)

    def parse_url(self, url, config):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        # Parse response
        if config["response"] != '0':
            try:
                self.attributes["response"] = urlopen(req).code
            except urllib.error.HTTPError as e:
                self.attributes["response"] = e.code
                return str(e), False

        # Get BeautifulSoup or error text when html parsing isn't successful
        soup = get_soup(req)
        if type(soup) == str:
            return soup, True

        # Parse title
        if config["title"] != '0':
            self.attributes["title"] = soup.find("title").string

        # Parse description, robots, og:image
        for tag in soup.find_all("meta"):
            if tag.get("name") == "description" and config["description"] != '0':
                self.attributes["description"] = tag.get("content")
            elif tag.get("name") == "robots" and config["robots"] != '0':
                self.attributes["robots"] = tag.get("content")
            elif tag.get("property") == "og:image" and config["image"] != '0':
                self.attributes["image"] = tag.get("content")

        # Parse content
        if config["content"] != '0':
            # Remove views, comments and likes count
            if soup.find(class_='entry-blog-adds'):
                soup.find(class_='entry-blog-adds').string = ""

            self.attributes["content"] = soup.get_text()

            # Remove hidden elements
            hidden_tags = soup.select('.hidden')
            for hidden_tag in hidden_tags:
                if hidden_tag.string:
                    self.attributes["content"] = self.attributes["content"].replace(hidden_tag.string, "")

            self.attributes["content"] = ' '.join(self.attributes["content"].split())

        return 0, True


uq = URLQueue()
uq.process_url_queue()
