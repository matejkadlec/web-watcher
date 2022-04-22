#!/usr/bin/python3
from database import select_from_queue, delete_from_queue, insert_many_url_results
from telegram_bot import send_error_message
from static_fields import initial_config
from utils import get_soup
from urllib.request import Request, urlopen
import urllib.error
from bs4 import BeautifulSoup
from datetime import datetime
import ssl
import json

ssl._create_default_https_context = ssl._create_unverified_context


class Queue:
    def __init__(self):
        self.attributes = initial_config

    def process_queue(self):
        # while there are any records in queue table
        while select_from_queue():
            url_results_list = []

            # select records from queue
            records = select_from_queue()

            for record in records:
                config = json.loads(record[2])
                result = self.parse_url(record[1], config)
                settings_id = record[0]
                url = record[1]

                # append result to the url_results_list for it to be inserted to db later
                if result != 0:
                    send_error_message(url, result)
                    for key in config:
                        if config[key] != '0':
                            if key == "response":
                                url_results_list.append(tuple((settings_id, url, datetime.now(), key,
                                                               self.attributes[key], None, 0, result)))
                            else:
                                url_results_list.append(tuple((settings_id, url, datetime.now(), key, None, None, 0,
                                                               result)))
                else:
                    for key in config:
                        if config[key] != '0':
                            url_results_list.append(tuple((settings_id, url, datetime.now(), key, self.attributes[key],
                                                           None, 1, None)))
                # reset attributes
                for k in self.attributes:
                    self.attributes[k] = None

            # after loop is finished, insert results to db and clear queue
            insert_many_url_results(url_results_list)
            delete_from_queue()

    def parse_url(self, url, config):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        # response
        if config["response"] != '0':
            try:
                self.attributes["response"] = urlopen(req).code
            except urllib.error.HTTPError as e:
                self.attributes["response"] = e.code
                return str(e)

        soup = get_soup(req)

        # title
        if config["title"] != '0':
            self.attributes["title"] = soup.find("title").string

        # description, robots, og:image
        for tag in soup.find_all("meta"):
            if tag.get("name") == "description" and config["description"] != '0':
                self.attributes["description"] = tag.get("content")
            elif tag.get("name") == "robots" and config["robots"] != '0':
                self.attributes["robots"] = tag.get("content")
            elif tag.get("property") == "og:image" and config["image"] != '0':
                self.attributes["image"] = tag.get("content")

        # content
        if config["content"] != '0':
            hidden_tags = soup.select('.hidden')
            self.attributes["content"] = soup.get_text()
            for hidden_tag in hidden_tags:
                if hidden_tag.string:
                    self.attributes["content"] = self.attributes["content"].replace(hidden_tag.string, "")
            self.attributes["content"] = ' '.join(self.attributes["content"].split())

        return 0


qu = Queue()
qu.process_queue()
