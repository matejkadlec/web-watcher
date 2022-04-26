#!/usr/bin/python3
from database import select_url_results, insert_url_results, select_distinct_configs, select_from_url_queue, \
    delete_from_url_queue, select_erroneous_urls, select_erroneous_configs
from utils.telegram_bot import TelegramBot
from utils.static_fields import initial_config
from utils.utils import get_soup
from urllib.request import Request, urlopen
import urllib.error
from datetime import datetime
import ssl
import json

ssl._create_default_https_context = ssl._create_unverified_context


class URLComparison:
    def __init__(self):
        self.attributes = initial_config
        self.error_url_list = []

    def compare_erroneous_urls(self):
        # Get distinct configs from url queue
        configs_db = select_erroneous_configs()

        for config_db in configs_db:
            # Get config and interval key
            settings_id = config_db[0]
            config = json.loads(config_db[1])
            interval_key = max(config, key=config.get)

            erroneous_urls = select_erroneous_urls(settings_id, interval_key)

            if not erroneous_urls:
                continue

            url_results_list = []

            for url_result in erroneous_urls:
                # Get values from url result
                settings_id = url_result[0]
                url = url_result[1]
                attribute = url_result[2]
                old_value = url_result[3]
                attempt = url_result[4]

                # Get result for current url
                result = self.parse_attribute(url, attribute)
                new_value = self.attributes[attribute]

                # Initialize Telegram bot
                tb = TelegramBot(url_result[5])

                # We get response even when parsing wasn't successful
                if attribute == "response":
                    if new_value == old_value:
                        # Responses match
                        if result == 0:
                            url_results_list.append(
                                tuple((settings_id, url, datetime.now(), attribute, old_value, None, None, 1)))
                        else:
                            if url not in self.error_url_list:
                                tb.send_error_message(url, result, attempt + 1)
                                self.error_url_list.append(url)
                            url_results_list.append(
                                tuple((settings_id, url, datetime.now(), attribute, old_value, None, result, attempt + 1)))
                    else:
                        # Responses don't match
                        difference = get_difference(new_value, old_value)
                        tb.send_url_changed_message(attribute, url, difference)
                        if result == 0:
                            url_results_list.append(
                                tuple((settings_id, url, datetime.now(), attribute, old_value, new_value, None, 1)))
                        else:
                            if url not in self.error_url_list:
                                tb.send_error_message(url, result, attempt + 1)
                                self.error_url_list.append(url)
                            url_results_list.append(
                                tuple((settings_id, url, datetime.now(), attribute, old_value, new_value, result,
                                       attempt + 1)))
                # Parsing wasn't successful
                elif result != 0:
                    # We only send one message for each erroneous url
                    if url not in self.error_url_list:
                        tb.send_error_message(url, result, attempt + 1)
                        self.error_url_list.append(url)
                    url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, None, result,
                                                   attempt + 1)))
                else:
                    url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, None, None, 1)))

                # Reset attributes
                for k in self.attributes:
                    self.attributes[k] = None

            # Empty error url list
            self.error_url_list = []

            # Insert url_results to db
            insert_url_results(url_results_list)

    def compare_url_results(self):
        # Get distinct configs from url queue
        configs_db = select_distinct_configs()

        for config_db in configs_db:
            # Get first url record for each config
            url_queue_record = select_from_url_queue(config_db[0], False)

            if not url_queue_record:
                continue

            # Get settings id and url from the record
            settings_id = url_queue_record[0][0]
            url = url_queue_record[0][1]

            # Get config and interval key
            config = json.loads(config_db[1])
            interval_key = max(config, key=config.get)

            # Get existing url results for current url
            url_results = select_url_results(settings_id, interval_key)
            url_results_list = []

            for url_result in url_results:
                # Get type of url result
                attribute = url_result[2]

                # Get result for current url
                result = self.parse_attribute(url, attribute)

                # Get new value set in parse_attribute function, and old value from url result
                new_value = self.attributes[attribute]
                old_value = str(url_result[3]) if not url_result[4] else str(url_result[4])

                # Initialize Telegram bot
                tb = TelegramBot(url_result[5])

                # Parsing wasn't successful
                if result != 0:
                    # We only send one message for each erroneous url
                    if url not in self.error_url_list:
                        tb.send_error_message(url, result, 1)
                        self.error_url_list.append(url)
                    # We get response even when parsing wasn't successful
                    if attribute == "response":
                        if new_value == old_value:
                            # Responses match
                            url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, None, result, 1)))
                        else:
                            # Responses don't match
                            difference = get_difference(new_value, old_value)
                            tb.send_url_changed_message(attribute, url, difference)
                            url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, new_value, result, 1)))
                    else:
                        url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, None, result, 1)))
                elif new_value == old_value:
                    # Values match
                    url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, None, None, 1)))
                else:
                    # Values don't match
                    difference = get_difference(new_value, old_value)
                    tb.send_url_changed_message(attribute, url, difference)
                    url_results_list.append(tuple((settings_id, url, datetime.now(), attribute, old_value, new_value, None, 1)))

                # Reset attributes
                for k in self.attributes:
                    self.attributes[k] = None

                delete_from_url_queue(settings_id, False)

            # Empty error url list
            self.error_url_list = []

            # Insert url_results to db
            insert_url_results(url_results_list)

    def parse_attribute(self, url, attribute):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        # Parse response
        if attribute == 'response':
            try:
                self.attributes["response"] = str(urlopen(req).code)
                return 0
            except urllib.error.HTTPError as e:
                self.attributes["response"] = str(e.code)
                return str(e)

        # Get BeautifulSoup or error text when html parsing isn't successful
        soup = get_soup(req)
        if type(soup) == str:
            return soup

        # Parse title
        if attribute == 'title':
            self.attributes["title"] = soup.find("title").string
            return 0

        # Parse description, robots, og:image
        for tag in soup.find_all("meta"):
            if tag.get("name") == "description" and attribute == 'description':
                self.attributes["description"] = tag.get("content")
                return 0
            elif tag.get("name") == "robots" and attribute == 'robots':
                self.attributes["robots"] = tag.get("content")
                return 0
            elif tag.get("property") == "og:image" and attribute == 'image':
                self.attributes["image"] = tag.get("content")
                return 0

        # Parse content
        if attribute == 'content':
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
            return 0

        return 0


def get_difference(new_value, old_value):
    old = ""
    new = ""
    # get first 200 or value length characters since first difference
    for i in range(min(len(new_value), len(old_value))):
        if new_value[i] != old_value[i]:
            for j in range(0, min(200, len(new_value) - i, len(old_value) - i)):
                old += old_value[i + j]
                new += new_value[i + j]
            break
    # save difference to string
    return f"OLD VALUE: \n-> {old}\n\nNEW VALUE: \n-> {new}"


uc = URLComparison()
uc.compare_url_results()
uc.compare_erroneous_urls()
