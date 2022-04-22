#!/usr/bin/python3
from database import select_url_results, insert_many_url_results
from telegram_bot import send_error_message, send_url_changed_message
from static_fields import initial_config
from utils import get_soup
from urllib.request import Request, urlopen
import urllib.error
from datetime import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class URLComparison:
    def __init__(self):
        self.attributes = initial_config
        self.error_url_list = []

    def compare_url_results(self):
        results = []
        for key in self.attributes:
            results.append(select_url_results(key))

        index = 0
        for key in self.attributes:
            url_results_list = []
            # get all url_results from db for given key
            url_results = results[index]
            index += 1
            if not url_results:
                continue

            for url_result in url_results:
                # parse current page
                result = self.parse_attribute(url_result[1], url_result[2])
                new_value = self.attributes[key]
                old_value = str(url_result[3]) if not url_result[4] else str(url_result[4])
                settings_id = url_result[0]
                url = url_result[1]

                # parsing wasn't successful
                if result != 0:
                    if url not in self.error_url_list:
                        send_error_message(url, result)
                        self.error_url_list.append(url)
                    # we got response even when parsing wasn't successful
                    if key == "response":
                        if new_value == old_value:
                            # responses match
                            url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, None, 0,
                                                           result)))
                        else:
                            # responses don't match
                            difference = get_difference(new_value, old_value)
                            send_url_changed_message(key, url, difference)
                            url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, new_value,
                                                           0, result)))
                    else:
                        url_results_list.append(tuple((settings_id, url, datetime.now(), key, None, None, 0, result)))
                elif new_value == old_value:
                    # values match
                    url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, None, 1, None)))
                else:
                    # values don't match
                    difference = get_difference(new_value, old_value)
                    send_url_changed_message(key, url, difference)
                    url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, new_value, 1, None)))

                # reset attributes
                for k in self.attributes:
                    self.attributes[k] = None

            # insert url_results to db
            insert_many_url_results(url_results_list)

    def parse_attribute(self, url, attribute):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        # response
        if attribute == 'response':
            try:
                self.attributes["response"] = str(urlopen(req).code)
                return 0
            except urllib.error.HTTPError as e:
                self.attributes["response"] = str(e.code)
                return str(e)

        soup = get_soup(req)

        # title
        if attribute == 'title':
            self.attributes["title"] = soup.find("title").string
            return 0

        # description, robots, og:image
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

        # content
        if attribute == 'content':
            hidden_tags = soup.select('.hidden')
            self.attributes["content"] = soup.get_text()
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
