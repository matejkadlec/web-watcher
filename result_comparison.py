#!/usr/bin/python3
from database import select_url_results_db, insert_many_url_results_db
from telegram_bot import send_error_message, send_changed_message
import gzip
from urllib.request import Request, urlopen
import urllib.error
from bs4 import BeautifulSoup
from datetime import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
attributes = {"response": None, "title": None, "description": None, "robots": None, "image": None, "content": None}
error_url_list = []


def compare_results():
    global attributes
    global error_url_list

    results = []
    for key in attributes:
        results.append(select_url_results_db(key))

    index = 0
    for key in attributes:
        url_results_list = []
        # get all url_results from db for given key
        url_results = results[index]
        index += 1
        if not url_results:
            continue

        for url_result in url_results:
            # parse current page
            result = parse_attribute(url_result[1], url_result[2])
            new_value = attributes[key]
            old_value = str(url_result[3]) if not url_result[4] else str(url_result[4])
            settings_id = url_result[0]
            url = url_result[1]

            # parsing wasn't successful
            if result != 0:
                if url not in error_url_list:
                    send_error_message(url, result)
                    error_url_list.append(url)
                # we got response even when parsing wasn't successful
                if key == "response":
                    if new_value == old_value:
                        # responses match
                        url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, None, 0, result)))
                    else:
                        # responses don't match
                        difference = calculate_difference(new_value, old_value)
                        send_changed_message(key, url, difference)
                        url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, new_value, 0, result)))
                else:
                    url_results_list.append(tuple((settings_id, url, datetime.now(), key, None, None, 0, result)))
            elif new_value == old_value:
                # values match
                url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, None, 1, None)))
            else:
                # values don't match
                difference = calculate_difference(new_value, old_value)
                send_changed_message(key, url, difference)
                url_results_list.append(tuple((settings_id, url, datetime.now(), key, old_value, new_value, 1, None)))

            # reset attributes
            for k in attributes:
                attributes[k] = None

        # insert url_results to db
        insert_many_url_results_db(url_results_list)


def parse_attribute(url, attribute):
    global attributes
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # response
    if attribute == 'response':
        try:
            attributes["response"] = str(urlopen(req).code)
            return 0
        except urllib.error.HTTPError as e:
            attributes["response"] = str(e.code)
            return str(e)

    # get HTML of compressed page
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    # if isn't compressed
    except gzip.BadGzipFile:
        try:
            html = urlopen(req).read().decode('utf-8')
        except Exception as e:
            return str(e)
    except Exception as e:
        return str(e)

    # make BeautifulSoup from html
    soup = BeautifulSoup(html, features="html.parser")

    # title
    if attribute == 'title':
        attributes["title"] = soup.find("title").string
        return 0

    # description, robots, og:image
    for tag in soup.find_all("meta"):
        if tag.get("name") == "description" and attribute == 'description':
            attributes["description"] = tag.get("content")
            return 0
        elif tag.get("name") == "robots" and attribute == 'robots':
            attributes["robots"] = tag.get("content")
            return 0
        elif tag.get("property") == "og:image" and attribute == 'image':
            attributes["image"] = tag.get("content")
            return 0

    # content
    if attribute == 'content':
        hidden_tags = soup.select('.hidden')
        attributes["content"] = soup.get_text()
        for hidden_tag in hidden_tags:
            if hidden_tag.string:
                attributes["content"] = attributes["content"].replace(hidden_tag.string, "")
        attributes["content"] = ' '.join(attributes["content"].split())
        return 0

    return 0


def calculate_difference(new_value, old_value):
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


compare_results()
