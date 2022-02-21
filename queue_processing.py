#!/usr/bin/python3
from database import select_from_queue, delete_from_queue, insert_many_url_results_db
import gzip
from urllib.request import Request, urlopen
import urllib.error
from bs4 import BeautifulSoup
from datetime import datetime
import ssl
import json

ssl._create_default_https_context = ssl._create_unverified_context
attributes = {"response": None, "title": None, "description": None, "robots": None, "image": None, "content": None}


def process_queue():
    global attributes
    init_attributes = attributes

    # while there are any records in queue table
    while select_from_queue():
        results_list = []
        # select records from queue
        queues = select_from_queue()

        for queue in queues:
            config = json.loads(queue[2])
            result = parse_url(queue[1], config)

            now = datetime.now()
            # append result to the results_list for it to be inserted to db later
            if result != 0:
                for key in config:
                    if config[key] != '0':
                        if key == "response":
                            results_list.append(tuple((queue[0], queue[1], now, key, attributes[key], None,
                                                       0, result)))
                        else:
                            results_list.append(tuple((queue[0], queue[1], now, key, None, None, 0, result)))
            else:
                for key in attributes:
                    if attributes[key]:
                        results_list.append(tuple((queue[0], queue[1], now, key, attributes[key], None, 1,
                                                   None)))

            attributes = init_attributes

        # after loop is finished, insert results to db and clear queue
        insert_many_url_results_db(results_list)
        delete_from_queue()


def parse_url(url, config):
    global attributes
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # response
    if config["response"] != '0':
        try:
            attributes["response"] = urlopen(req).code
        except urllib.error.HTTPError as e:
            attributes["response"] = e.code
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
    if config["title"] != '0':
        attributes["title"] = soup.find("title").string

    # description, robots, og:image
    for tag in soup.find_all("meta"):
        if tag.get("name") == "description" and config["description"] != '0':
            attributes["description"] = tag.get("content")
        elif tag.get("name") == "robots" and config["robots"] != '0':
            attributes["robots"] = tag.get("content")
        elif tag.get("property") == "og:image" and config["image"] != '0':
            attributes["image"] = tag.get("content")

    # content
    if config["content"] != '0':
        hidden_tags = soup.select('.hidden')
        attributes["content"] = soup.get_text()
        for hidden_tag in hidden_tags:
            if hidden_tag.string:
                attributes["content"] = attributes["content"].replace(hidden_tag.string, "")
                attributes["content"] = ' '.join(attributes["content"].split())

    return 0


process_queue()
