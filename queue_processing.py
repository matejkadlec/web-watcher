#!/usr/bin/python3
from database import select_from_queue, delete_from_queue, insert_many_results_db
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import ssl


ssl._create_default_https_context = ssl._create_unverified_context
ERROR_CODE = "HSp6625YJuKZ7h84UJ4v"


def process_queue():
    global ERROR_CODE

    # while there are any records in queue table
    while select_from_queue():
        results_list = []
        # select records from queue
        queues = select_from_queue()

        for queue in queues:
            # parse page content
            response, title, description, robots, image, content = get_content(queue[2])

            # append result to the results_list for it to be inserted to db later
            if ERROR_CODE in content:
                # if there was an error, replace content with ERROR_MESSAGE and add retrieved exception
                exception = content.split('KZ7h84UJ4v', 1)[1]
                results_list.append(tuple((queue[1], queue[2], datetime.now(), response, None, None, None,
                                           None, None, 0, None, 0, exception)))
            else:
                results_list.append(tuple((queue[1], queue[2], datetime.now(), response, title, description, robots,
                                           image, content, 0, None, 1, None)))

        # after loop is finished, insert results to db and clear queue
        insert_many_results_db(results_list)
        delete_from_queue()


def get_content(url):
    global ERROR_CODE
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urlopen(req).code

    # get HTML of compressed page
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    # if isn't compressed
    except gzip.BadGzipFile:
        try:
            html = urlopen(req).read().decode('utf-8')
        # any exception
        except Exception as e:
            return response, None, None, None, None, ERROR_CODE + str(e)
    # any other exception
    except Exception as e:
        return response, None, None, None, None, ERROR_CODE + str(e)

    # make BeautifulSoup from html
    soup = BeautifulSoup(html, features="html.parser")

    # title
    title = soup.find("title").string

    # description, robots, og:image
    description = robots = image = ''
    for tag in soup.find_all("meta"):
        if tag.get("name") == "description":
            description = tag.get("content")
        elif tag.get("name") == "robots":
            robots = tag.get("content")
        elif tag.get("property") == "og:image":
            image = tag.get("content")

    # content
    hidden_tags = soup.select('.hidden')
    content = soup.get_text()
    for hidden_tag in hidden_tags:
        if hidden_tag.string:
            content = content.replace(hidden_tag.string, "")

    # remove line breaks and empty spaces
    content = ' '.join(content.split())

    return response, title, description, robots, image, content


process_queue()
