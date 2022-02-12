from database import select_from_queue, delete_from_queue, insert_many_results_db
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from datetime import datetime


ERROR_MESSAGE = "Couldn't parse web page content, see error for details."


def process_queue():
    global ERROR_MESSAGE

    # while there are any records in queue table
    while select_from_queue():
        results_list = []
        # select records from queue
        queues = select_from_queue()

        for queue in queues:
            # parse page content
            content = get_content(queue[2])
            content = ' '.join(content.split())

            # append result to the results_list for it to be inserted to db later
            if ERROR_MESSAGE in content:
                # if there was an error, replace content with ERROR_MESSAGE and add retrieved exception
                exception = content.split('.', 1)[1]
                results_list.append(tuple((queue[1], queue[2], ERROR_MESSAGE, 0, None, datetime.now(), exception)))
            else:
                results_list.append(tuple((queue[1], queue[2], content, 1, None, datetime.now(), None)))

        # after loop is finished, insert results to db and clear queue
        insert_many_results_db(results_list)
        delete_from_queue()


def get_content(url):
    global ERROR_MESSAGE
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # get HTML of compressed page
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    # if it's not compressed
    except gzip.BadGzipFile:
        try:
            html = urlopen(req).read().decode('utf-8')
        # any exception
        except Exception as e:
            return ERROR_MESSAGE + str(e)
    # any other exception
    except Exception as e:
        return ERROR_MESSAGE + str(e)

    # make BeautifulSoup from html
    soup = BeautifulSoup(html, features="html.parser")
    # return plain text
    return soup.get_text()


process_queue()
