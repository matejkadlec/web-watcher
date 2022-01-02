from database import select_from_queue, delete_from_queue, insert_many_results_db
import gzip
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def process_queue():
    while select_from_queue():
        results_list = []
        queues = select_from_queue()
        for queue in queues:
            content = get_content(queue[2])
            results_list.append(tuple((queue[1], queue[2], content, 1)))
        insert_many_results_db(results_list)
        delete_from_queue()


def get_content(url):
    req = Request(url)
    # some urls have their content zipped
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    except gzip.BadGzipFile:
        html = urlopen(req).read().decode('utf-8')
    # make BeautifulSoup from html
    soup = BeautifulSoup(html, features="html.parser")
    # return plain text
    return soup.get_text()
