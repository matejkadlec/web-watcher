#!/usr/bin/python3
from database import select_sitemap_results, insert_many_sitemap_results
from telegram_bot import send_sitemap_changed_message
from urllib.request import Request, urlopen
import urllib.error
from bs4 import BeautifulSoup
from datetime import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def get_sitemap_results():
    sitemap_results = select_sitemap_results()

    current_settings_id = sitemap_results[0][1]
    current_sitemap = sitemap_results[0][2]
    current_urls = []
    all_urls = []

    for sitemap_result in sitemap_results:
        new_urls = ""
        missing_urls = ""
        if sitemap_result[1] != current_settings_id or sitemap_result == sitemap_results[-1]:
            if sitemap_result == sitemap_results[-1]:
                current_urls.append(sitemap_result[3])

            new_urls_arr = get_urls(current_sitemap)

            for url in current_urls:
                if url in new_urls_arr:
                    all_urls.append(tuple((current_settings_id, current_sitemap, url, datetime.now(), 0, 0)))
                else:
                    missing_urls += f"{url}\n"
                    all_urls.append(tuple((current_settings_id, current_sitemap, url, datetime.now(), 0, 1)))
            for url in new_urls_arr:
                if url not in current_urls:
                    new_urls += f"{url}\n"
                    all_urls.append(tuple((current_settings_id, current_sitemap, url, datetime.now(), 1, 0)))

            if len(all_urls) >= 1000:
                insert_many_sitemap_results(all_urls)
                all_urls = []

            if new_urls != "" or missing_urls != "":
                send_sitemap_changed_message(current_sitemap, new_urls, missing_urls)

            current_settings_id = sitemap_result[1]
            current_sitemap = sitemap_result[2]
            current_urls = [sitemap_result[3]]
        else:
            current_urls.append(sitemap_result[3])

    insert_many_sitemap_results(all_urls)


def get_urls(base_url):
    # get sitemap as xml
    req = Request(base_url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        xml = urlopen(req).read()
    except urllib.error.HTTPError:
        return
    soup = BeautifulSoup(xml, features="html.parser")

    # initialize lists for sitemaps and urls for current sitemap
    sitemap_urls = []
    urls = []

    # find all sitemaps and urls
    sitemap_tags = soup.find_all("sitemap")
    url_tags = soup.find_all("url")

    # append links to lists
    for sitemap_tag in sitemap_tags:
        sitemap_urls.append(sitemap_tag.findNext("loc").text)
    for url_tag in url_tags:
        urls.append(url_tag.findNext("loc").text)

    # append settings for all sitemaps and urls
    return sitemap_urls + urls


get_sitemap_results()
