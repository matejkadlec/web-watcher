#!/usr/bin/python3
from database import select_sitemap_results, insert_many_sitemap_results
from utils.telegram_bot import TelegramBot
from utils.utils import get_urls
from datetime import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def get_sitemap_results():
    sitemap_results = select_sitemap_results()

    if not sitemap_results:
        return

    current_settings_id = sitemap_results[0][0]
    current_sitemap = sitemap_results[0][1]
    current_urls = []
    all_urls = []

    for sitemap_result in sitemap_results:
        new_urls = ""
        missing_urls = ""
        if sitemap_result[0] != current_settings_id or sitemap_result == sitemap_results[-1]:
            if sitemap_result == sitemap_results[-1]:
                current_urls.append(sitemap_result[2])

            sitemap_urls, urls = get_urls(current_sitemap)
            new_urls_arr = sitemap_urls + urls

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
                tb = TelegramBot(sitemap_result[3])
                tb.send_sitemap_changed_message(current_sitemap, new_urls, missing_urls)

            current_settings_id = sitemap_result[0]
            current_sitemap = sitemap_result[1]
            current_urls = [sitemap_result[2]]
        else:
            current_urls.append(sitemap_result[2])

    insert_many_sitemap_results(all_urls)


get_sitemap_results()
