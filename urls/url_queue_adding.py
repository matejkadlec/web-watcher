#!/usr/bin/python3
from database import select_urls_for_processing, select_from_config, insert_into_url_queue
import json
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def add_urls_to_queue(is_new):
    # Get all configs
    configs_db = select_from_config()

    # For each config, add all urls under that config which meet given conditions into url_queue
    for config_db in configs_db:
        url_queue_list = []

        # Get max value from config attributes
        config = json.loads(config_db[1])
        interval_key = max(config, key=config.get)

        # Add all urls and their config and settings id's into url_queue
        records = select_urls_for_processing(config_db[0], interval_key)
        for record in records:
            url_queue_list.append(tuple((record[0], record[1], record[2], is_new)))

        if url_queue_list:
            insert_into_url_queue(url_queue_list)


add_urls_to_queue(False)
