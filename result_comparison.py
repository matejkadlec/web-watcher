#!/usr/bin/python3
from database import select_results_db, insert_many_results_db
from queue_processing import ERROR_CODE, get_content
from datetime import datetime
import time
import telebot
import ssl


ssl._create_default_https_context = ssl._create_unverified_context


def compare_results():
    results_list = []
    # get all results from db (for which setting meets given conditions)
    results = select_results_db()
    bot = telebot.TeleBot('5038704908:AAEAAkRdrWtJs384RPIGqEUiX0xPSp5IrM8')

    for result in results:
        # parse current page
        response, title, description, robots, image, content = get_content(result[2])

        # append result to the results_list for it to be inserted to db later
        if ERROR_CODE in content:
            # if there was an error, replace content with ERROR_MESSAGE and add retrieved exception
            exception = content.split('KZ7h84UJ4v', 1)[1]
            results_list.append(tuple((result[1], result[2], datetime.now(), response, None, None, None,
                                       None, None, 0, None, 0, exception)))
        elif response == result[4] and title == result[5] and description == result[6] and robots == result[7] and \
                image == result[8] and content == result[9]:
            # everything match
            results_list.append(tuple((result[1], result[2], datetime.now(), response, title, description, robots,
                                       image, content, 0, None, 1, None)))
        else:
            # something doesn't match
            values = [response, title, description, robots, image, content]
            names = ['Response', 'Title', 'Description', 'Robots', 'Image', 'Content']
            changed = ""
            for i in range(0, 6):
                # iterate through all values and filter those that don't match
                if values[i] != result[i + 4]:
                    changed += f'{names[i]}, '
                    if names[i] == 'Response':
                        # int change
                        old = str(result[i + 4])
                        new = str(values[i])
                    else:
                        # string change
                        old = ""
                        new = ""
                        # get first 200 or value length characters since first difference
                        for j in range(min(len(values[i]), len(result[i + 4]))):
                            if values[i][j] != result[i + 4][j]:
                                for k in range(0, min(200, len(values[i]) - j, len(result[i + 4]) - j)):
                                    old += result[i + 4][j + k]
                                    new += values[i][j + k]
                                break
                    # save difference to string
                    difference = f"OLD VALUE: \n-> {old}\n\nNEW VALUE: \n-> {new}"

                    # send message on telegram
                    bot.send_message(-1001731120154, f"{names[i]} of URL {result[2]} changed.\n\n{difference}")
                    time.sleep(3)

            results_list.append(tuple((result[1], result[2], datetime.now(), response, title, description, robots,
                                       image, content, 1, changed[:-2], 1, None)))

    # insert results to db
    insert_many_results_db(results_list)


compare_results()
