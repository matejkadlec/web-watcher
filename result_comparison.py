from database import select_results_db, insert_many_results_db
from queue_processing import ERROR_MESSAGE, get_content
from datetime import datetime
import telegram_send


def compare_results():
    results_list = []
    # get all results from db (for which settings meets given conditions)
    results = select_results_db()
    for result in results:
        # parse current content
        content = get_content(result[2])
        # append result to the results_list for it to be inserted to db later
        if ERROR_MESSAGE in content:
            # if there was an error, replace content with ERROR_MESSAGE and add retrieved exception
            exception = content.split('.', 1)[1]
            results_list.append(tuple((result[1], result[2], ERROR_MESSAGE, 0, None, datetime.now(), exception)))
        elif content == result[3]:
            # contents match
            results_list.append(tuple((result[1], result[2], content, 1, None, datetime.now(), None)))
        else:
            # contents don't match
            old = ""
            new = ""
            # get first 50 characters since first difference
            for i in range(min(len(content), len(result[3]))):
                if content[i] != result[3][i]:
                    old += result[3][i]
                    new += content[i]
                if len(old) >= 50:
                    break
            difference = f"OLD VALUE: \n{old.strip()}...\n\nNEW VALUE: \n{new.strip()}..."
            # send message on telegram
            telegram_send.send(messages=[f"Content of URL {result[2]} has changed.\n\n{difference}"])
            results_list.append(tuple((result[1], result[2], content, 0, difference, datetime.now(), None)))
    # insert results to db
    insert_many_results_db(results_list)


# compare_results()
