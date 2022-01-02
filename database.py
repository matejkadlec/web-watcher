import mysql.connector


def with_cursor(f):
    def wrapper(*args):
        connection = mysql.connector.connect(host='127.0.0.1',
                                             port='3306',
                                             database='root',
                                             user='root',
                                             password='posote19')
        cursor = connection.cursor()

        values = f(*args, cursor, connection)
        cursor.close()
        connection.close()
        return values
    return wrapper


@with_cursor
def insert_settings_db(type, url, is_sitemap, interval, cursor, connection):
    cursor.execute("INSERT INTO settings (`type`, url, is_sitemap, `interval`) "
                   "VALUES (%s, %s, %s, %s)", (type, url, is_sitemap, interval))
    connection.commit()
    return cursor.lastrowid


@with_cursor
def insert_many_settings_db(settings_list, cursor, connection):
    sql_statement = "INSERT INTO settings (`type`, url, is_sitemap, `interval`) " \
                    "VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_statement, settings_list)
    connection.commit()


@with_cursor
def insert_result_db(settings_id, url, content, is_valid, cursor, connection):
    cursor.execute("INSERT INTO results (settings_id, url, content, is_valid) "
                   "VALUES (%s, %s, %s, %s)", (settings_id, url, content, is_valid))
    connection.commit()


@with_cursor
def insert_many_results_db(results_list, cursor, connection):
    sql_statement = "INSERT INTO results (settings_id, url, content, is_valid) " \
                    "VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_statement, results_list)
    connection.commit()


@with_cursor
def insert_many_queues_db(queue_list, cursor, connection):
    sql_statement = "INSERT INTO queue (settings_id, url) " \
                    "VALUES (%s, %s)"
    cursor.executemany(sql_statement, queue_list)
    connection.commit()


@with_cursor
def select_from_queue(cursor, connection):
    cursor.execute("SELECT * FROM queue LIMIT 10")
    records = cursor.fetchall()
    return records


@with_cursor
def delete_from_queue(cursor, connection):
    cursor.execute("DELETE FROM queue LIMIT 10")
    connection.commit()
