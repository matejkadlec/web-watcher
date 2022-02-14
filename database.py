import mysql.connector


def with_cursor(f):
    def wrapper(*args):
        connection = mysql.connector.connect(host='localhost',
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
def select_from_settings(cursor, connection):
    cursor.execute("SELECT * FROM settings")
    records = cursor.fetchall()
    return records


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
def insert_result_db(settings_id, url, created, response, title, description, robots, image, content, has_changed,
                     changed, is_valid, error, cursor, connection):
    cursor.execute("INSERT INTO results (settings_id, url, created, response, title, description, robots, image, "
                   "content, has_changed, changed, is_valid, error) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (settings_id, url, created, response, title, description, robots, image, content, has_changed,
                    changed, is_valid, error))
    connection.commit()


@with_cursor
def insert_many_results_db(results_list, cursor, connection):
    sql_statement = "INSERT INTO results (settings_id, url, created, response, title, description, robots, image, " \
                    "content, has_changed, changed, is_valid, error) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql_statement, results_list)
    connection.commit()


@with_cursor
def select_results_db(cursor, connection):
    cursor.execute("SELECT r1.* "
                   "FROM results r1 "
                   "INNER JOIN "
                   "( "
                   "    SELECT DISTINCT MAX(created) MaxResultDate "
                   "    FROM results "
                   "    INNER JOIN settings s1 "
                   "        ON settings_id = s1.id "
                   "            AND s1.last_run IS NULL "
                   "            OR TIMESTAMPDIFF(SECOND, last_run, SYSDATE()) >= s1.`interval` "
                   "     WHERE error IS NULL "
                   "     GROUP BY settings_id "
                   ") r2 "
                   "    ON r1.created = r2.MaxResultDate "
                   "WHERE r1.error IS NULL "
                   "ORDER BY r1.created DESC")
    records = cursor.fetchall()
    return records


@with_cursor
def insert_many_queues_db(queue_list, cursor, connection):
    sql_statement = "INSERT INTO queue (settings_id, url) " \
                    "VALUES (%s, %s)"
    cursor.executemany(sql_statement, queue_list)
    connection.commit()


@with_cursor
def select_from_queue(cursor, connection):
    cursor.execute("SELECT * FROM queue LIMIT 50")
    records = cursor.fetchall()
    return records


@with_cursor
def delete_from_queue(cursor, connection):
    cursor.execute("DELETE FROM queue LIMIT 50")
    connection.commit()
