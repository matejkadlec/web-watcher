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
def insert_settings(url, is_sitemap, config_id, cursor, connection):
    cursor.execute("INSERT INTO settings (url, is_sitemap, is_active, config_id) "
                   "VALUES (%s, %s, %s, %s)", (url, is_sitemap, 1, config_id))
    connection.commit()
    return cursor.lastrowid


@with_cursor
def insert_many_settings(settings_list, cursor, connection):
    sql_statement = "INSERT INTO settings (url, is_sitemap, is_active, config_id) " \
                    "VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_statement, settings_list)
    connection.commit()


@with_cursor
def insert_url_results(url_results_list, cursor, connection):
    sql_statement = "INSERT INTO url_results (settings_id, url, created, type, old_value, new_value, error, attempt) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql_statement, url_results_list)
    connection.commit()


@with_cursor
def select_url_results(settings_id, key, cursor, connection):
    cursor.execute("SELECT DISTINCT r1.settings_id, r1.url, r1.type, r1.old_value, r1.new_value, chat_id "
                   "FROM url_results r1 "
                   "INNER JOIN "
                   "( "
                   "    SELECT MAX(created) MaxResultDate, chat_id "
                   "    FROM url_results "
                   "    INNER JOIN settings s1 "
                   "        ON settings_id = s1.id "
                   "    INNER JOIN config c1 "
                   "        ON c1.id = s1.config_id "
                   "    WHERE settings_id = %s AND "
                   "    TIMESTAMPDIFF(SECOND, s1.last_url_run, SYSDATE()) > JSON_VALUE(c1.config, %s) "
                   "    GROUP BY settings_id, url_results.type "
                   ") r2 "
                   "ON r1.created = r2.MaxResultDate "
                   "WHERE error IS NULL",
                   (settings_id, f'$.{key}'))
    records = cursor.fetchall()
    return records


@with_cursor
def insert_many_queues(queue_list, cursor, connection):
    sql_statement = "INSERT INTO queue (settings_id, url) " \
                    "VALUES (%s, %s)"
    cursor.executemany(sql_statement, queue_list)
    connection.commit()


@with_cursor
def select_from_queue(cursor, connection):
    cursor.execute("SELECT queue.settings_id, queue.url, config, chat_id FROM queue "
                   "JOIN settings ON queue.settings_id = settings.id "
                   "JOIN config ON settings.config_id = config.id "
                   "LIMIT 50")
    records = cursor.fetchall()

    return records


@with_cursor
def delete_from_queue(cursor, connection):
    cursor.execute("DELETE FROM queue LIMIT 50")
    connection.commit()


@with_cursor
def insert_config(config, chat_id, cursor, connection):
    cursor.execute("INSERT INTO config (config, chat_id) VALUES (%s, %s)", (config, chat_id))
    connection.commit()
    return cursor.lastrowid


@with_cursor
def insert_many_sitemap_results(sitemap_results_list, cursor, connection):
    sql_statement = "INSERT INTO sitemap_results (settings_id, sitemap, url, created, new, missing) " \
                    "VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql_statement, sitemap_results_list)
    connection.commit()


@with_cursor
def select_sitemap_results(cursor, connection):
    cursor.execute("SELECT DISTINCT settings_id, sitemap, url, chat_id "
                   "FROM sitemap_results r1 "
                   "INNER JOIN "
                   "( "
                   "   SELECT MAX(created) MaxResultDate, chat_id "
                   "   FROM sitemap_results "
                   "   INNER JOIN settings s1 "
                   "       ON settings_id = s1.id "
                   "   JOIN config c1 "
                   "       ON s1.config_id = c1.id"
                   "   WHERE s1.last_url_run IS NULL OR TIMESTAMPDIFF(SECOND, s1.last_url_run, SYSDATE()) > 24 "
                   "   GROUP BY sitemap_results.url "
                   ") r2 "
                   "ON r1.created = r2.MaxResultDate "
                   "WHERE missing != 1 "
                   "ORDER BY settings_id")
    records = cursor.fetchall()
    return records


@with_cursor
def select_urls_for_processing(config_id, key, cursor, connection):
    cursor.execute("SELECT DISTINCT config_id, settings.id, settings.url "
                   "FROM settings "
                   "JOIN config ON settings.config_id = config.id "
                   "LEFT JOIN url_results ON settings.id = url_results.settings_id "
                   "WHERE is_active = 1 AND is_sitemap = 0 AND config_id = %s AND error IS NULL AND "
                   "(last_url_run IS NULL OR "
                   "TIMESTAMPDIFF(SECOND, last_url_run, SYSDATE()) > JSON_VALUE(config.config, %s))",
                   (config_id, f'$.{key}'))
    records = cursor.fetchall()
    return records


@with_cursor
def select_erroneous_configs(cursor, connection):
    cursor.execute("SELECT DISTINCT settings.id, config FROM config "
                   "JOIN settings ON config.id = settings.config_id "
                   "JOIN url_results ON settings.id = url_results.settings_id "
                   "WHERE error IS NOT NULL")
    records = cursor.fetchall()
    return records


@with_cursor
def select_erroneous_urls(settings_id, key, cursor, connection):
    cursor.execute("SELECT DISTINCT r1.settings_id, r1.url, r1.type, r1.old_value, r1.attempt, chat_id "
                   "FROM url_results r1 "
                   "INNER JOIN "
                   "( "
                   "    SELECT DISTINCT MAX(created) MaxResultDate, chat_id "
                   "    FROM url_results "
                   "    INNER JOIN settings s1 "
                   "        ON settings_id = s1.id "
                   "    INNER JOIN config c1 "
                   "        ON c1.id = s1.config_id "
                   "    WHERE error IS NOT NULL AND settings_id = %s AND "
                   "    TIMESTAMPDIFF(SECOND, s1.last_error_run, SYSDATE()) > (JSON_VALUE(c1.config, %s) * attempt) "
                   "    GROUP BY settings_id, type "
                   ") r2 "
                   "ON r1.created = r2.MaxResultDate "
                   "WHERE attempt < 5 AND error IS NOT NULL", (settings_id, f'$.{key}'))
    records = cursor.fetchall()
    return records


@with_cursor
def select_from_config(cursor, connection):
    cursor.execute("SELECT id, config FROM config")
    records = cursor.fetchall()
    return records


@with_cursor
def insert_into_url_queue(url_queue_list, cursor, connection):
    sql_statement = "INSERT INTO url_queue (config_id, settings_id, url, is_new) " \
                    "VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_statement, url_queue_list)
    connection.commit()


@with_cursor
def select_distinct_configs(cursor, connection):
    cursor.execute("SELECT DISTINCT config_id, config, chat_id "
                   "FROM url_queue JOIN config ON url_queue.config_id = config.id")
    records = cursor.fetchall()
    return records


@with_cursor
def select_from_url_queue(config_id, is_new, cursor, connection):
    cursor.execute("SELECT settings_id, url FROM url_queue WHERE config_id = %s AND is_new = %s LIMIT 1",
                   (config_id, is_new))
    records = cursor.fetchall()
    return records


@with_cursor
def delete_from_url_queue(settings_id, is_new, cursor, connection):
    cursor.execute("DELETE FROM url_queue WHERE settings_id = %s AND is_new = %s", (settings_id, is_new))
    connection.commit()


@with_cursor
def insert_url_result(settings_id, url, created, type, old_value, new_value, error, attempt, cursor, connection):
    cursor.execute("INSERT INTO url_results (settings_id, url, created, type, old_value, new_value, error, attempt) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                   (settings_id, url, created, type, old_value, new_value, error, attempt))
    connection.commit()

