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
def insert_many_url_results(url_results_list, cursor, connection):
    sql_statement = "INSERT INTO url_results (settings_id, url, created, type, old_value, new_value, is_valid, error) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql_statement, url_results_list)
    connection.commit()


@with_cursor
def select_url_results(key, cursor, connection):
    cursor.execute("SELECT DISTINCT r1.settings_id, r1.url, r1.type, r1.old_value, r1.new_value "
                   "FROM url_results r1 "
                   "INNER JOIN "
                   "( "
                   "    SELECT MAX(created) MaxResultDate "
                   "    FROM url_results "
                   "    INNER JOIN settings s1 "
                   "        ON settings_id = s1.id "
                   "    INNER JOIN config c1 "
                   "        ON c1.id = s1.config_id "
                   "    WHERE type = %s AND (s1.last_run IS NULL "
                   "        OR TIMESTAMPDIFF(SECOND, s1.last_run, SYSDATE()) > JSON_VALUE(c1.config, %s)) "
                   "    GROUP BY settings_id "
                   ") r2 "
                   "ON r1.created = r2.MaxResultDate "
                   "WHERE type = %s ", (key, f'$.{key}', key))
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
    cursor.execute("SELECT queue.settings_id, queue.url, config FROM queue "
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
def insert_config(config, cursor, connection):
    cursor.execute("INSERT INTO config (config) VALUES (%s)", (config,))
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
    cursor.execute("SELECT DISTINCT * "
                   "FROM sitemap_results r1 "
                   "INNER JOIN "
                   "( "
                   "   SELECT MAX(created) MaxResultDate "
                   "   FROM sitemap_results "
                   "   INNER JOIN settings s1 "
                   "       ON settings_id = s1.id "
                   "   WHERE s1.last_run IS NULL OR TIMESTAMPDIFF(SECOND, s1.last_run, SYSDATE()) > 24 "
                   "   GROUP BY sitemap_results.url "
                   ") r2 "
                   "ON r1.created = r2.MaxResultDate "
                   "ORDER BY settings_id ")
    records = cursor.fetchall()
    return records
