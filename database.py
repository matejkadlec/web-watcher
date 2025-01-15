from typing import List, Tuple, Callable
from functools import wraps
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Database:
    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            connection_string = (
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            cls._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=int(os.getenv("DB_POOL_SIZE", 5)),
                pool_pre_ping=True,
            )
        return cls._engine


def with_connection(f: Callable) -> Callable:
    @wraps(f)
    def wrapper(*args, **kwargs):
        engine = Database.get_engine()
        with engine.connect() as connection:
            try:
                result = f(*args, connection=connection, **kwargs)
                return result
            except Exception as e:
                connection.rollback()
                raise e

    return wrapper


class DatabaseOperations:
    @staticmethod
    @with_connection
    def select_from_settings(connection) -> List[Tuple]:
        result = connection.execute(text("SELECT * FROM settings"))
        return result.fetchall()

    @staticmethod
    @with_connection
    def insert_settings(url: str, is_sitemap: bool, config_id: int, connection) -> int:
        result = connection.execute(
            text(
                "INSERT INTO settings (url, is_sitemap, is_active, config_id) VALUES (:url, :is_sitemap, :active, :config_id) RETURNING id"
            ),
            {"url": url, "is_sitemap": is_sitemap, "active": 1, "config_id": config_id},
        )
        connection.commit()
        return result.scalar()

    @staticmethod
    @with_connection
    def insert_many_settings(settings_list: List[Tuple], connection) -> None:
        connection.execute(
            text(
                "INSERT INTO settings (url, is_sitemap, is_active, config_id) VALUES (:url, :is_sitemap, :active, :config_id)"
            ),
            [
                {"url": s[0], "is_sitemap": s[1], "active": s[2], "config_id": s[3]}
                for s in settings_list
            ],
        )
        connection.commit()

    @staticmethod
    @with_connection
    def select_url_results(settings_id: int, key: str, connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT r1.settings_id, r1.url, r1.type, r1.old_value, r1.new_value, chat_id
            FROM url_results r1
            INNER JOIN (
                SELECT MAX(created) MaxResultDate, chat_id
                FROM url_results
                INNER JOIN settings s1 ON settings_id = s1.id
                INNER JOIN config c1 ON c1.id = s1.config_id
                WHERE settings_id = :settings_id 
                AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s1.last_url_run)) >= (c1.config ->> :key)::int
                GROUP BY settings_id, url_results.type
            ) r2 ON r1.created = r2.MaxResultDate
            WHERE error IS NULL
        """
        result = connection.execute(
            text(query), {"settings_id": settings_id, "key": key}
        )
        return result.fetchall()

    @staticmethod
    @with_connection
    def insert_url_results(url_results_list: List[Tuple], connection) -> None:
        sql_statement = (
            "INSERT INTO url_results (settings_id, url, created, type, old_value, new_value, error, attempt) "
            "VALUES (:settings_id, :url, :created, :type, :old_value, :new_value, :error, :attempt)"
        )
        connection.execute(
            text(sql_statement),
            [
                {
                    "settings_id": ur[0],
                    "url": ur[1],
                    "created": ur[2],
                    "type": ur[3],
                    "old_value": ur[4],
                    "new_value": ur[5],
                    "error": ur[6],
                    "attempt": ur[7],
                }
                for ur in url_results_list
            ],
        )
        connection.commit()

    @staticmethod
    @with_connection
    def insert_config(config: str, chat_id: int, connection) -> int:
        result = connection.execute(
            text(
                "INSERT INTO config (config, chat_id) VALUES (:config, :chat_id) RETURNING id"
            ),
            {"config": config, "chat_id": chat_id},
        )
        connection.commit()
        return result.scalar()

    @staticmethod
    @with_connection
    def insert_many_sitemap_results(
        sitemap_results_list: List[Tuple], connection
    ) -> None:
        sql_statement = (
            "INSERT INTO sitemap_results (settings_id, sitemap, url, created, new, missing) "
            "VALUES (:settings_id, :sitemap, :url, :created, :new, :missing)"
        )
        connection.execute(
            text(sql_statement),
            [
                {
                    "settings_id": sr[0],
                    "sitemap": sr[1],
                    "url": sr[2],
                    "created": sr[3],
                    "new": sr[4],
                    "missing": sr[5],
                }
                for sr in sitemap_results_list
            ],
        )
        connection.commit()

    @staticmethod
    @with_connection
    def select_sitemap_results(connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT settings_id, sitemap, url, chat_id 
            FROM sitemap_results r1 
            INNER JOIN 
            ( 
               SELECT MAX(created) MaxResultDate, chat_id 
               FROM sitemap_results 
               INNER JOIN settings s1 
                   ON settings_id = s1.id 
               JOIN config c1 
                   ON s1.config_id = c1.id
               WHERE s1.last_url_run IS NULL OR EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s1.last_url_run)) >= 24 
               GROUP BY sitemap_results.url 
            ) r2 
            ON r1.created = r2.MaxResultDate 
            WHERE missing != 1 
            ORDER BY settings_id
        """
        result = connection.execute(text(query))
        return result.fetchall()

    @staticmethod
    @with_connection
    def select_urls_for_processing(config_id: int, key: str, connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT config_id, settings.id, settings.url 
            FROM settings 
            JOIN config ON settings.config_id = config.id 
            LEFT JOIN url_results ON settings.id = url_results.settings_id 
            WHERE is_active = 1 AND is_sitemap = 0 AND config_id = :config_id AND error IS NULL AND 
            (last_url_run IS NULL OR 
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_url_run)) >= (config.config ->> :key)::int)
        """
        result = connection.execute(text(query), {"config_id": config_id, "key": key})
        return result.fetchall()

    @staticmethod
    @with_connection
    def select_erroneous_configs(connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT settings.id, config 
            FROM config 
            JOIN settings ON config.id = settings.config_id 
            JOIN url_results ON settings.id = url_results.settings_id 
            WHERE error IS NOT NULL
        """
        result = connection.execute(text(query))
        return result.fetchall()

    @staticmethod
    @with_connection
    def select_erroneous_urls(settings_id: int, key: str, connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT r1.settings_id, r1.url, r1.type, r1.old_value, r1.attempt, chat_id 
            FROM url_results r1 
            INNER JOIN 
            ( 
                SELECT DISTINCT MAX(created) MaxResultDate, chat_id 
                FROM url_results 
                INNER JOIN settings s1 
                    ON settings_id = s1.id 
                INNER JOIN config c1 
                    ON c1.id = s1.config_id 
                WHERE error IS NOT NULL AND settings_id = :settings_id AND 
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s1.last_error_run)) >= ((c1.config ->> :key)::int * attempt) 
                GROUP BY settings_id, type 
            ) r2 
            ON r1.created = r2.MaxResultDate 
            WHERE attempt < 5 AND error IS NOT NULL
        """
        result = connection.execute(
            text(query), {"settings_id": settings_id, "key": key}
        )
        return result.fetchall()

    @staticmethod
    @with_connection
    def select_from_config(connection) -> List[Tuple]:
        result = connection.execute(text("SELECT id, config FROM config"))
        return result.fetchall()

    @staticmethod
    @with_connection
    def insert_into_url_queue(url_queue_list: List[Tuple], connection) -> None:
        sql_statement = (
            "INSERT INTO url_queue (config_id, settings_id, url, is_new) "
            "VALUES (:config_id, :settings_id, :url, :is_new)"
        )
        connection.execute(
            text(sql_statement),
            [
                {
                    "config_id": uq[0],
                    "settings_id": uq[1],
                    "url": uq[2],
                    "is_new": uq[3],
                }
                for uq in url_queue_list
            ],
        )
        connection.commit()

    @staticmethod
    @with_connection
    def select_distinct_configs(connection) -> List[Tuple]:
        query = """
            SELECT DISTINCT config_id, config, chat_id 
            FROM url_queue JOIN config ON url_queue.config_id = config.id
        """
        result = connection.execute(text(query))
        return result.fetchall()

    @staticmethod
    @with_connection
    def select_from_url_queue(config_id: int, is_new: bool, connection) -> List[Tuple]:
        query = """
            SELECT settings_id, url 
            FROM url_queue 
            WHERE config_id = :config_id AND is_new = :is_new 
            LIMIT 1
        """
        result = connection.execute(
            text(query), {"config_id": config_id, "is_new": is_new}
        )
        return result.fetchall()

    @staticmethod
    @with_connection
    def delete_from_url_queue(settings_id: int, is_new: bool, connection) -> None:
        query = """
            DELETE FROM url_queue 
            WHERE settings_id = :settings_id AND is_new = :is_new
        """
        connection.execute(text(query), {"settings_id": settings_id, "is_new": is_new})
        connection.commit()

    @staticmethod
    @with_connection
    def insert_url_result(
        settings_id: int,
        url: str,
        created: datetime,
        type: str,
        old_value: str,
        new_value: str,
        error: str,
        attempt: int,
        connection,
    ) -> None:
        query = """
            INSERT INTO url_results (settings_id, url, created, type, old_value, new_value, error, attempt) 
            VALUES (:settings_id, :url, :created, :type, :old_value, :new_value, :error, :attempt)
        """
        connection.execute(
            text(query),
            {
                "settings_id": settings_id,
                "url": url,
                "created": created,
                "type": type,
                "old_value": old_value,
                "new_value": new_value,
                "error": error,
                "attempt": attempt,
            },
        )
        connection.commit()
