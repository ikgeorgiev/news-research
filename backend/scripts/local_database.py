"""Connection settings for host-side development and read-only research tools."""
from contextlib import contextmanager
import os
from pathlib import Path

from dotenv import dotenv_values


def published_postgres_port():
    value = os.environ.get('POSTGRES_PORT')
    if value is None:
        value = dotenv_values(Path(__file__).resolve().parents[2] / '.env').get('POSTGRES_PORT')
    if not value:
        return 5433
    if not value.isascii() or not value.isdecimal() or not 1 <= int(value) <= 65535:
        raise ValueError('POSTGRES_PORT must be a port between 1 and 65535')
    return int(value)


@contextmanager
def local_report_database(database_url):
    import psycopg
    from psycopg.rows import dict_row
    from sqlalchemy.engine import make_url

    port = published_postgres_port()
    dsn = make_url(database_url)
    if (dsn.get_backend_name() != 'postgresql' or dsn.database != 'cef_news'
            or dsn.host not in {'localhost', '127.0.0.1', 'db'}):
        raise ValueError('Local production database identity changed; review before connecting')
    expected_url_port = 5432 if dsn.host == 'db' else port
    if (dsn.port or 5432) != expected_url_port:
        raise ValueError('DATABASE_URL and the configured Compose port mapping disagree')
    with psycopg.connect(host='127.0.0.1', hostaddr='127.0.0.1', port=port, user=dsn.username,
                         password=dsn.password, dbname=dsn.database, connect_timeout=5,
                         row_factory=dict_row, options='-c statement_timeout=5000 -c lock_timeout=5000') as db:
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        yield db
