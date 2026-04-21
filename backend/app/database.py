from collections.abc import Generator
from functools import lru_cache

from sqlalchemy.engine import make_url
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings


class Base(DeclarativeBase):
    pass


def get_engine_kwargs(database_url: str | None = None) -> dict[str, object]:
    settings = get_settings()
    url = make_url(database_url or settings.database_url)
    engine_kwargs: dict[str, object] = {"pool_pre_ping": True}

    if url.get_backend_name() != "sqlite":
        engine_kwargs.update(
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            pool_timeout=settings.database_pool_timeout,
        )

    return engine_kwargs


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, **get_engine_kwargs(settings.database_url))


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(autoflush=False, autocommit=False, bind=get_engine())


def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def db_health_check() -> bool:
    engine = get_engine()
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
