from contextlib import contextmanager
from sqlalchemy.orm import Session
from db.engine import DbEngine


class SessionManager:
    def __init__(self, db_engine: DbEngine) -> None:
        self.db_engine = db_engine

    @contextmanager
    def session_scope(self) -> Session:
        session = self.db_engine.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()