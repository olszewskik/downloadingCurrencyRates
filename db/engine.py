from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,Session
from config.settings import config


class DbEngine:
    def __init__(self) -> None:
        self.engine = create_engine(config.DB_URL)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self) -> Session:
        return self.Session()