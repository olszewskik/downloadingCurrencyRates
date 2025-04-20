from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config import DB_URL

# Inicjalizacja silnika SQLAlchemy
engine: Engine = create_engine(DB_URL)

def get_engine() -> Engine:
    """Zwraca instancję silnika SQLAlchemy."""
    return engine

def get_connection():
    """Zwraca połączenie do bazy danych."""
    return engine.connect()
