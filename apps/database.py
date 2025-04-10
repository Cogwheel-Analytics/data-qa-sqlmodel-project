from sqlmodel import SQLModel, create_engine, Session
from contextlib import contextmanager

DATABASE_URL = (
    "postgresql+psycopg2://cwa_local_user:Pass%40123@localhost:5432/cwa_local_db"
)


# Create the engine
engine = create_engine(DATABASE_URL, echo=True)


# Context manager for sessions
@contextmanager
def get_session():
    with Session(engine) as session:
        yield session
