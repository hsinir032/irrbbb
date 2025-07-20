# database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os
from typing import Generator

# Database Configuration
# IMPORTANT: Read DATABASE_URL from environment variable first
# This is how Render will provide the connection string to your PostgreSQL database.
# If running locally without the env var, it will fall back to localhost.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://irrbb_user:irrbb_password@localhost:5432/irrbb_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency to get a database session
# This function will be used by FastAPI's dependency injection system
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
