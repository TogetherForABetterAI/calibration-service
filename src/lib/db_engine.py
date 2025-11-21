from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass

def get_engine(database_url) -> Optional[create_engine]:
    return create_engine(database_url, echo=True)