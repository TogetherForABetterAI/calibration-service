from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

def get_engine(database_url) -> Optional[create_engine]:
    return create_engine(database_url, echo=False)