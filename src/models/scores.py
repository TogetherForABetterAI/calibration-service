
import datetime
from click import DateTime
from lib.orm_base import Base
from sqlalchemy import Column, String
import uuid

class Scores(Base):
    __tablename__ = "scores"

    session_id = Column(uuid.UUID(as_uuid=True), primary_key=True)
    user_id = Column(uuid.UUID(as_uuid=True))
    batch_id = Column(int, nullable=False) # The last batch id that produced these scores
    status = Column(String, nullable=False)
    scores = Column(bytes)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self) -> str:
        return f"Scores(user_id={self.user_id}, status={self.status}, scores={self.scores}, batch_id={self.batch_id})"