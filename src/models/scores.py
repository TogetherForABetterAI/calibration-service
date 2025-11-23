
import datetime
from lib.db_engine import Base
from sqlalchemy import Column, Integer, LargeBinary, String, DateTime
from sqlalchemy.dialects.postgresql import UUID

class Scores(Base):
    __tablename__ = "scores"

    session_id = Column(UUID(as_uuid=True), primary_key=True)
    user_id = Column(UUID(as_uuid=True))
    batch_id = Column(Integer, nullable=False) # The last batch id that produced these scores
    status = Column(String, nullable=False)
    scores = Column(LargeBinary)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self) -> str:
        return f"Scores(user_id={self.user_id}, status={self.status}, scores={self.scores}, batch_id={self.batch_id})"