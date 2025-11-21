from typing import List
from typing import Optional
from lib.orm_base import Base
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
import uuid

class Scores(Base):
    __tablename__ = "scores"

    internal_id = Column(uuid.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id = Column(int, nullable=False) # The last batch id that produced these scores
    user_id = Column(uuid.UUID(as_uuid=True), primary_key=True)
    session_id = Column(uuid.UUID(as_uuid=True), primary_key=True, unique=True)
    scores = Column(bytes, unique=True)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"Scores(user_id={self.user_id}, status={self.status}, scores={self.scores}, batch_id={self.batch_id})"