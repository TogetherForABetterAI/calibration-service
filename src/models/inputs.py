

import datetime
from src.lib.db_engine import Base
from sqlalchemy import Column, DateTime, Integer, LargeBinary, String
from sqlalchemy.dialects.postgresql import UUID

class ModelInputs(Base):
    __tablename__ = "model_inputs"

    user_id = Column(UUID(as_uuid=True), primary_key=True)
    session_id = Column(UUID(as_uuid=True), primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    batch_id = Column(Integer, nullable=False)
    inputs = Column(LargeBinary)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"ModelInputs(user_id={self.user_id}, status={self.status}, inputs={self.inputs})"