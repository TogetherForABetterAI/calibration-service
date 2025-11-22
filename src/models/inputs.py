

import datetime
from lib.orm_base import Base
from sqlalchemy import Column, DateTime, String
import uuid

class ModelInputs(Base):
    __tablename__ = "model_inputs"

    user_id = Column(uuid.UUID(as_uuid=True), primary_key=True)
    session_id = Column(uuid.UUID(as_uuid=True), primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    batch_id = Column(int, nullable=False)
    inputs = Column(bytes)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"ModelInputs(user_id={self.user_id}, status={self.status}, inputs={self.inputs})"