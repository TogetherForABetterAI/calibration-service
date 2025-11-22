

from datetime import datetime
from lib.orm_base import Base
from sqlalchemy import Column, DateTime, String
import uuid

class ModelOutputs(Base):
    __tablename__ = "model_outputs"

    session_id = Column(uuid.UUID(as_uuid=True), nullable=False, primary_key=True)
    batch_id = Column(int, nullable=False, primary_key=True)
    user_id = Column(uuid.UUID(as_uuid=True))
    timestamp = Column(DateTime, default=datetime.utcnow)
    outputs = Column(bytes)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"ModelOutputs(user_id={self.user_id}, status={self.status}, outputs={self.outputs})"