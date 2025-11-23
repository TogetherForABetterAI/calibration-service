

from datetime import datetime
from lib.db_engine import Base
from sqlalchemy import Column, DateTime, Integer, LargeBinary, String
from sqlalchemy.dialects.postgresql import UUID



class ModelOutputs(Base):
    __tablename__ = "model_outputs"

    session_id = Column(UUID(as_uuid=True), nullable=False, primary_key=True)
    batch_id = Column(Integer, nullable=False, primary_key=True)
    user_id = Column(UUID(as_uuid=True))
    timestamp = Column(DateTime, default=datetime.utcnow)
    outputs = Column(LargeBinary)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"ModelOutputs(user_id={self.user_id}, status={self.status}, outputs={self.outputs})"