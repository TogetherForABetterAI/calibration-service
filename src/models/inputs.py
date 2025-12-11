

import datetime
from src.lib.db_engine import Base
from sqlalchemy import Column, DateTime, Integer, LargeBinary
from sqlalchemy.dialects.postgresql import UUID


class ModelInputs(Base):
    __tablename__ = "model_inputs"

    session_id = Column(UUID(as_uuid=True), nullable=False, primary_key=True)
    batch_index = Column(Integer, nullable=False, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    inputs = Column(LargeBinary)

    def __repr__(self) -> str:
        return f"ModelInputs(session_id={self.session_id}, batch_index={self.batch_index}, inputs={self.inputs}, labels={self.labels})"