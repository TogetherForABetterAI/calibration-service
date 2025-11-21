

from typing import List
from typing import Optional
from lib.orm_base import Base
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
import uuid

class ModelInputs(Base):
    __tablename__ = "model_inputs"

    internal_id = Column(uuid.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(uuid.UUID(as_uuid=True), primary_key=True)
    session_id = Column(uuid.UUID(as_uuid=True), primary_key=True, unique=True)
    batch_id = Column(int, nullable=False)
    inputs = Column(bytes, unique=True)
    status = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"ModelInputs(user_id={self.user_id}, status={self.status}, inputs={self.inputs})"