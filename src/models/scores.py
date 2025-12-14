import datetime
from sqlalchemy import Column, Integer, String, Float, Enum as SqEnum, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.types import LargeBinary
from src.lib.db_engine import Base
from src.lib.calibration_stages import CalibrationStage

class Scores(Base):
    __tablename__ = "scores"

    session_id = Column(UUID(as_uuid=True), primary_key=True)
    batchs_counter = Column(Integer, default=0, nullable=False)
    stage = Column(SqEnum(CalibrationStage), nullable=False, default=CalibrationStage.INITIAL_CALIBRATION)
    
    # Variables del UQ
    alpha = Column(Float, nullable=True)
    scores = Column(LargeBinary, nullable=True)

    confidences = Column(LargeBinary, nullable=True)   
    alphas = Column(ARRAY(Float), default=[], nullable=True)         
    uncertainties = Column(ARRAY(Float), default=[], nullable=True)  
    coverages = Column(ARRAY(Float), default=[], nullable=True)      
    setsizes = Column(ARRAY(Integer), default=[], nullable=True) 
    accuracy = Column(Float, default=0.0)
    correct_preds = Column(Integer, default=0)
    total_samples = Column(Integer, default=0)
    
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def __repr__(self) -> str:
        return f"<Scores(session={self.session_id}, stage={self.stage}, batch={self.batchs_counter})>"