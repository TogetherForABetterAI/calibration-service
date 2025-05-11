from pydantic import BaseModel


class ClassProbability(BaseModel):
    name: str  
    probability: float  
    
class ProbabilisticOutputs(BaseModel):
    items: List[ClassProbability]
    
class AlphaResponse(BaseModel):
    alpha: float