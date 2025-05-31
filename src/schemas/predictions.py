from pydantic import BaseModel
from typing import List

class Predictions(BaseModel):
    label: int
    probabilities: List[float]
    alpha: float
    eof: bool  

    
