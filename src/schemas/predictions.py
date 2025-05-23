from pydantic import BaseModel
from typing import List, Tuple, Union

class Predictions(BaseModel):
    data_prob_pairs: List[Tuple[str, float]]