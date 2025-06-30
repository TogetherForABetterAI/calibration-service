from random import random

class Session:
    
    def __init__(self, alpha: float = 0.5, run_id: str = None):
        self.alpha = alpha
        self.run_id = run_id
        
    def calculate_error_rate(
        curr_alpha: float, probabilities: list[float], label: int
    ) -> float:
        return random()
    
    