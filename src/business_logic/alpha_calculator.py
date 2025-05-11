

class AlphaCalculator:
    def __init__(self):
        self.user_alphas = {}
        
    def contains(self, user_id: str) -> bool:
        return user_id in self.user_alphas
        
    def add(self, user_id: str, alpha: float):
        if user_id in self.user_alphas:
            raise ValueError(f"User ID {user_id} already exists.")
        
        self.user_alphas[user_id] = alpha

    def update(self, user_id: str, alpha: float):
        if user_id not in self.user_alphas:
            raise ValueError(f"User ID {user_id} does not exist.")
        
        self.user_alphas[user_id] = alpha
        
    def calculate_alpha(self, user_id: str) -> float:
        #TODO: Implementar esta función
        new_alpha = self.user_alphas.get(user_id, 0) + 0.1
        self.update(user_id, new_alpha)
        return new_alpha