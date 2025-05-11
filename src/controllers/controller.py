from fastapi import FastAPI
from business_logic.schemas import ProbabilisticOutputs, AlphaResponse

app = FastAPI()
alpha_calculator = AlphaCalculator()

@app.post("/users/{user_id}/outputs")
async def get_output(user_id: str, probabilistic_outputs: ProbabilisticOutputs):
    """
    Recibo el user_id para después guardarlo en alguna estructura.
    Devuelvo el nuevo parámetro alpha
    """
    if not alpha_calculator.contains(user_id):
        alpha_calculator.add(user_id, 0.1)
        
    alpha = alpha_calculator.calculate_alpha(user_id)
    return AlphaResponse(alpha=alpha)
    