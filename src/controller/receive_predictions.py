from fastapi import APIRouter
from src.schemas.predictions import Predictions
from src.service.service import calculate_error_rate

router = APIRouter()


@router.post("", status_code=200)
def receive_predictions(predictions: Predictions):

    print(f"Received predictions: {predictions.probabilities}")
    new_alpha = calculate_error_rate(predictions.alpha, predictions.probabilities, predictions.label)
    return {"alpha": new_alpha}