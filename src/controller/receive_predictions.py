from fastapi import APIRouter
from src.schemas.predictions import Predictions
from src.service.service import calculate_error_rate
from src.service.mlflow_logger import batch_logger

router = APIRouter()

@router.post("", status_code=200)
def receive_predictions(predictions: Predictions):
    print(f"Received predictions: {predictions}")
    new_alpha = calculate_error_rate(predictions.alpha, predictions.probabilities, predictions.label)
    
    batch_logger.log_batch(predictions.probabilities)

    if predictions.eof:  # <- chequeo de finalización
        path = batch_logger.finalize()
        return {"message": "Finished logging", "alpha": new_alpha, "file": path}

    return {"alpha": new_alpha}
