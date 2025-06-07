from fastapi import APIRouter, status
from src.schemas.predictions import Predictions
from src.service.service import calculate_error_rate
from src.service.mlflow_logger import batch_logger

router = APIRouter()


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Predictions received and logged successfully"},
        400: {"description": "Invalid input data"},
    },
)
def receive_predictions(predictions: Predictions):
    print(f"Received predictions: {predictions}")
    # new_alpha = calculate_error_rate(predictions.alpha, predictions.probabilities, predictions.label)

    batch_logger.log_batch(predictions.probabilities)

    if predictions.eof:  # <- chequeo de finalización
        batch_logger.finalize()

