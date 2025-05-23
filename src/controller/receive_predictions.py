from fastapi import APIRouter
from src.schemas.predictions import Predictions

router = APIRouter()


@router.post("", status_code=200)
def receive_predictions(predictions: Predictions):


    print(predictions.data_prob_pairs)
    # salto de linea
    print("\n")
    print("\n")

    return {"message": "Predictions received successfully"}