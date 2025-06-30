from fastapi import APIRouter

from controller.receive_predictions import router as receive_predictions_router


router = APIRouter()


router.include_router(receive_predictions_router, prefix="/probs", tags=["predictions"])
