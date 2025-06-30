from fastapi import FastAPI
from routes import router as api_router
import mlflow

app = FastAPI()

# Include the API router
app.include_router(api_router)

# mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Calibration Experiment for MNIST")

# Register custom exception handlers
# configure_exception_handlers(app)
