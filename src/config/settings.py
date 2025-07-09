import os
from typing import Optional


class Settings:
    """Application settings and configuration."""

    # RabbitMQ Configuration
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "rabbitmq")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASS: str = os.getenv("RABBITMQ_PASS", "guest")

    # gRPC Configuration
    GRPC_PORT: int = int(os.getenv("GRPC_PORT", "50051"))

    # MLflow Configuration
    MLFLOW_TRACKING_URI: Optional[str] = os.getenv("MLFLOW_TRACKING_URI")
    MLFLOW_EXPERIMENT_NAME: str = os.getenv(
        "MLFLOW_EXPERIMENT_NAME", "Calibration Experiment for MNIST"
    )

    # Application Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    ARTIFACTS_PATH: str = os.getenv("ARTIFACTS_PATH", "artifacts")

    # Legacy queue names (for backward compatibility)
    PROB_OUTPUTS_QUEUE_NAME: str = "calibration_outputs_queue"
    DATA_INPUTS_QUEUE_NAME: str = "calibration_inputs_queue"
    EXCHANGE_TYPE: str = "direct"
    EXCHANGE_NAME: str = "calibration_exchange"


# Global settings instance
settings = Settings()
