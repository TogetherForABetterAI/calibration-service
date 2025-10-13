import os


class ServerConfig:
    def __init__(self):
        self.service_name = os.getenv("SERVICE_NAME", "calibration-service")
        self.container_name = os.getenv("CONTAINER_NAME", "")


class MiddlewareConfig:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username = os.getenv("RABBITMQ_USER", "guest")
        self.password = os.getenv("RABBITMQ_PASS", "guest")
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))


class MlflowConfig:
    def __init__(self):
        self.artifacts_path = os.getenv("ARTIFACTS_PATH", "artifacts")
        self.experiment_name = os.getenv(
            "MLFLOW_EXPERIMENT_NAME", "Global Calibration Experiment"
        )
        self.tracking_uri = os.getenv("TRACKING_URI_MLFLOW", "http://mlflow:5000")


class GlobalConfig:
    def __init__(self):
        self.server_config = ServerConfig()
        self.middleware_config = MiddlewareConfig()
        self.mlflow_config = MlflowConfig()
        self.log_level = os.getenv("LOGGING_LEVEL", "INFO")


def initialize_config():
    return GlobalConfig()
