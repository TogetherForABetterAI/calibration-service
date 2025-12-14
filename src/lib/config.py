import os

# RabbitMQ
REPLIES_EXCHANGE = "replies_exchange"
INPUTS_EXCHANGE = "inputs_exchange"
CONNECTION_EXCHANGE = "new_connections_exchange"
CONNECTION_QUEUE_NAME = "calibration_service_connections_queue"
COORDINATOR_EXCHANGE = "coordinator_exchange"
MLFLOW_EXCHANGE = "mlflow_exchange"
MLFLOW_ROUTING_KEY = "mlflow.key"
INPUTS_QUEUE_NAME = "inputs_cal_queue"
OUTPUTS_QUEUE_NAME = "outputs_cal_queue"
INTI_LOGO_PATH = "reports/img/inti_logo.png"
SIGNATURE_PATH = "reports/img/signature.jpg"
REPORTS_DIR = "reports/"
CALIBRATION_LIMIT = 10
UNCERTAINTY_LIMIT = 20

class ServerConfig:
    def __init__(self):
        self.pod_name = os.getenv("POD_NAME", "calibration-service") # or container name, but for k8s pods are better
        self.upper_bound_clients = int(os.getenv("UPPER_BOUND_CLIENTS", "100"))
        self.replica_id = int(os.getenv("REPLICA_ID", "1"))
        self.client_timeout_seconds = int(os.getenv("CLIENT_TIMEOUT_SECONDS", "60")) 


class MiddlewareConfig:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username = os.getenv("RABBITMQ_USER", "guest")
        self.password = os.getenv("RABBITMQ_PASSWORD", "guest")
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

class GlobalConfig:
    def __init__(self):
        self.server_config = ServerConfig()
        self.middleware_config = MiddlewareConfig()
        # self.mlflow_config = MlflowConfig()
        self.log_level = os.getenv("LOGGING_LEVEL", "INFO")
        self.email_sender = os.getenv("EMAIL_SENDER", "default_sender@example.com")
        self.email_password = os.getenv("EMAIL_PASSWORD", "default_password")
        postgres_user = os.getenv("POSTGRES_USER", "user")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "password")
        postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        postgres_db = os.getenv("POSTGRES_DB", "calibration_db")
        self.database_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        self.connections_service_url = os.getenv("CONNECTIONS_SERVICE_URL", "http://connection-service:8000")
        self.environment = os.getenv("ENVIRONMENT", "PRODUCTION")


def initialize_config():
    return GlobalConfig()
