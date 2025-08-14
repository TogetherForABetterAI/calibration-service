import os
import logging

def initialize_config():
    config_params = {}

    # Email Configuration
    config_params["email_sender"] = os.getenv("EMAIL_SENDER", "")
    config_params["email_password"] = os.getenv("EMAIL_PASSWORD", "")
    # gRPC Configuration
    config_params["grpc_port"] = int(os.getenv("GRPC_PORT", "50051"))

    config_params["artifacts_path"] = os.getenv("ARTIFACTS_PATH", "artifacts")
    config_params["experiment_name"] = os.getenv("MLFLOW_EXPERIMENT_NAME", "Global Calibration Experiment")

    # Application Configuration
    config_params["logging_level"] = os.getenv("LOGGING_LEVEL", "INFO")
    
    config_params["rabbitmq_host"] = os.getenv("RABBITMQ_HOST", "rabbitmq")
    config_params["rabbitmq_port"] = int(os.getenv("RABBITMQ_PORT", "5672"))
    

    return config_params

config_params = initialize_config()