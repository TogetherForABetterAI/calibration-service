import os
import signal
import sys
import logging
import threading
from server.main import Server
from lib.logger import initialize_logging
from lib.config import initialize_config
from src.middleware.middleware import Middleware


def main():
    config = initialize_config()
    initialize_logging(config.log_level.upper())

    middleware = Middleware(config.middleware_config)
    def middleware_factory(config):
        return Middleware(config=config)
    
    def mlflow_logger_factory(client_id: str):
        from src.service.mlflow_logger import MlflowLogger
        from src.server.client_manager import MlflowClient

        mlflow_client = MlflowClient()
        return MlflowLogger(mlflow_client=mlflow_client, client_id=client_id)

    server = Server(config, middleware_cls=middleware, cm_middleware_factory=middleware_factory, mlflow_client_factory=mlflow_logger_factory)
    server.run()
    

if __name__ == "__main__":
    main()
