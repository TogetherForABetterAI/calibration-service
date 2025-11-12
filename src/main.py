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
    
    def report_builder_factory(client_id: str):
        from src.server.batch_handler import ReportBuilder
        return ReportBuilder(client_id=client_id, email_sender=config.email_sender, email_password=config.email_password)

    server = Server(config, middleware_cls=middleware, cm_middleware_factory=middleware_factory, report_builder_factory=report_builder_factory)
    server.run()
    

if __name__ == "__main__":
    main()
