import logging

def initialize_logging(logging_level: str):
    logging.basicConfig(
        level=getattr(logging, logging_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

