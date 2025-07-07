from router import Router
from lib.connection import setup_rabbitmq
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

middleware = setup_rabbitmq()
router = Router(middleware)
router.start()
