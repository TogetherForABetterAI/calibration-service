from router import Router
from lib.connection import setup_rabbitmq

middleware = setup_rabbitmq()
router = Router(middleware)
router.start()