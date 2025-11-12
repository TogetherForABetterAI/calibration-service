from ast import List
import json
from multiprocessing import Queue
import time
from typing import Union
import pytest
import threading
from unittest.mock import Mock, patch
from src.lib.config import CONNECTION_QUEUE_NAME
from src.lib.logger import initialize_logging
from src.proto import calibration_pb2, dataset_pb2
from tests.mocks.fake_middleware import FakeMiddleware
from src.server.main import Server
import numpy as np

from tests.mocks.fake_report_builder import FakeReportBuilder

@pytest.fixture
def mock_global_config():
    server_config = Mock(
        service_name="calibration-service",
        container_name="calibration-service-container",
        upper_bound_clients=10,
        lower_bound_clients=3,
        replica_id=1,
        replica_timeout_seconds=5,
        master_replica_id=None,
        initial_timeout=5
    )
    
    middleware_config = Mock(
        host="rabbitmq",
        port=5672,
        username="guest",
        password="guest",
        max_retries=3
    )
    
    fake_global_config = Mock(
        server_config=server_config,
        middleware_config=middleware_config,
        log_level="INFO"
    )
    
    return fake_global_config


@pytest.fixture
def mock_cm_middleware(mock_global_config):
    probs: Union[List[float], np.ndarray] = [0.005, 0.9, 0.005, 0.005, 0.005, 0.005, 0.005, 0.06, 0.01, 0.005]

    pred = calibration_pb2.Predictions()
    pred_list = calibration_pb2.PredictionList()
    pred_list.values.extend(probs)
    pred.pred.append(pred_list)

    pred.batch_index = 1
    pred.eof = True

    fake_image = np.random.rand(1, 28, 28).astype(np.float32)
    inputs = dataset_pb2.DataBatch()
    inputs.batch_index = 1
    inputs.is_last_batch = True
    inputs.data = fake_image.tobytes()
    inputs.labels.extend([1])

    return FakeMiddleware(config=mock_global_config.middleware_config, messages=
        {"labeled_queue": [inputs.SerializeToString()],
        "replies_queue": [pred.SerializeToString()]}, processing_delay=0.1, module="client_manager")

def test_integration_shutdown_runs_without_errors(mock_global_config, mock_cm_middleware):
    """Prueba de integración para verificar que el shutdown se ejecute sin errores usando FakeMiddleware."""
   
    middleware_queue = Queue()
   
    notification = [b'{"client_id": "client-001"}', b'{"client_id": "client-002"}', b'{"invalid_json": "missing_closing_bracket"}']
    fake_listener_middleware = FakeMiddleware(config=mock_global_config.middleware_config, messages={CONNECTION_QUEUE_NAME: notification}, queue=middleware_queue, module="listener")

    initialize_logging(mock_global_config.log_level.upper())

    def middleware_factory_fake(*args, **kwarg):
        return mock_cm_middleware
    
    fake_report_builder = FakeReportBuilder()
    def report_builder_factory(*args, **kwarg):
        return fake_report_builder

    server = Server(mock_global_config, middleware_cls=fake_listener_middleware, cm_middleware_factory=middleware_factory_fake, report_builder_factory=report_builder_factory)
    server_thread = threading.Thread(target=server.run)
    server_thread.start()
    
    print("Waiting for middleware to finish processing...")

    _ = middleware_queue.get()    # Espera hasta que el middleware llame a stop_consuming()
    time.sleep(1) 
    server.handle_sigterm() 
    server_thread.join() 
    
    assert fake_listener_middleware.close_connection_called == True
    assert fake_listener_middleware.is_running() == False
    assert len(fake_listener_middleware.msg_ack) == 3
    assert fake_listener_middleware.msg_ack[0] == b'{"client_id": "client-001"}'
    assert fake_listener_middleware.msg_ack[1] == b'{"client_id": "client-002"}'
    assert fake_listener_middleware.msg_ack[2] == b'{"invalid_json": "missing_closing_bracket"}'
    assert fake_listener_middleware.stop_consuming_called == True
    assert fake_listener_middleware.close_channel_called == True

def test_exceed_upper_bound_clients(mock_global_config, mock_cm_middleware):
    """Prueba de integración para verificar que el servidor maneje el límite superior de clientes."""
    notifications = []
    for i in range(mock_global_config.server_config.upper_bound_clients):
        notifications.append(f'{{"client_id": "client-{i:03d}"}}'.encode('utf-8'))
        
    middleware_queue = Queue()
    
    fake_listener_middleware = FakeMiddleware(config=mock_global_config.middleware_config, messages={CONNECTION_QUEUE_NAME: notifications}, queue=middleware_queue, module="listener")

    initialize_logging(mock_global_config.log_level.upper())

    def middleware_factory_fake(*args, **kwarg):
        return mock_cm_middleware
    
    def report_builder_factory(*args, **kwarg):
        return FakeReportBuilder()

    server = Server(mock_global_config, middleware_cls=fake_listener_middleware, cm_middleware_factory=middleware_factory_fake, report_builder_factory=report_builder_factory)
    server_thread = threading.Thread(target=server.run)
    server_thread.start()
    
    print("Waiting for middleware to finish processing...")
    _ = middleware_queue.get()   
    time.sleep(1)
    server.handle_sigterm() 

    server_thread.join() 
    
    assert len(fake_listener_middleware.messages_sent) == 1
    exchange_name, routing_key, message_body = fake_listener_middleware.messages_sent[0]
    assert exchange_name == "coordinator_exchange"
    assert routing_key == "scale.up"
    assert json.loads(message_body)["replica_id"] == mock_global_config.server_config.replica_id
    assert json.loads(message_body)["action"] == "scale_up"
    assert fake_listener_middleware.clients_handled_until_stop_consuming == mock_global_config.server_config.upper_bound_clients
    assert fake_listener_middleware.clients_handled_after_stop_consuming == 0

    
def test_exceed_upper_bound_clients_and_continue_handling_connections(mock_global_config, mock_cm_middleware):
    """Prueba de integración para verificar que el servidor maneje el límite superior de clientes."""
    notifications = []
    for i in range(mock_global_config.server_config.upper_bound_clients + 5):
        notifications.append(f'{{"client_id": "client-{i:03d}"}}'.encode('utf-8'))
        
    middleware_queue = Queue()
    
    fake_listener_middleware = FakeMiddleware(config=mock_global_config.middleware_config, messages={CONNECTION_QUEUE_NAME: notifications}, queue=middleware_queue, module="listener")

    initialize_logging(mock_global_config.log_level.upper())

    def middleware_factory_fake(*args, **kwarg):
        return mock_cm_middleware
    
    def report_builder_factory(*args, **kwarg):
        return FakeReportBuilder()

    server = Server(mock_global_config, middleware_cls=fake_listener_middleware, cm_middleware_factory=middleware_factory_fake, report_builder_factory=report_builder_factory)
    server_thread = threading.Thread(target=server.run)
    server_thread.start()
    
    print("Waiting for middleware to finish processing...")
    _ = middleware_queue.get()   
    time.sleep(2)
    server.handle_sigterm() 

    server_thread.join() 
    
    assert len(fake_listener_middleware.messages_sent) >= 1
    exchange_name, routing_key, message_body = fake_listener_middleware.messages_sent[0]
    assert exchange_name == "coordinator_exchange"
    assert routing_key == "scale.up"
    assert json.loads(message_body)["replica_id"] == mock_global_config.server_config.replica_id
    assert json.loads(message_body)["action"] == "scale_up"
    assert fake_listener_middleware.clients_handled_until_stop_consuming == mock_global_config.server_config.upper_bound_clients
    assert fake_listener_middleware.clients_handled_after_stop_consuming == 5
    

def test_does_not_reach_lower_bound_clients(mock_global_config, mock_cm_middleware):
    """Prueba de integración para verificar que el servidor no intente escalar si no se alcanza el límite inferior de clientes."""
    notifications = []
    for i in range(mock_global_config.server_config.lower_bound_clients - 1):
        notifications.append(f'{{"client_id": "client-{i:03d}"}}'.encode('utf-8'))
        
    middleware_queue = Queue()
    
    fake_listener_middleware = FakeMiddleware(config=mock_global_config.middleware_config, messages={CONNECTION_QUEUE_NAME: notifications}, queue=middleware_queue, module="listener")

    initialize_logging(mock_global_config.log_level.upper())

    def middleware_factory_fake(*args, **kwarg):
        return mock_cm_middleware
    
    def report_builder_factory(*args, **kwarg):
        return FakeReportBuilder()

    server = Server(mock_global_config, middleware_cls=fake_listener_middleware, cm_middleware_factory=middleware_factory_fake, report_builder_factory=report_builder_factory)
    server_thread = threading.Thread(target=server.run)
    server_thread.start()
    
    _ = middleware_queue.get()   
    server_thread.join() 
    
    assert len(fake_listener_middleware.messages_sent) == 0
    assert fake_listener_middleware.clients_handled_until_stop_consuming == mock_global_config.server_config.lower_bound_clients - 1
    assert fake_listener_middleware.clients_handled_after_stop_consuming == 0

