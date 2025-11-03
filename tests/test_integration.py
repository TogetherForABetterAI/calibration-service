from ast import List
import json
import time
from typing import Union
import pytest
import signal
import threading
import builtins
from unittest.mock import Mock, patch, call
from src.lib.config import CONNECTION_QUEUE_NAME, GlobalConfig, ServerConfig, initialize_config
from src.lib.logger import initialize_logging
from src.main import (
    main,
)
from src.middleware.middleware import Middleware
from src.proto import calibration_pb2, dataset_pb2
from tests.mocks.fake_middleware import FakeMiddleware
from src.server.main import Server
import numpy as np

from tests.mocks.fake_report_builder import FakeReportBuilder


def test_integration_main_runs_without_errors():
    """Prueba de integración para verificar que main se ejecute sin errores usando FakeMiddleware."""
    server_config = Mock(
        service_name="calibration-service",
        container_name="calibration-service-container",
        upper_bound_clients=10,
        lower_bound_clients=3,
        replica_id=1,
        replica_timeout_seconds=180,
        master_replica_id=1,
        initial_timeout=30
    )
    
    middleware_config = Mock(
        host="rabbitmq",
        port=5672,
        username="guest",
        password="guest",
        max_retries=3
    )
    
    mlflow_config = Mock(
        artifacts_path="artifacts",
        experiment_name="Global Calibration Experiment",
        tracking_uri="http://mlflow:5000"
    )
    
    fake_global_config = Mock(
        server_config=server_config,
        middleware_config=middleware_config,
        mlflow_config=mlflow_config,
        log_level="INFO"
    )
    
    notification = [b'{"client_id": "client-001"}', b'{"client_id": "client-002"}', b'{"invalid_json": "missing_closing_bracket"}']
    fake_listener_middleware = FakeMiddleware(config=middleware_config, messages={CONNECTION_QUEUE_NAME: notification})

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
    inputs.data = fake_image.tobytes()  # convierte a bytes compatibles con np.frombuffer
    inputs.labels.extend([1])

    fake_client_manager_middleware = FakeMiddleware(config=middleware_config, messages=
        {"labeled_queue": [inputs.SerializeToString()],
        "replies_queue": [pred.SerializeToString()]})
    

    initialize_logging(fake_global_config.log_level.upper())

    def middleware_factory_fake(*args, **kwarg):
        return fake_client_manager_middleware
    
    def mlflow_logger_factory(*args, **kwarg):
        return Mock()
    
    fake_report_builder = FakeReportBuilder()
    def report_builder_factory(*args, **kwarg):
        return fake_report_builder

    server = Server(fake_global_config, middleware_cls=fake_listener_middleware, cm_middleware_factory=middleware_factory_fake, mlflow_logger_factory=mlflow_logger_factory, report_builder_factory=report_builder_factory)
    server.run()
    
    
    assert fake_listener_middleware.close_connection_called == True
    assert fake_listener_middleware.start_consuming_called == True
    assert fake_listener_middleware.is_running() == False
    assert len(fake_listener_middleware.msg_ack) == 3
    assert fake_listener_middleware.msg_ack[0] == b'{"client_id": "client-001"}'
    assert fake_listener_middleware.msg_ack[1] == b'{"client_id": "client-002"}'
    assert fake_listener_middleware.msg_ack[2] == b'{"invalid_json": "missing_closing_bracket"}'