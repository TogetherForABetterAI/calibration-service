from ast import List
import time
from typing import Union
import pytest
import signal
import threading
import builtins
from unittest.mock import Mock, patch, call
from src.lib.config import CONNECTION_QUEUE_NAME, GlobalConfig, ServerConfig
from src.main import (
    setup_logging,
    main,
)
from src.proto import calibration_pb2, dataset_pb2
from tests.mocks.fake_middleware import FakeMiddleware
from src.server.main import Server
import numpy as np

def test_setup_logging_invokes_initialize_logging():
    """Verifica que setup_logging llame correctamente a initialize_logging."""
    with patch("src.main.initialize_logging") as mock_init:
        mock_cfg = Mock(log_level="debug")
        setup_logging(mock_cfg)
        mock_init.assert_called_once_with("DEBUG")


@patch("src.main.start_service_with_graceful_shutdown")
@patch("src.main.Middleware")
@patch("src.main.Server")
@patch("src.main.initialize_config")
@patch("src.main.setup_logging")
def test_main_starts_service(mock_setup_logging, mock_init_config, mock_server, mock_middleware_cls, mock_start_service):
    """Verifica que main ejecute correctamente el flujo principal."""
    fake_config = Mock()
    fake_config.middleware_config = Mock()
    fake_middleware = Mock()

    mock_server_instance = Mock()
    mock_server.return_value = mock_server_instance
    mock_middleware_cls.return_value = fake_middleware
    mock_init_config.return_value = fake_config

    mock_start_service.return_value = None


    main()
    mock_init_config.assert_called_once()
    mock_setup_logging.assert_called_once_with(fake_config)
    mock_middleware_cls.assert_called_once_with(fake_config.middleware_config)
    mock_start_service.assert_called_once_with(mock_server_instance, fake_config)

@patch("src.main.initialize_config")
@patch("src.main.Middleware")
@patch("src.server.client_manager.Middleware")
@patch("src.server.batch_handler.MlflowClient")
@patch("src.main.threading.Event")
def test_integration_main_runs_without_errors(mock_event, mock_mlflow_client_cls, mock_client_manager_middleware_cls, mock_listener_middleware_cls, mock_init_config):
    """Prueba de integración para verificar que main se ejecute sin errores usando FakeMiddleware."""
    
    server_config = Mock(
        service_name="calibration-service",
        container_name="calibration-service-container"
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
    mock_init_config.return_value = fake_global_config

    fake_listener_middleware = FakeMiddleware(config=middleware_config, messages={CONNECTION_QUEUE_NAME: ["{\"client_id\": \"client-001\"}", "{\"client_id\": \"client-002\"}", "{\"invalid_json\": \"missing_closing_bracket\"}"]})
    mock_listener_middleware_cls.return_value = fake_listener_middleware

    probs: Union[List[float], np.ndarray] = [0.005, 0.9, 0.005, 0.005, 0.005, 0.005, 0.005, 0.06, 0.01, 0.005]

    pred = calibration_pb2.Predictions()
    pred_list = calibration_pb2.PredictionList()
    pred_list.values.extend(probs)
    pred.pred.append(pred_list)

    pred.batch_index = 1
    pred.eof = True

    inputs = dataset_pb2.DataBatch()
    inputs.batch_index = 1
    inputs.is_last_batch = True
    inputs.data = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\xdac\xf8\x0f\x00\x01\x01\x01\x00\x18\xdd\x03\xdd\x00\x00\x00\x00IEND\xaeB`\x82'
    inputs.labels.extend([1])

    fake_client_manager_middleware = FakeMiddleware(config=middleware_config, messages=
        {"labeled_queue": [inputs.SerializeToString()],
         "replies_queue": [pred.SerializeToString()]})
    
    mock_client_manager_middleware_cls.return_value = fake_client_manager_middleware

    mock_mlflow_client = Mock()
    mock_mlflow_client_cls.return_value = mock_mlflow_client
    mock_event.return_value = Mock()

    server = Server(fake_global_config, middleware_cls=fake_listener_middleware)
    server.start()
    time.sleep(1)
    server.stop()
    server.join()
    