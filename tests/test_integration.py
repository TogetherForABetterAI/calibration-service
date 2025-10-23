import pytest
import signal
import threading
import builtins
from unittest.mock import Mock, patch, call
from src.main import (
    setup_logging,
    signal_handler,
    start_service_with_graceful_shutdown,
    main,
)


@pytest.fixture
def mock_config():
    """Config simulado con los atributos mínimos necesarios."""
    cfg = Mock()
    cfg.log_level = "info"
    cfg.server_config.service_name = "dataset-generator"
    return cfg


@pytest.fixture
def mock_server():
    """Servidor simulado con métodos start, stop y join."""
    srv = Mock()
    srv.start = Mock()
    srv.stop = Mock()
    srv.join = Mock()
    return srv


def test_setup_logging_invokes_initialize_logging():
    """Verifica que setup_logging llame correctamente a initialize_logging."""
    with patch("src.main.initialize_logging") as mock_init:
        mock_cfg = Mock(log_level="debug")
        setup_logging(mock_cfg)
        mock_init.assert_called_once_with("DEBUG")


@patch("src.main.start_service_with_graceful_shutdown")
@patch("src.main.Server")
@patch("src.main.initialize_config")
@patch("src.main.setup_logging")
def test_main_starts_service(mock_setup_logging, mock_init_config, mock_server_cls, mock_start_service):
    """Verifica que main ejecute correctamente el flujo principal."""
    fake_config = Mock()
    mock_server_instance = Mock()
    mock_server_cls.return_value = mock_server_instance
    mock_init_config.return_value = fake_config

    main()

    mock_init_config.assert_called_once()
    mock_setup_logging.assert_called_once_with(fake_config)
    mock_server_cls.assert_called_once_with(fake_config)
    mock_start_service.assert_called_once_with(mock_server_instance, fake_config)
