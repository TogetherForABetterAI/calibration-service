import pytest
import signal
from unittest.mock import patch, Mock, MagicMock
from src.server.client_manager import ClientManager


@pytest.fixture
def mock_middleware():
    return Mock()

@pytest.fixture
def client_manager(mock_middleware):
    def report_builder_factory(client_id: str):
        return Mock()
    return ClientManager(client_id="client123", middleware=mock_middleware, clients_to_remove_queue=None, report_builder=report_builder_factory(client_id="client123"))


def test_initialization(client_manager):
    """Verifica que el ClientManager se inicializa correctamente."""
    assert client_manager.client_id == "client123"
    assert not client_manager.shutdown_initiated
    assert client_manager.middleware is not None
    assert client_manager.clients_to_remove_queue is None


def test_handle_shutdown_signal_stops_all(client_manager):
    """Verifica que _handle_shutdown_signal detiene consumer y batch_handler."""
    client_manager.consumer = Mock()
    client_manager.batch_handler = Mock()

    client_manager._handle_shutdown_signal(None, None)

    assert client_manager.shutdown_initiated is True
    client_manager.consumer.handle_sigterm.assert_called_once()
    client_manager.batch_handler.handle_sigterm.assert_called_once()


@patch("src.server.client_manager.signal.signal")
@patch("src.server.client_manager.BatchHandler")
@patch("src.server.client_manager.Consumer")
def test_run_success(MockConsumer, MockBatchHandler, mock_signal, client_manager):
    """Simula una ejecución exitosa del proceso."""
    mock_consumer = Mock()
    MockConsumer.return_value = mock_consumer
    mock_batch = Mock()
    MockBatchHandler.return_value = mock_batch

    client_manager.clients_to_remove_queue = Mock()

    client_manager.run()

    MockBatchHandler.assert_called_once()
    MockConsumer.assert_called_once()
    mock_consumer.start.assert_called_once()
    client_manager.clients_to_remove_queue.put.assert_called_once_with("client123")


@patch("src.server.client_manager.signal.signal")
@patch("src.server.client_manager.BatchHandler")
@patch("src.server.client_manager.Consumer")
def test_run_with_exception(MockConsumer, MockBatchHandler, mock_signal, client_manager):
    """Verifica que si algo falla en run, se loguea el error y no lanza excepción."""
    MockBatchHandler.side_effect = Exception("boom")
    client_manager.logger = Mock()

    client_manager.run()

    client_manager.logger.error.assert_called_once()
    assert "boom" in client_manager.logger.error.call_args[0][0]


def test_handle_predictions_message_calls_batchhandler(client_manager):
    """Verifica que _handle_predictions_message invoque correctamente el handler."""
    mock_batch = Mock()
    client_manager.batch_handler = mock_batch
    client_manager._handle_predictions_message(None, None, None, b"xyz")
    mock_batch._handle_predictions_message.assert_called_once_with(None, b"xyz")


def test_handle_EOF_message_stops_processing(client_manager):
    """Verifica que _handle_EOF_message detenga batch_handler y consumer."""
    client_manager.batch_handler = Mock()
    client_manager.consumer = Mock()

    client_manager._handle_EOF_message()

    client_manager.batch_handler.handle_sigterm.assert_called_once()
    client_manager.consumer.handle_sigterm.assert_called_once()
