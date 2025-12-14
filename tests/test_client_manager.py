import time
import pytest
import signal
from unittest.mock import patch, Mock, MagicMock
from src.server.client_manager import ClientManager


@pytest.fixture
def mock_middleware():
    return Mock()

@pytest.fixture
def client_manager(mock_middleware):
    def report_builder_factory(user_id: str):
        return Mock()
    def utrace_calculator_factory(database=None, session_id=None):
        return Mock()
    return ClientManager(user_id="client123", session_id="session123", recipient_email="", middleware=mock_middleware, clients_to_remove_queue=None, config=Mock(client_timeout_seconds=30), report_builder=report_builder_factory(user_id="client123"), utrace_calculator_factory=utrace_calculator_factory, inputs_format=None)


def test_initialization(client_manager):
    """Verifica que el ClientManager se inicializa correctamente."""
    assert client_manager.user_id == "client123"
    assert client_manager.session_id == "session123"
    assert not client_manager.shutdown_initiated
    assert client_manager.middleware is not None
    assert client_manager.clients_to_remove_queue is None
    assert client_manager.report_builder is not None
    assert client_manager.inputs_format is None


def test_handle_shutdown_signal_stops_all(client_manager):
    """Verifica que _handle_shutdown_signal detiene consumer y batch_handler."""
    client_manager.consumer = Mock()
    client_manager.batch_handler = Mock()

    client_manager._handle_shutdown_signal(None, None)

    assert client_manager.shutdown_initiated is True
    client_manager.consumer.handle_sigterm.assert_called_once()
    client_manager.batch_handler.handle_sigterm.assert_called_once()


@patch("src.server.client_manager.get_engine")
@patch("src.server.client_manager.Database")
@patch("src.server.client_manager.signal.signal")
@patch("src.server.client_manager.BatchHandler")
@patch("src.server.client_manager.Consumer")
def test_run_success(MockConsumer, MockBatchHandler, mock_signal, MockDatabase, mock_get_engine, client_manager):
    """Simula una ejecución exitosa del proceso."""
    
    mock_consumer_instance = MockConsumer.return_value
    mock_batch_instance = MockBatchHandler.return_value
    
    client_manager.timeout_checker_handler = Mock()
    client_manager.clients_to_remove_queue = Mock()
    client_manager.config.database_url = "sqlite:///:memory:"

    client_manager.run()
    mock_get_engine.assert_called_once_with("sqlite:///:memory:")
    MockDatabase.assert_called_once()
    
    MockBatchHandler.assert_called_once()
    MockConsumer.assert_called_once()
    
    client_manager.timeout_checker_handler.start.assert_called_once()
    
    mock_consumer_instance.start.assert_called_once()
    
    client_manager.clients_to_remove_queue.put.assert_called_once_with(client_manager.user_id)


@patch("src.server.client_manager.get_engine")
@patch("src.server.client_manager.Database")
@patch("src.server.client_manager.signal.signal")
@patch("src.server.client_manager.BatchHandler")
@patch("src.server.client_manager.Consumer")
def test_run_with_exception(MockConsumer, MockBatchHandler, mock_signal, MockDatabase, mock_get_engine, client_manager):
    MockBatchHandler.side_effect = Exception("boom")
    client_manager.logger = Mock()
    client_manager.timeout_checker_handler = Mock() 

    client_manager.run()

    client_manager.logger.error.assert_called()
    assert "boom" in client_manager.logger.error.call_args_list[0][0][0]


def test_handle_predictions_message_calls_batchhandler(client_manager):
    mock_batch = Mock()
    client_manager.batch_handler = mock_batch
    
    mock_ch = Mock()
    mock_method = Mock()
    
    client_manager._handle_predictions_message(mock_ch, mock_method, None, b"xyz")
    
    mock_batch._handle_predictions_message.assert_called_once_with(mock_ch, b"xyz")
    mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)


@patch("requests.put")
def test_handle_EOF_message_stops_processing(mock_put, client_manager):
    """Verifica que _handle_EOF_message detenga batch_handler y consumer."""
    client_manager.batch_handler = Mock()
    client_manager.consumer = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_put.return_value = mock_response
    client_manager._handle_EOF_message()

    client_manager.batch_handler.handle_sigterm.assert_called_once()
    client_manager.consumer.handle_sigterm.assert_called_once()
    
    
def test_timeout_triggers_status_update():
    config_mock = Mock()
    config_mock.server_config.client_timeout_seconds = 0.1
    config_mock.database_url = "sqlite:///:memory:"

    manager = ClientManager(
        user_id="123",
        session_id="abc",
        middleware=None,
        clients_to_remove_queue=None,
        config=config_mock,
        report_builder=None,
        utrace_calculator_factory=lambda database=None, session_id=None: Mock()
    )
    
    manager._initiate_shutdown = Mock()

    with patch("requests.put") as mock_put:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response
        
        manager.timeout_checker_handler.start()
        time.sleep(0.3)
        manager.shutdown_initiated = True
        manager.timeout_checker_handler.join()
        
        mock_put.assert_called()
        args, kwargs = mock_put.call_args
        assert "/sessions/abc/status/timeout" in args[0]