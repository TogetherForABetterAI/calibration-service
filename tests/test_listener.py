import pytest
import json
import threading
from unittest.mock import Mock, patch, MagicMock
from src.server.listener import Listener


@pytest.fixture
def mock_middleware():
    middleware = Mock()
    middleware.config = {"mock": "config"}
    return middleware


@pytest.fixture
def mock_channel():
    return Mock()

def cm_middleware_factory(config):
    return Mock()

def report_builder_factory(user_id: str):
    return Mock()

def mock_config():
    config = Mock()
    config.client_timeout_seconds = 30
    config.lower_bound_clients = 2
    config.upper_bound_clients = 5
    config.replica_timeout_seconds = 60
    config.initial_timeout = 10
    config.replica_id = 1
    config.master_replica_id = 1
    return config

def utrace_calculator_factory(database=None, session_id=None):
    return Mock()

@pytest.fixture
def listener(mock_middleware, mock_channel):
    return Listener(middleware=mock_middleware, 
                    channel=mock_channel, 
                    cm_middleware_factory=cm_middleware_factory, 
                    report_builder_factory=report_builder_factory,
                    utrace_calculator_factory=utrace_calculator_factory,
                    config=mock_config())



def test_listener_initialization(listener):
    """Verifica que el Listener se inicializa correctamente."""
    assert isinstance(listener.channel, Mock)
    assert isinstance(listener.config, Mock)
    assert isinstance(listener._active_clients, dict)
    assert not listener.shutdown_initiated
    assert listener.clients_to_remove_queue is not None


def test_add_and_remove_client(listener):
    """Verifica que los métodos _add_client y _remove_client funcionan correctamente."""
    mock_client = Mock()
    user_id = "client-123"

    listener._add_client(user_id, mock_client, Mock(), Mock())
    assert user_id in listener._active_clients

    listener._remove_client(user_id)
    assert user_id not in listener._active_clients


@patch("src.server.listener.parse_inputs_format")
def test_handle_new_client_success(mock_parse_format, listener):
    """Simula recibir una notificación válida y verifica creación de ClientManager."""
    with patch("src.server.listener.ClientManager") as MockClientManager:
        mock_manager = Mock()
        MockClientManager.return_value = mock_manager
        
        notification = {
            "user_id": "client-001", 
            "session_id": "session-abc",
            "inputs_format": "RAW",
            "email": "test@test.com"
        }
        body = json.dumps(notification).encode("utf-8")
        
        mock_method = Mock()
        mock_method.delivery_tag = 123

        mock_ch = Mock()

        listener._handle_new_client(mock_ch, mock_method, None, body)

        MockClientManager.assert_called_once()
        mock_manager.start.assert_called_once()
        assert "client-001" in listener._active_clients


def test_handle_new_client_missing_id(listener):
    """Verifica que si falta user_id o session_id, se lance ValueError."""
    with patch("src.server.listener.ClientManager") as MockClientManager:
        notification = {"foo": "bar"} # Falta user_id y session_id
        body = json.dumps(notification).encode("utf-8")

        with pytest.raises(ValueError, match="Client notification missing fields"):
            listener._handle_new_client(None, None, None, body)
        
        MockClientManager.assert_not_called()

def test_monitor_removals_removes_client(listener):
    """Verifica que el monitor elimina clientes cuando llegan IDs a la queue."""
    user_id = "client-xyz"
    mock_client = Mock()
    
    listener._add_client(user_id, mock_client, Mock(), 99)

    listener.clients_to_remove_queue.put(user_id)
    listener.clients_to_remove_queue.put(None)

    thread = threading.Thread(target=listener._monitor_removals)
    thread.start()
    thread.join(timeout=2)

    assert user_id not in listener._active_clients
