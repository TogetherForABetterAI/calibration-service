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


@pytest.fixture
def listener(mock_middleware, mock_channel):
    return Listener(middleware=mock_middleware, channel=mock_channel)


def test_listener_initialization(listener):
    """Verifica que el Listener se inicializa correctamente."""
    assert isinstance(listener.channel, Mock)
    assert listener.middleware_config == {"mock": "config"}
    assert isinstance(listener._active_clients, dict)
    assert not listener.shutdown_initiated
    assert listener.remove_client_queue is not None


def test_add_and_remove_client(listener):
    """Verifica que los métodos _add_client y _remove_handler funcionan correctamente."""
    mock_client = Mock()
    client_id = "client-123"

    listener._add_client(client_id, mock_client)
    assert client_id in listener._active_clients

    listener._remove_handler(client_id)
    assert client_id not in listener._active_clients


def test_handle_new_client_success(listener):
    """Simula recibir una notificación de cliente válida y verifica que se crea un ClientManager."""
    with patch("src.server.listener.ClientManager") as MockClientManager:
        mock_manager = Mock()
        MockClientManager.return_value = mock_manager

        notification = {"client_id": "client-001"}
        body = json.dumps(notification).encode("utf-8")

        listener._handle_new_client(None, None, None, body)

        MockClientManager.assert_called_once()
        mock_manager.start.assert_called_once()
        assert "client-001" in listener._active_clients


def test_handle_new_client_missing_id(listener):
    """Verifica que si falta el client_id, no se lanza un proceso."""
    with patch("src.server.listener.ClientManager") as MockClientManager:
        notification = {"foo": "bar"}
        body = json.dumps(notification).encode("utf-8")

        listener._handle_new_client(None, None, None, body)
        MockClientManager.assert_not_called()

def test_monitor_removals_removes_client(listener):
    """Verifica que el monitor elimina clientes cuando llegan IDs a la queue."""
    client_id = "client-xyz"
    mock_client = Mock()
    listener._add_client(client_id, mock_client)

    # Agregamos una tarea de eliminación y una señal de fin
    listener.remove_client_queue.put(client_id)
    listener.remove_client_queue.put(None)

    thread = threading.Thread(target=listener._monitor_removals)
    thread.start()
    thread.join(timeout=1)

    assert client_id not in listener._active_clients


def test_shutdown_calls_all_methods(listener):
    """Verifica que el shutdown cierra conexiones y termina procesos."""
    listener.remove_client_monitor = Mock()
    listener.remove_client_monitor.join = Mock()

    listener.middleware.close_channel = Mock()
    listener.middleware.close_connection = Mock()

    listener._shutdown_all_clients = Mock()

    listener.shutdown()

    listener._shutdown_all_clients.assert_called_once()
    listener.middleware.close_channel.assert_called_once_with(listener.channel)
    listener.middleware.close_connection.assert_called_once()


def test_shutdown_all_clients_terminates(listener):
    """Verifica que los ClientManagers activos sean terminados correctamente."""
    mock_client1 = Mock()
    mock_client1.is_alive.return_value = True
    listener._active_clients["client-a"] = mock_client1

    mock_client2 = Mock()
    mock_client2.is_alive.return_value = False
    listener._active_clients["client-b"] = mock_client2

    listener._shutdown_all_clients()

    mock_client1.terminate.assert_called_once()
    mock_client1.join.assert_called_once()
    mock_client2.terminate.assert_not_called()
    mock_client2.join.assert_not_called()

def test_stop_consuming_sets_flag(listener):
    """Verifica que stop_consuming marca el shutdown y llama al método del middleware."""
    listener.middleware.stop_consuming = Mock()
    listener.stop_consuming()
    listener.middleware.stop_consuming.assert_called_once()
