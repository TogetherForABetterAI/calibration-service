import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.server.batch_handler import BatchHandler

def report_builder_factory(client_id: str):
    return Mock()   

@pytest.fixture
def handler():
    return BatchHandler(client_id="client1", report_builder=report_builder_factory(client_id="client1"), on_eof=Mock(), middleware=Mock())


def test_initialization(handler):
    """Verifica la inicialización correcta de BatchHandler."""
    assert handler.client_id == "client1"
    assert handler._batches == {}


def test_store_data_creates_entry(handler):
    """Verifica que _store_data almacene correctamente los datos y loguee batch completo."""
    probs = np.array([[0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]])
    labels = np.array([1, 0])
    handler._store_data(0, probs, labels, eof=False)

    assert 0 in handler._batches


@patch("src.server.batch_handler.calibration_pb2.Predictions")
def test_handle_probability_message(MockPredictions, handler):
    """Verifica que _handle_probability_message procese correctamente un mensaje."""
    mock_msg = Mock()
    mock_msg.pred = [Mock(values=[0.1, 0.9])]
    mock_msg.batch_index = 1
    mock_msg.eof = True
    MockPredictions.return_value = mock_msg

    handler._store_data = Mock()
    handler._send_report = Mock()
    handler._on_eof = Mock()
    ch = Mock()

    handler._handle_probability_message(ch, b"body")

    mock_msg.ParseFromString.assert_called_once_with(b"body")
    handler._store_data.assert_called_once()
