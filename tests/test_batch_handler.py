import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.lib.data_types import DataType
from src.server.batch_handler import BatchHandler

def report_builder_factory(client_id: str):
    return Mock()   

def db_mock():
    db = Mock()
    db.get_inputs_from_session.return_value = []
    db.get_outputs_from_session.return_value = []
    return db

@pytest.fixture
def handler():
    return BatchHandler(client_id="client1", session_id="session1", report_builder=report_builder_factory(client_id="client1"), on_eof=Mock(), middleware=Mock(), database=db_mock(), inputs_format=None)

def test_initialization(handler):
    """Verifica la inicialización correcta de BatchHandler."""
    assert handler.client_id == "client1"
    assert handler._batches == {}


def test_store_data_creates_entry(handler):
    data = np.zeros((1, 28, 28))
    probs = np.array([[0.1, 0.9]])
    labels = np.array([1])

    handler._store_data(0, DataType.INPUTS, data)
    handler._store_data(0, DataType.PROBS, probs)
    handler._store_data(0, DataType.LABELS, labels)

    assert 0 in handler._batches

@patch("src.server.batch_handler.calibration_pb2.Predictions")
def test_handle_predictions_message(MockPredictions, handler):
    """Verifica que _handle_predictions_message procese correctamente un mensaje."""
    mock_msg = Mock()
    mock_msg.pred = [Mock(values=[0.1, 0.9])]
    mock_msg.batch_index = 1
    mock_msg.eof = True
    MockPredictions.return_value = mock_msg

    handler._store_data = Mock()
    handler._send_report = Mock()
    handler._on_eof = Mock()
    ch = Mock()

    handler._handle_predictions_message(ch, b"body")

    mock_msg.ParseFromString.assert_called_once_with(b"body")
    handler._store_data.assert_called_once()
