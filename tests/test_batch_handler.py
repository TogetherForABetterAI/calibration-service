import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.lib.data_types import DataType
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
    handler._mlflow_logger = Mock()

    data = np.zeros((1, 28, 28))
    handler._store_data(0, DataType.INPUTS, data, eof=False)
    handler._store_data(0, DataType.PROBS, data, eof=False)
    handler._store_data(0, DataType.LABELS, np.array([1]), eof=True)

    assert 0 in handler._batches
    handler._mlflow_logger.log_single_batch.assert_called_once()

def test_process_input_data_valid(handler):
    """Verifica que _process_input_data convierta bytes a np.array con forma (n,1,28,28)."""
    dummy = np.random.rand(28 * 28).astype(np.float32).tobytes()
    out = handler._process_input_data(dummy)
    assert out.shape == (1, 1, 28, 28)

def test_process_input_data_invalid_size(handler):
    """Verifica que _process_input_data lance ValueError si el tamaño no coincide."""
    dummy = np.random.rand(10).astype(np.float32).tobytes()
    with pytest.raises(ValueError):
        handler._process_input_data(dummy)


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
