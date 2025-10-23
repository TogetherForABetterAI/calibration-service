import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.server.batch_handler import BatchHandler
from lib.data_types import DataType


@pytest.fixture
def mock_mlflow_client():
    return Mock()


@pytest.fixture
def handler(mock_mlflow_client):
    with patch("src.server.batch_handler.MlflowLogger") as MockLogger:
        instance = MockLogger.return_value
        return BatchHandler(client_id="client1", mlflow_client=mock_mlflow_client, on_eof=Mock())


def test_initialization(handler):
    """Verifica la inicialización correcta de BatchHandler."""
    assert handler.client_id == "client1"
    assert handler._labeled_eof is False
    assert handler._replies_eof is False
    assert handler._batches == {}


def test_stop_processing_calls_end_run(handler):
    """Verifica que stop_processing invoque end_run en el logger."""
    handler._mlflow_logger = Mock()
    handler.stop_processing()
    handler._mlflow_logger.end_run.assert_called_once()


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


@patch("src.server.batch_handler.dataset_pb2.DataBatch")
def test_handle_data_message_parsing(MockDataBatch, handler):
    """Verifica que _handle_data_message procese correctamente un mensaje."""
    mock_msg = Mock()
    mock_msg.data = b"0" * (28 * 28 * 4)
    mock_msg.batch_index = 0
    mock_msg.is_last_batch = True
    mock_msg.labels = [1, 0, 1]
    MockDataBatch.return_value = mock_msg

    handler.store_input_data = Mock()
    handler._send_report = Mock()
    handler._on_eof = Mock()

    handler._handle_data_message(b"body")

    mock_msg.ParseFromString.assert_called_once_with(b"body")
    handler.store_input_data.assert_called_once()
    assert handler._labeled_eof is True


@patch("src.server.batch_handler.calibration_pb2.Predictions")
def test_handle_probability_message(MockPredictions, handler):
    """Verifica que _handle_probability_message procese correctamente un mensaje."""
    mock_msg = Mock()
    mock_msg.pred = [Mock(values=[0.1, 0.9])]
    mock_msg.batch_index = 1
    mock_msg.eof = True
    MockPredictions.return_value = mock_msg

    handler.store_outputs = Mock()
    handler._send_report = Mock()
    handler._on_eof = Mock()

    handler._handle_probability_message(b"body")

    mock_msg.ParseFromString.assert_called_once_with(b"body")
    handler.store_outputs.assert_called_once()
    assert handler._replies_eof is True
