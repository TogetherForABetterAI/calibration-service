import pytest
import numpy as np
from unittest.mock import MagicMock, call, ANY

# Ajusta estos imports según la ruta real de tu proyecto
from src.server.utrace_calculator import UtraceCalculator
from src.lib.calibration_stages import CalibrationStage
from src.lib.data_types import DataType

CALIB_LIMIT_MOCK = 2
UNCERTAINTY_LIMIT_MOCK = 4 

@pytest.fixture
def mock_db():
    """Mock de la base de datos."""
    db = MagicMock()
    db.get_latest_scores_record.return_value = None
    return db

@pytest.fixture
def mock_uq(mocker):
    """Mock de la clase UncertaintyQuantifier."""
    uq_class = mocker.patch('src.server.utrace_calculator.UncertaintyQuantifier')
    uq_instance = uq_class.return_value
    
    uq_instance.get_uncertainty_opt.return_value = (0.5, 0.1) # U, alpha
    uq_instance.build_prediction_sets.return_value = np.array([
        [0, 1], # Set para la muestra 1
        [1, 0]  # Set para la muestra 2
    ])
    return uq_instance

@pytest.fixture
def calculator(mock_db, mock_uq, mocker):
    """Instancia del calculador con límites de configuración parcheados."""
    mocker.patch('src.server.utrace_calculator.CALIBRATION_LIMIT', CALIB_LIMIT_MOCK)
    mocker.patch('src.server.utrace_calculator.UNCERTAINTY_LIMIT', UNCERTAINTY_LIMIT_MOCK)
    
    calc = UtraceCalculator(database=mock_db, user_id=1, session_id="sess_123")
    return calc

@pytest.fixture
def sample_entry():
    """Genera datos de entrada dummy."""
    return {
        DataType.PROBS: np.array([[0.2, 0.8], [0.6, 0.4]]), 
        DataType.LABELS: np.array([1, 0])
    }


def test_initialization_new_session(calculator, mock_db):
    """Verifica el estado inicial cuando no hay datos previos en DB."""
    mock_db.get_latest_scores_record.assert_called_once_with(1, "sess_123")
    assert calculator.stage == CalibrationStage.INITIAL_CALIBRATION
    assert calculator.batch_counter == 0
    assert calculator.accuracy == 0.0

def test_initialization_restore_session(mock_db, mocker):
    """Verifica que se restauren los datos si la DB devuelve un registro."""
    # Simulamos un registro de DB
    record = MagicMock()
    record.batchs_counter = 10
    record.stage = CalibrationStage.PREDICTION_SET_CONSTRUCTION
    record.vec_scores = [0.9, 0.8]
    record.alphas = [0.1]
    record.uncertainties = [0.5]
    record.coverages = [0.95]
    record.setsizes = [2.0]
    record.correct_preds = 50
    record.total_samples = 60
    record.accuracy = 0.83
    record.scores = np.array([0.9, 0.8], dtype=np.float64).tobytes()
    record.alpha = 0.1
    record.confidences = np.array([0.9, 0.8], dtype=np.float64).tobytes()

    mock_db.get_latest_scores_record.return_value = record
    
    # Instanciamos manualmente para que tome el mock configurado
    calc = UtraceCalculator(mock_db, 1, "sess_res")
    
    assert calc.batch_counter == 10
    assert calc.stage == CalibrationStage.PREDICTION_SET_CONSTRUCTION
    assert np.array_equal(calc.uq.conformity_scores_, np.array([0.9, 0.8]))
    assert calc.alphas_ == [0.1]
    # Verifica conversión a int de setsizes
    assert calc.batch_setsizes == [2]
    assert calc.accuracy == 0.83

def test_phase_calibration(calculator, sample_entry, mock_uq):
    """Test de la etapa de Calibración (contador <= CALIBRATION_LIMIT)."""
    calculator.process_entry(sample_entry)
    
    mock_uq.calibrate.assert_called_once()
    mock_uq.get_uncertainty_opt.assert_not_called()
    mock_uq.build_prediction_sets.assert_not_called()
    
    assert calculator.batch_counter == 1
    assert calculator.stage == CalibrationStage.INITIAL_CALIBRATION

def test_transition_to_uncertainty(calculator, sample_entry):
    """Test transición de INITIAL_CALIBRATION -> UNCERTAINTY_ESTIMATION."""
    # Avanzamos el contador hasta el límite de calibración
    calculator.batch_counter = CALIB_LIMIT_MOCK 
    
    calculator.process_entry(sample_entry)
    
    assert calculator.stage == CalibrationStage.UNCERTAINTY_ESTIMATION
    assert calculator.batch_counter == CALIB_LIMIT_MOCK + 1

def test_phase_uncertainty_estimation(calculator, sample_entry, mock_uq):
    """Test etapa de Estimación de Incertidumbre."""
    calculator.batch_counter = CALIB_LIMIT_MOCK + 1
    calculator.stage = CalibrationStage.UNCERTAINTY_ESTIMATION
    
    calculator.process_entry(sample_entry)
    
    mock_uq.get_uncertainty_opt.assert_called_once()
    assert len(calculator.alphas_) == 1
    assert len(calculator.U_) == 1
    
    args, _ = calculator._db.update_session_state.call_args
    updates = args[2] # El tercer argumento es 'updates'
    assert 'push_alphas' in updates
    assert 'push_uncertainties' in updates

def test_transition_to_prediction(calculator, sample_entry):
    """Test transición de UNCERTAINTY_ESTIMATION -> PREDICTION_SET_CONSTRUCTION."""
    calculator.batch_counter = UNCERTAINTY_LIMIT_MOCK
    calculator.stage = CalibrationStage.UNCERTAINTY_ESTIMATION
    
    calculator.process_entry(sample_entry)
    
    assert calculator.stage == CalibrationStage.PREDICTION_SET_CONSTRUCTION

def test_phase_prediction(calculator, sample_entry, mock_uq):
    """Test etapa final (Producción)."""
    calculator.batch_counter = UNCERTAINTY_LIMIT_MOCK + 1
    calculator.stage = CalibrationStage.PREDICTION_SET_CONSTRUCTION
    
    calculator.process_entry(sample_entry)
    assert calculator.total_samples == 2 
    assert calculator.correct_preds >= 0
    assert calculator.accuracy > 0
    
    mock_uq.build_prediction_sets.assert_called_once()
    
    assert len(calculator.batch_coverages) == 1
    assert len(calculator.batch_setsizes) == 1
    assert len(calculator.stored_confidences) == 1
    
    args, _ = calculator._db.update_session_state.call_args
    updates = args[2]
    
    assert 'push_confidences' in updates
    assert isinstance(updates['push_confidences'], bytes) 
    assert 'accuracy' in updates
    assert 'push_coverages' in updates

def test_accuracy_math(calculator):
    """Prueba unitaria específica para _update_accuracy_stats."""
    probs = np.array([
        [0.1, 0.9], 
        [0.8, 0.2], 
        [0.6, 0.4] 
    ])
    labels = np.array([1, 0, 1])
    
    calculator._update_accuracy_stats(probs, labels)
    
    assert calculator.correct_preds == 2
    assert calculator.total_samples == 3
    assert calculator.accuracy == pytest.approx(2/3)
    
    assert len(calculator.stored_confidences) == 1
    np.testing.assert_array_almost_equal(
        calculator.stored_confidences[0], 
        np.array([0.9, 0.8, 0.6])
    )

def test_get_calibration_results_fails_wrong_stage(calculator):
    """Verifica que lance error si no está en estado FINISHED."""
    with pytest.raises(ValueError, match="Calibration results can only be retrieved"):
        calculator.get_calibration_results()