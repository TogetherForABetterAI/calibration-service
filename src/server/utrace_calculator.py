import logging
import numpy as np
from typing import List, Optional, Dict, Any
from src.lib.calibration_stages import CalibrationStage
from src.lib.config import CALIBRATION_LIMIT, UNCERTAINTY_LIMIT
from src.lib.data_types import DataType
from utrace.uncertaintyQuantifier import UncertaintyQuantifier
from utrace.utils.utils import flatten_batch, get_coverage

class UtraceCalculator:
    def __init__(self, database, session_id):
        self._db = database
        self._session_id = session_id
        
        # Estado Inicial
        self.stage = CalibrationStage.INITIAL_CALIBRATION
        self.uq = UncertaintyQuantifier(classes=np.arange(10))  
        self.batch_counter = 0

        # Métricas en Memoria
        self.alphas_: List[float] = []
        self.U_: List[float] = []
        self.batch_coverages: List[float] = []
        self.batch_setsizes: List[int] = []
        
        # Estadísticas acumuladas
        self.correct_preds = 0
        self.total_samples = 0
        self.accuracy = 0.0
        self.stored_confidences = []

        self.restore_session()

    def restore_session(self):
        """Carga el estado desde la DB para tolerancia a fallos."""

        record = self._db.get_latest_scores_record(self._session_id)
        if not record:
            self._db.create_scores_record(self._session_id)
            return

        # Restaurar contadores y stage
        self.batch_counter = record.batchs_counter or 0
        self.stage = CalibrationStage.from_int(record.stage or 0)

        # Restaurar variables del uq
        if record.scores is not None:
            scores = np.frombuffer(record.scores, dtype=np.float64) if record.scores else np.empty(0)
            self.uq.reset(conformity_scores_=scores)

        if record.alpha is not None:
            alpha = record.alpha
            self.uq.alpha = alpha if alpha is not None else np.float64('nan')
            
        # Restaurar listas 
        self.alphas_ = record.alphas if record.alphas else []
        self.U_ = record.uncertainties if record.uncertainties else []
        self.batch_coverages = record.coverages if record.coverages else []
        self.batch_setsizes = [int(x) for x in record.setsizes] if record.setsizes else []
        self.stored_confidences = [np.frombuffer(record.confidences, dtype=np.float64)] if record.confidences else []

        # Restaurar estadísticas acumuladas
        self.correct_preds = record.correct_preds or 0
        self.total_samples = record.total_samples or 0
        self.accuracy = record.accuracy or 0.0

        logging.info(f"Restoring values: batch_counter={self.batch_counter}, stage={self.stage}, accuracy={self.accuracy} scores={scores if record.scores is not None else None}, alpha={alpha if record.alpha is not None else None}, alphas={self.alphas_}, uncertainties={self.U_}, coverages={self.batch_coverages}, setsizes={self.batch_setsizes}, correct_preds={self.correct_preds}, total_samples={self.total_samples}, confidences={self.stored_confidences}, correct_preds={self.correct_preds}, total_samples={self.total_samples}, accuracy={self.accuracy})")

    def process_entry(self, entry: Dict[DataType, Any]):
        probs = entry[DataType.PROBS]
        labels = entry[DataType.LABELS]
        
        current_metrics = {}

        if self.batch_counter <= CALIBRATION_LIMIT:
            self.uq.calibrate(probs, labels, batched=True)
            current_metrics['scores'] =  self.uq.conformity_scores_.astype(np.float64).tobytes()

            if self.batch_counter == CALIBRATION_LIMIT:
                self.update_stage(CalibrationStage.UNCERTAINTY_ESTIMATION)

        elif self.batch_counter <= UNCERTAINTY_LIMIT:
            U, alpha = self.uq.get_uncertainty_opt(probs, labels)
            current_metrics
            self.alphas_.append(float(alpha))
            self.U_.append(float(U))
            current_metrics = {'alpha': float(alpha), 'uncertainty': float(U)}
            
            if self.batch_counter == UNCERTAINTY_LIMIT:
                self.update_stage(CalibrationStage.PREDICTION_SET_CONSTRUCTION)

        else:
            confidences_curr_batch = self._update_accuracy_stats(probs, labels) # Refactorizado abajo
            y_s = self.uq.build_prediction_sets(probs, force_non_empty_sets=False)
            cov = get_coverage(labels, y_s)
            size = int(y_s.sum(axis=1).max())
            
            self.batch_coverages.append(cov)
            self.batch_setsizes.append(size)
            
            current_metrics = {
                'confidences': confidences_curr_batch.astype(np.float64).tobytes(),
                'coverage': cov, 
                'setsize': size,
                'accuracy': self.accuracy,
                'correct_preds': self.correct_preds,
                'total_samples': self.total_samples
            }

        self._persist_batch_state(current_metrics)
        self.batch_counter += 1

    def _persist_batch_state(self, metrics: dict):
        """
        Guarda los resultados del batch actual y actualiza el contador en una sola transacción.
        Esto hace que el sistema sea tolerante a fallos.
        """
        updates = {
            "batchs_counter": self.batch_counter + 1,
            "stage": self.stage
        }
        if 'scores' in metrics:
            updates['scores'] = metrics['scores']

        if 'alpha' in metrics:
            updates['push_alphas'] = metrics['alpha'] 
            updates['alpha'] = metrics['alpha']
            updates['push_uncertainties'] = metrics['uncertainty']
            updates['uncertainties'] = metrics['uncertainty']
        
        if 'coverage' in metrics:
            updates['push_confidences'] = metrics['confidences']
            updates['push_coverages'] = metrics['coverage']
            updates['push_setsizes'] = metrics['setsize']
            updates['accuracy'] = metrics['accuracy']
            updates['correct_preds'] = metrics['correct_preds']
            updates['total_samples'] = metrics['total_samples']

        self._db.update_session_state(self._session_id, updates)

    def _update_accuracy_stats(self, probs, labels):
        """Calcula accuracy y guarda confidencias."""
        preds = np.argmax(probs, axis=1)
        labels_flat = labels.ravel()
        confidences = np.max(probs, axis=1)
        
        self.correct_preds += int(np.sum(preds == labels_flat))
        self.total_samples += len(labels_flat)
        self.accuracy = self.correct_preds / self.total_samples if self.total_samples > 0 else 0.0
        
        self.stored_confidences.append(confidences)

        return confidences

    def update_stage(self, new_stage):
        logging.info(f"Transitioning: {self.stage.name} -> {new_stage.name}")
        self.stage = new_stage
        
    def get_calibration_results(self):
        if self.stage != CalibrationStage.FINISHED:
            raise ValueError("Calibration results can only be retrieved in the FINISHED stage.")
        
        all_confidences = np.concatenate(self.stored_confidences) if self.stored_confidences else np.array([])
        setsize = np.array(self.batch_setsizes).max()
        alphas = np.array(self.alphas_)
        alpha = np.nanmean(alphas)
        alpha_std = np.nanstd(alphas)
        Us = np.array(self.U_)
        U = np.nanmean(Us)
        U_std = np.nanstd(Us)
        coverage = np.array(self.batch_coverages).mean()
        
        results = {
            "metrics": {
                "Accuracy": self.accuracy,  
                "Model Uncertainty Upper Bound": U,
                "Empirical Coverage": coverage,
                "Max Set Size (Worst case scenario)": setsize, 
                "Alpha": alpha
            },
            "history": {
                "alphas": self.alphas_,
                "uncertainty": self.U_,
                "batch_coverage": self.batch_coverages,
                "batch_setsizes": self.batch_setsizes
            },
            "raw_data": {
                "confidences": all_confidences
            },
            "parameters": {
                "alpha_std": alpha_std,
                "U_std": U_std,
            }
        }
        return results