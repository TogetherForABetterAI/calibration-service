
import logging
from re import match
import numpy as np
from src.lib.calibration_stages import CalibrationStage
from src.lib.data_types import DataType
from utrace.uncertaintyQuantifier import UncertaintyQuantifier
from utrace.utils.utils import flatten_batch, get_coverage


class UtraceCalculator:
    def __init__(self, database, user_id, session_id):
        self._db = database
        self._user_id = user_id
        self._session_id = session_id
        self._scores = self._db.get_scores_from_session(session_id)
        self.stage = CalibrationStage.INITIAL_CALIBRATION
        self.uq = UncertaintyQuantifier(classes=np.arange(10))
        self.alphas_ = []
        self.U_ = []
        self.alphas = []
        self.alpha = None
        self.alpha_std = None
        self.U_std = None
        self.U = None
        self.Us = []
        self.batch_coverages, self.batch_setsizes = [], []
        self.correct_preds = 0
        self.total_samples = 0
        self.accuracy = 0.0
        self.stored_confidences = []
        self.counter = 0

    def _update_accuracy(self, probs, labels):
        preds = np.argmax(probs, axis=1)
        labels = labels.ravel()
        confidences = np.max(probs, axis=1)
        
        self.correct_preds += np.sum(preds == labels)
        self.total_samples += len(labels)
        self.stored_confidences.append(confidences)

    def process_entry(self, entry):
        # Define thresholds
        CALIBR_LIMIT = 10
        UNCERT_LIMIT = 20


        # Handle logic ranges
        match self.counter:
            case x if x <= CALIBR_LIMIT:
                self.calibrate(entry[DataType.PROBS], entry[DataType.LABELS])
            case x if x <= UNCERT_LIMIT:
                self.calculate_uncertainty(entry[DataType.PROBS], entry[DataType.LABELS])
            case _:
                self.build_prediction_sets(entry[DataType.PROBS], entry[DataType.LABELS], force_non_empty_sets=False)

        # Handle transitions explicitly
        match self.counter:
            case x if x == CALIBR_LIMIT:
                self.update_stage(CalibrationStage.UNCERTAINTY_ESTIMATION)
            case x if x == UNCERT_LIMIT:
                self.update_stage(CalibrationStage.PREDICTION_SET_CONSTRUCTION)

        self.counter += 1

    def calibrate(self, probs, labels):
        if self.stage != CalibrationStage.INITIAL_CALIBRATION:
            raise ValueError("Calibration can only be performed in the INITIAL_CALIBRATION stage.")
        
        self.uq.calibrate(probs, labels)

    def calculate_uncertainty(self, probs, labels):
        if self.stage != CalibrationStage.UNCERTAINTY_ESTIMATION:
            raise ValueError("Uncertainty estimation can only be performed in the UNCERTAINTY_ESTIMATION stage.")
        
        U, alpha = self.uq.get_uncertainty_opt(probs, labels)
        self.alphas_.append(alpha)
        self.U_.append(U)


    def build_prediction_sets(self, probs, labels, force_non_empty_sets=False):
        if self.stage != CalibrationStage.PREDICTION_SET_CONSTRUCTION:
            raise ValueError("Prediction set construction can only be performed in the PREDICTION_SET_CONSTRUCTION stage.")
        
        self._update_accuracy(probs, labels)

        y_p, y_s = self.uq.build_prediction_sets(probs, force_non_empty_sets=force_non_empty_sets)
        self.batch_coverages.append(get_coverage(labels, y_s))
        self.batch_setsizes.append(y_s.sum(axis=1).max())

    def update_calibration_stats(self):
        self.alphas = np.array(self.alphas_)
        self.alpha = np.nanmean(self.alphas)
        self.alpha_std = np.nanstd(self.alphas)
        self.Us = np.array(self.U_)
        self.U = np.nanmean(self.Us)
        self.U_std = np.nanstd(self.Us)

    def update_last_stage_stats(self):
        self.coverage = np.array(self.batch_coverages).mean()
        self.setsize = np.array(self.batch_setsizes).max()
        if self.total_samples > 0:
            self.accuracy = self.correct_preds / self.total_samples
        
        logging.info("Calibration finished.")

    def update_stage(self, stage):
        if stage != self.stage + 1:
            raise ValueError("Cannot revert to a previous calibration stage.")

        match stage:
            case CalibrationStage.PREDICTION_SET_CONSTRUCTION:
                self.update_calibration_stats()
            case CalibrationStage.FINISHED:
                self.update_last_stage_stats()

        logging.info(f"Transitioning from stage: {self.stage.name} to stage: {stage.name}")
        self.stage = stage 

        
    def get_calibration_results(self):
        if self.stage != CalibrationStage.FINISHED:
            raise ValueError("Calibration results can only be retrieved in the FINISHED stage.")
        
        all_confidences = np.concatenate(self.stored_confidences) if self.stored_confidences else np.array([])

        results = {
            "metrics": {
                "Accuracy": self.accuracy,
                "Optimal Alpha (Risk)": self.alpha,
                "Model Uncertainty (U)": self.U,
                "Empirical Coverage": self.coverage,
                "Max Set Size": self.setsize
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
                "alpha_std": self.alpha_std,
                "U_std": self.U_std,
            }
        }
        return results