
import logging
from numpy import np
from src.lib.calibration_stages import CalibrationStage
from uqm.uncertaintyQuantifier import UncertaintyQuantifier
from uqm.utils.utils import flatten_batch, get_coverage


class UtraceCalculator:
    def __init__(self):
        self.stage = CalibrationStage.INITIAL_CALIBRATION
        self.uq = UncertaintyQuantifier(classes=np.arange(10))
        self.alphas_ = []
        self.U_ = []
        self.alphas = []
        self.alpha = None
        self.alpha_std = None
        self.U_std = None
        self.Us = []
        self._build_state()
        self.batch_coverages, self.batch_setsizes = [], []

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


    def build_prediction_sets(self, probs, force_non_empty_sets=False):
        if self.stage != CalibrationStage.PREDICTION_SET_CONSTRUCTION:
            raise ValueError("Prediction set construction can only be performed in the PREDICTION_SET_CONSTRUCTION stage.")
        
        y_p, y_s = self.uq.build_prediction_sets(probs, force_non_empty_sets=force_non_empty_sets)
        y_n = flatten_batch(y_n).ravel()#.astype(int)
        self.batch_coverages.append(get_coverage(y_n.numpy(), y_s))
        self.batch_setsizes.append(y_s.sum(axis=1).max())
    
    def update_stage(self, stage):

        if self.stage == CalibrationStage.UNCERTAINTY_ESTIMATION and stage == CalibrationStage.PREDICTION_SET_CONSTRUCTION:
            self.alphas = np.array(self.alphas_)
            self.Us = np.array(self.U_)
            self.alpha = np.nanmean(self.alphas)
            self.alpha_std = np.nanstd(self.alphas)
            U = np.nanmean(self.Us)
            self.U_std = np.nanstd(self.Us)
            self.uq.alpha = U

        if self.stage == CalibrationStage.FINISHED:
            self.coverage = np.array(self.batch_coverages).mean()
            self.setsize = np.array(self.batch_setsizes).max()


            logging.info("Calibration process is already finished. No further stage updates allowed.")
            logging.info("Final alpha: %.4f ± %.4f", self.alpha, self.alpha_std)
            logging.info("Final U: %.4f ± %.4f", self.uq.alpha, self.U_std)
            logging.info("Final coverage: %.4f", self.coverage)
            logging.info("Final set size: %d", self.setsize)

        self.stage = stage 

        