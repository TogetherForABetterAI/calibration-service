"""Conformal predictor wrapper.
"""

import logging
from typing import Literal, Union

import numpy as np

from utrace.scores import aps, aps_cal, lac, lac_cal

logger = logging.getLogger(__name__)


class UncertaintyQuantifier:
    """Uncertainty quantification using U-TraCE.

    Parameters
    ----------
    classes : Union[list[int], np.ndarray, None], optional
        List or array of class labels
    score : Literal['lac','aps'], optional
        The scoring function to use, by default 'lac'
    """
    def __init__(self,
                 classes:Union[list[int], np.ndarray, None]=None,
                 score:Literal['lac','aps']='lac'):
        self.classes = classes
        match score:
            case 'lac':
                self.cal_score_ = lac_cal
                self.score_ = lac
            case 'aps':
                self.cal_score_ = aps_cal
                self.score_ = aps
            case _:
                self.cal_score_ = lac_cal
                self.score_ = lac
        self.reset()


    def reset(self, conformity_scores_:np.ndarray=np.empty(0)):
        """Resets the scoores and alpha."""
        self.conformity_scores_ = conformity_scores_
        self.__q_hat:np.float64 = np.float64('nan')
        self.__alpha:np.float64 = np.float64('nan')
        self._class_scores:list[np.ndarray] = [np.empty(0) for _ in self.classes] if self.classes is not None else []

        logger.debug("UQ reset.")


    @property
    def alpha(self) -> np.float64:
        """The alpha value used for the conformal prediction stage."""
        return self.__alpha
    
    @alpha.setter
    def alpha(self, alpha: np.float64):
        """Sets the alpha value and calculates the q_hat level based on the current conformity scores."""
        n = self.conformity_scores_.shape[0]
        if n == 0:
            raise ValueError("The model must be calibrated before setting alpha.")
        
        q_level = np.divide(np.ceil((n + 1) * (1 - alpha)), n, dtype=np.float64)
        if q_level > 1.0:
            logger.warning("'q_level' > 1.0, setting to 1.0 - Scores size: %d (< 1/alpha???) - alpha %f", n, alpha)
            q_level = np.float64(1.0)
        self.__alpha = np.float64(alpha)
        self.__q_hat = np.nanquantile(self.conformity_scores_, q_level, method='higher')
        logger.debug("'q_hat' set to %f for alpha %f", self.__q_hat, self.__alpha)     
    

    def calibrate(self, y_probs, y_true, batched:bool=False):
        """Calibrates the conformal predictor with the given data.

        Parameters
        ----------
        y_probs : np.ndarray
            Predicted probabilities for each class.
        y_true : np.ndarray
            True labels for calibration.
        batched : bool, optional
            For batched calibration; concatenates new scores with previous ones. By default False
        """
        
        logger.debug('Fitting with %d samples', len(y_true))

        if batched:
            # If batched calibration, we need to concatenate the conformity scores for each batch
            self.conformity_scores_ = np.concatenate([self.conformity_scores_,
                                                      self.cal_score_(y_true, y_probs)])
        else:
            self.conformity_scores_ = self.cal_score_(y_true, y_probs)
        
        logger.debug("Conformity scores shape: %s", self.conformity_scores_.shape)
        
        # Classes
        if self.classes is not None:
            for c_idx, C in enumerate(self.classes):
                logger.debug("Calibrating for class %d", C)
                if batched:
                    self._class_scores[c_idx] = np.sort(
                                                np.concatenate([self._class_scores[c_idx],
                                                                self.cal_score_(y_true[y_true==C], y_probs[y_true==C])]))
                else:
                    self._class_scores[c_idx] = np.sort(self.cal_score_(y_true[y_true==C], y_probs[y_true==C]))
                if self._class_scores[c_idx].size == 0:
                    logger.warning("No scores for class %d after calibration.", C)
            self.conformity_scores_ = np.sort(np.concatenate(self._class_scores))
            logger.debug("Total conformity scores shape after class calibration: %s", self.conformity_scores_.shape) 


    def build_prediction_sets(self, y_probs: np.ndarray, force_non_empty_sets: bool = False) -> tuple[np.ndarray, np.ndarray]:
        """Builds prediction sets based on the calibrated q_hat level.

        Parameters
        ----------
        y_probs: np.ndarray
            Predicted probabilities for each class (Batch, Classes).
        force_non_empty_sets : bool, optional
            If True, ensures that the predicted class is included in the set, by default False.

        Returns
        -------
        y_sets : np.ndarray
            The sets of labels as a boolean array.
        """
        y_pred = np.argmax(y_probs, axis=1)

        scores = self.score_(y_probs)
        logging.info("Building prediction sets with q_hat: %f", self.__q_hat)
        y_sets = scores <= self.__q_hat
        
        if force_non_empty_sets:
            y_sets[np.arange(len(y_pred)), y_pred] = True

        return y_sets


    def get_uncertainty_opt(self, y_pred, y_true) -> tuple[np.float64, np.float64]:
        """Calculates the overall uncertainty of the model predictions.
        
        This method uses a intelligent grid search-like approach to find the optimal alpha value
        that yields the minimum upper bound for model uncertatinty.
        
        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities for each class.
        y_true : np.ndarray
            Target labels for prediction.
        Returns
        -------
        U, alpha : float
            The uncertainty of the model predictions and the alpha of the CP found.
        """

        if self.classes is not None:
            K = len(y_pred)
            
        N = len(self.conformity_scores_)
        Ns = len(y_true)

        if self.classes is not None:
            valid_indexes = np.isin(y_true, np.array(self.classes))  #type: ignore
        else:
            valid_indexes = np.ones(len(y_true), dtype=bool)

        best_alpha = np.float64('nan')
        
        max_lower_bound = np.float64(0.0) # This represents P(y=y_t), or 1 - U

        for j,score in enumerate(self.conformity_scores_):
            
            q_hat = score
            alpha = 1 - (j + 1) / (N + 1)

            prediction_sets = (y_pred >= (1 - q_hat))

            is_covered = prediction_sets[np.arange(Ns), y_true]
        
            success_indices = np.where(is_covered)[0]
            failure_indices = np.where(~is_covered)[0]
        
            n_succ = len(success_indices)
            n_fail = len(failure_indices)

            # --- p1_hat: E[1/k | success] ---
            p1_hat = np.float64(0.0)
            if n_succ > 0:
                set_sizes_succ = prediction_sets[success_indices].sum(axis=1)
                p1_hat = np.mean(1.0 / set_sizes_succ)

            # --- p2_hat: E[(1/K)δ_k,0 | fail] ---
            # This is not used since the assumption of random fail does not hold in practice (*)
            p2_hat = np.float64(0.0)
            if n_fail > 0:
                set_sizes_fail = prediction_sets[failure_indices].sum(axis=1)
                n_fail_empty = np.sum(set_sizes_fail == 0)
                p2_hat = (1.0 / K) * (n_fail_empty / n_fail)

            lower_bound = p1_hat * (1 - alpha)  # + p2_hat * (alpha - 1/(N + 1)) (*)

            # Update bound and alpha if better
            if lower_bound > max_lower_bound:
                max_lower_bound = lower_bound
                best_alpha = np.float64(alpha)


        self.alpha = best_alpha

        logger.debug("Best alpha: %f - Min upper uncertainty bound: %f\n", best_alpha, 1-max_lower_bound)
        return 1-max_lower_bound, best_alpha
