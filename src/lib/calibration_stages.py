

from enum import Enum


class CalibrationStage(Enum):
    INITIAL_CALIBRATION = 1
    UNCERTAINTY_ESTIMATION = 2
    PREDICTION_SET_CONSTRUCTION = 3
    FINISHED = 4