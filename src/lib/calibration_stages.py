
from enum import IntEnum


class CalibrationStage(IntEnum):
    INITIAL_CALIBRATION = 1
    UNCERTAINTY_ESTIMATION = 2
    PREDICTION_SET_CONSTRUCTION = 3
    FINISHED = 4

    @classmethod
    def from_int(cls, value: int):
        return cls(value)

    def __str__(self):
        return self.name

    
