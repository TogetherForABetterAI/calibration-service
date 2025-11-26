from enum import Enum


class SessionStatus(Enum):
    TIMEOUT = 1
    COMPLETED = 2
    IN_PROGRESS = 3

    def name(self):
        if self == SessionStatus.TIMEOUT:
            return "TIMEOUT"
        elif self == SessionStatus.COMPLETED:
            return "COMPLETED"
        elif self == SessionStatus.IN_PROGRESS:
            return "IN_PROGRESS"
        