from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NewClientRequest(_message.Message):
    __slots__ = ("client_id", "routing_key", "outputs_queue_calibration", "inputs_queue_calibration")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTING_KEY_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_QUEUE_CALIBRATION_FIELD_NUMBER: _ClassVar[int]
    INPUTS_QUEUE_CALIBRATION_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    routing_key: str
    outputs_queue_calibration: str
    inputs_queue_calibration: str
    def __init__(self, client_id: _Optional[str] = ..., routing_key: _Optional[str] = ..., outputs_queue_calibration: _Optional[str] = ..., inputs_queue_calibration: _Optional[str] = ...) -> None: ...

class NewClientResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class HealthCheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...
