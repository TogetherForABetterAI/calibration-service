import numpy as np
from typing import Optional, NamedTuple

class InputsFormat(NamedTuple):
    """Format specification for model inputs."""

    dtype: np.dtype
    shape: tuple[int, ...]
    
def parse_inputs_format(inputs_format_str: str) -> Optional[InputsFormat]:
    """
    Parse input format string into InputsFormat object.

    Args:
        inputs_format_str: String representation of shape, e.g., "(1, 224, 224, 3)"

    Returns:
        InputsFormat object with parsed dtype and shape, or None if empty

    Raises:
        ValueError: If the format string is invalid
    """
    if not inputs_format_str or not inputs_format_str.strip():
        return None

    if not (inputs_format_str.startswith("(") and inputs_format_str.endswith(")")):
        raise ValueError(f"Invalid shape format in: {inputs_format_str}")

    try:
        shape_str = inputs_format_str.strip("()")
        shape_parts = [part.strip() for part in shape_str.split(",") if part.strip()]

        if not shape_parts:
            raise ValueError("Empty shape not allowed")

        shape = tuple(map(int, shape_parts))

        if any(dim <= 0 for dim in shape):
            raise ValueError("All dimensions must be positive")

    except ValueError as e:
        raise ValueError(f"Invalid shape format in: {inputs_format_str}") from e

    return InputsFormat(shape=shape, dtype=np.float32)
