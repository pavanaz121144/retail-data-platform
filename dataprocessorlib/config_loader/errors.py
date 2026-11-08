from typing import List


class ConfigValidationError(Exception):
    """Raised once with EVERY problem found, not just the first."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__("Invalid pipeline config:\n  - " + "\n  - ".join(errors))
