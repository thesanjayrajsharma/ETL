"""
Base classes for data validators
"""

from abc import ABCMeta, abstractmethod


class _BaseValidator(metaclass=ABCMeta):
    """Base class for all validators"""

    def validate(self, data):
        """Validate the data"""
        return self._validate(data)

    @abstractmethod
    def _validate(self, data):
        """Validate the data"""
        pass
