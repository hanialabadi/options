"""Doctrine layer — owns all thresholds and RAG citations."""

from ._rule import DoctrineRule, GraduatedRule
from . import income_doctrine, directional_doctrine, volatility_doctrine

__all__ = [
    "DoctrineRule",
    "GraduatedRule",
    "income_doctrine",
    "directional_doctrine",
    "volatility_doctrine",
]
