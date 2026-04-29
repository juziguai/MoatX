"""Macro event intelligence package.

This package is the foundation for event-driven opportunity discovery:
collect news, extract event signals, update event states, and generate
explainable A-share opportunities.
"""

from .models import (
    EventDefinition,
    EventOpportunity,
    EventSignal,
    EventSource,
    EventState,
    NewsItem,
    TransmissionEffect,
)

__all__ = [
    "EventDefinition",
    "EventOpportunity",
    "EventSignal",
    "EventSource",
    "EventState",
    "NewsItem",
    "TransmissionEffect",
]
