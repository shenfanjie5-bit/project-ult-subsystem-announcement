"""Announcement Ex-2 signal generation public API."""

from __future__ import annotations

from .aggregator import SignalFunc, derive_signal_candidates
from .candidates import (
    AnnouncementSignalCandidate,
    SignalDirection,
    SignalTimeHorizon,
    make_signal_id,
)
from .classifier import SignalDecision, classify_signal_for_fact
from .templates import SIGNAL_TEMPLATES, SignalTemplate

__all__ = [
    "AnnouncementSignalCandidate",
    "SIGNAL_TEMPLATES",
    "SignalDecision",
    "SignalDirection",
    "SignalFunc",
    "SignalTemplate",
    "SignalTimeHorizon",
    "classify_signal_for_fact",
    "derive_signal_candidates",
    "make_signal_id",
]
