"""
turf – Horse racing domain-specific utilities.

Modules
-------
musique_decoder             Decode PMU "musique" strings into structured data.
race_conditions_parser      Parse race conditions text into structured features.
equipment_change_builder    Detect equipment changes between consecutive races.
runner_status_manager       Compute rest / recovery features per runner.
handicap_weight_feature_builder   Compute weight-related features for handicap races.
"""

from . import musique_decoder
from . import race_conditions_parser
from . import equipment_change_builder
from . import runner_status_manager
from . import handicap_weight_feature_builder

__all__ = [
    "musique_decoder",
    "race_conditions_parser",
    "equipment_change_builder",
    "runner_status_manager",
    "handicap_weight_feature_builder",
]
