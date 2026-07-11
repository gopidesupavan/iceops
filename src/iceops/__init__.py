"""iceops — doctor, janitor, and autopilot for Apache Iceberg tables."""

from .models import (
    CostReport,
    Finding,
    FleetReport,
    HealthReport,
    Severity,
    Status,
    TableMetrics,
)
from .operators import cost, doctor, scan

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "CostReport",
    "Finding",
    "FleetReport",
    "HealthReport",
    "Severity",
    "Status",
    "TableMetrics",
    "cost",
    "doctor",
    "scan",
]
