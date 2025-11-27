from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Callable, Iterable, List, Optional

logger = logging.getLogger(__name__)


def _normalize_decimal(value: str, decimal: str) -> str:
    if decimal == ".":
        return value
    return value.replace(decimal, ".")


def load_numeric_column(
    csv_path: str | Path,
    column: str,
    *,
    delimiter: str = ",",
    decimal: str = ".",
    invalid_values: Optional[Iterable[str]] = None,
) -> List[float]:
    """Load a numeric column from a CSV file, skipping invalid or missing entries."""
    file_path = Path(csv_path)
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    invalid_set = set(invalid_values or [])
    values: List[float] = []

    with file_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        if column not in reader.fieldnames:
            raise ValueError(f"Column '{column}' not found in {file_path.name}")

        for row in reader:
            raw_value = row.get(column)
            if raw_value is None:
                continue
            raw_value = raw_value.strip()
            if not raw_value or raw_value in invalid_set:
                continue
            normalized = _normalize_decimal(raw_value, decimal)
            try:
                values.append(float(normalized))
            except ValueError:
                logger.debug("Skipping non-numeric value '%s' in %s", raw_value, file_path.name)

    if not values:
        raise ValueError(f"No numeric values found for column '{column}' in {file_path}")

    return values


def create_csv_value_provider(
    csv_path: str | Path,
    column: str,
    *,
    delimiter: str = ",",
    decimal: str = ".",
    invalid_values: Optional[Iterable[str]] = None,
    loop: bool = True,
    postprocess: Optional[Callable[[float], float]] = None,
) -> Callable[[], float]:
    """Return a callable that cycles through numeric values from a CSV column."""
    values = load_numeric_column(
        csv_path,
        column,
        delimiter=delimiter,
        decimal=decimal,
        invalid_values=invalid_values,
    )
    total = len(values)
    index = 0

    def _provider() -> float:
        nonlocal index
        if not values:
            raise RuntimeError("No values available for CSV provider")

        value = values[index]
        if loop:
            index = (index + 1) % total
        else:
            index = min(index + 1, total - 1)

        if postprocess:
            return postprocess(value)
        return value

    return _provider



