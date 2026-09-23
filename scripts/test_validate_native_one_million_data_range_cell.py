"""Independent closeout refuses a missing source or sealed outcome."""

import pytest

from scripts.validate_native_one_million_data_range_cell import validate


def test_validate_refuses_missing_frozen_inputs(tmp_path) -> None:
    with pytest.raises(ValueError, match="validation input missing"):
        validate(tmp_path, tmp_path)
