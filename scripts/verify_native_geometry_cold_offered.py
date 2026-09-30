"""Verify the qualified geometry binary's curve using the independent reducer."""
import sys

from scripts import launch_native_geometry_cold_offered_spot as campaign
from scripts.verify_native_cold_offered import main


if __name__ == '__main__':
    assert len(sys.argv) == 2
    main(sys.argv[1], campaign=campaign)
