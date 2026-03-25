import pandas as pd
import pytest as pyt
import sys
from pathlib import Path

# -------------------------------
# Add src/services to sys.path so imports in gc_storage.py work
# -------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent  # goes up from tests/ -> src/
services_path = project_root / "src" / "services"
sys.path.insert(0, str(services_path))

# Now imports like `from decorators import logger, app_logger` will work
import gc_storage as gcs

def test_initialize_sclient():
    gcs.initialize_sclient()

