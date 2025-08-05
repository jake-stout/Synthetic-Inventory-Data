import sys
import random
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


import pytest


@pytest.fixture(autouse=True)
def reset_random_seed():
    random.seed(42)
