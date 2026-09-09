import os

# Silence Great Expectations' per-metric tqdm progress bars during tests.
os.environ.setdefault("TQDM_DISABLE", "1")
