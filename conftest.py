import sys
from pathlib import Path

# Make project root importable so that `import download.xxx` and
# `import upload.xxx` resolve correctly.
sys.path.insert(0, str(Path(__file__).parent))
