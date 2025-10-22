from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_nyc_endpoints_routes():
    # Ensure flask can list routes (app imports cleanly)
    proc = subprocess.run([sys.executable, "-m", "flask", "--app", "src/pocket_gis/api.py", "routes"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "/nyc/crashes" in proc.stdout
    assert "/nyc/hotspots" in proc.stdout

