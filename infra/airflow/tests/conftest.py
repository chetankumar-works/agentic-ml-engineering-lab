"""Makes `higgs_pipeline_tasks` importable without turning `dags/` into a
package (Airflow expects flat, independently-discoverable DAG modules).
"""

import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
