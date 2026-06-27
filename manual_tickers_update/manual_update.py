import sys
from pathlib import Path

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))

# Now import from A.utils
from weekly_tickers_update.weekly_tickers_update import (
    check_query_db_length,
)  # or: from A.utils import some_function

print("jap jeb")
