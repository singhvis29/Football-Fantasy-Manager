#!/usr/bin/env python3
"""
Convenience script to run the data ingestion pipeline.

This script provides a simple way to run the data ingestion pipeline
without needing to use the full module path.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.data_ingestion import main

if __name__ == "__main__":
    main()
