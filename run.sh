#!/usr/bin/env bash
# Generate raw sources, then run the orchestrated ingest->staging->marts pipeline.
set -e
cd "$(dirname "$0")"

pip install -r requirements.txt
python3 src/generate_data.py
python3 src/pipeline.py
echo ""
echo "See reports/lineage.png and reports/run_report.json"
