"""
pipeline_runner.py
------------------
Automated pipeline runner for Olist 360.
Runs ingestion, transform, validation, and monitoring in sequence.

Usage:
    python src/ops/pipeline_runner.py
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

LOGS = BASE_DIR / "logs"
LOGS.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "pipeline_runner.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("pipeline_runner")


def run_pipeline():
    start_time = datetime.now()
    steps = []

    logger.info("=" * 55)
    logger.info("OLIST 360 AUTOMATED PIPELINE STARTING")
    logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    # Step 1 — Ingestion
    try:
        logger.info("STEP 1 — Ingestion starting...")
        from src.etl.ingest import run_all as run_ingest
        run_ingest()
        steps.append(("Ingestion", "PASSED"))
        logger.info("STEP 1 — Ingestion done")
    except Exception as e:
        logger.error(f"STEP 1 FAILED: {e}")
        steps.append(("Ingestion", "FAILED"))
        _summary(steps, start_time)
        return

    # Step 2 — Transform
    try:
        logger.info("STEP 2 — Transform starting...")
        from src.etl.transform import run_all as run_transform
        run_transform()
        steps.append(("Transform", "PASSED"))
        logger.info("STEP 2 — Transform done")
    except Exception as e:
        logger.error(f"STEP 2 FAILED: {e}")
        steps.append(("Transform", "FAILED"))
        _summary(steps, start_time)
        return

    # Step 3 — Validation
    try:
        logger.info("STEP 3 — Validation starting...")
        from src.etl.validate import run_all as run_validate
        score = run_validate()
        steps.append(("Validation", f"PASSED ({score}%)"))
        logger.info(f"STEP 3 — Validation done | Score: {score}%")
    except Exception as e:
        logger.error(f"STEP 3 FAILED: {e}")
        steps.append(("Validation", "FAILED"))

    # Step 4 — Monitoring
    try:
        logger.info("STEP 4 — Monitoring starting...")
        from src.ops.monitor import run_monitoring
        run_monitoring()
        steps.append(("Monitoring", "PASSED"))
        logger.info("STEP 4 — Monitoring done")
    except Exception as e:
        logger.error(f"STEP 4 FAILED: {e}")
        steps.append(("Monitoring", "FAILED"))

    _summary(steps, start_time)


def _summary(steps, start_time):
    duration = (datetime.now() - start_time).seconds
    logger.info("=" * 55)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 55)
    for step, status in steps:
        logger.info(f"  {step:<20} {status}")
    logger.info(f"  Duration        : {duration} seconds")
    passed = sum(1 for _, s in steps if "PASSED" in s)
    logger.info(f"  Steps passed    : {passed}/{len(steps)}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_pipeline()