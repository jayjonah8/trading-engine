#!/usr/bin/env python3

"""
============================================================
HYPERLIQUID HISTORICAL DATE-RANGE DOWNLOADER
============================================================

Downloads hourly Hyperliquid historical fills from AWS S3.

Features:
    - Configurable start and end dates.
    - End date is exclusive.
    - Preserves all previously downloaded archives.
    - Skips existing files with matching S3 sizes.
    - Retries failed downloads.
    - Downloads to temporary files before finalizing.
    - Reports progress and failed archives.

============================================================
"""

import json
import os
import subprocess
import sys
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "hl-mainnet-node-data"
PREFIX = "node_fills_by_block/hourly"
AWS_PROFILE = "admin"

# ============================================================
# HISTORICAL DOWNLOAD BATCHES (1 MONTH EACH - 1 ENTIRE YEAR)
# ============================================================

# Historical date range (UTC).
# END_DATE is exclusive.
# Mark each batch [x] after successfully downloading and verifying.

# [ ] Batch 1: November 2025
# START_DATE = "2025-11-01"
# END_DATE   = "2025-12-01"

# [ ] Batch 2: December 2025
# START_DATE = "2025-12-01"
# END_DATE   = "2026-01-01"

# [ ] Batch 3: January 2026
# START_DATE = "2026-01-01"
# END_DATE   = "2026-02-01"

# [ ] Batch 4: February 2026
# START_DATE = "2026-02-01"
# END_DATE   = "2026-03-01"

# [ ] Batch 5: March 2026
# START_DATE = "2026-03-01"
# END_DATE   = "2026-04-01"

# [ ] Batch 6: April 2026
# START_DATE = "2026-04-01"
# END_DATE   = "2026-05-01"

# [ ] Batch 7: May 2026
# START_DATE = "2026-05-01"
# END_DATE   = "2026-06-01"

# [ ] Batch 8: June 2026
# START_DATE = "2026-06-01"
# END_DATE   = "2026-07-01"

# [ ] Batch 9: July 2026
# START_DATE = "2026-07-01"
# END_DATE   = "2026-08-01"

# [ ] Batch 10: August 2026
# START_DATE = "2026-08-01"
# END_DATE   = "2026-09-01"

# [ ] Batch 11: September 2026
START_DATE = "2026-09-01"
END_DATE   = "2026-10-01"

# [✅] Batch 12: October 2026
# START_DATE = "2026-10-01"
# END_DATE   = "2026-11-01"

# ============================================================
# COMPLETION STATUS
# ============================================================

# [ ] Batch 1  - November 2025
# [ ] Batch 2  - December 2025
# [ ] Batch 3  - January 2026
# [ ] Batch 4  - February 2026
# [ ] Batch 5  - March 2026
# [ ] Batch 6  - April 2026
# [ ] Batch 7  - May 2026
# [ ] Batch 8  - June 2026
# [ ] Batch 9  - July 2026
# [ ] Batch 10 - August 2026
# [ ] Batch 11 - September 2026
# [✅] Batch 12 - October 2026

# [ ] FULL YEAR DOWNLOADED (8,760 hourly archives)

DOWNLOAD_ROOT = (
    Path.home()
    / "trading-data/hyperliquid/node_fills_by_block/hourly"
)

MAX_RETRIES = 5


# ============================================================
# AWS COMMAND HELPER
# ============================================================

def run_aws(args):
    return subprocess.run(
        ["aws", *args, "--profile", AWS_PROFILE],
        capture_output=True,
        text=True,
        check=True,
    )


# ============================================================
# S3 METADATA
# ============================================================

def get_day_objects(day):
    prefix = f"{PREFIX}/{day:%Y%m%d}/"

    result = run_aws([
        "s3api",
        "list-objects-v2",
        "--bucket", BUCKET,
        "--prefix", prefix,
        "--request-payer", "requester",
        "--output", "json",
    ])

    return {
        obj["Key"]: obj["Size"]
        for obj in json.loads(result.stdout).get("Contents", [])
    }


# ============================================================
# DOWNLOAD ARCHIVE
# ============================================================

def download_archive(key, destination, expected_size):
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.is_file():
        if destination.stat().st_size == expected_size:
            return "skipped"

    temporary = destination.with_name(destination.name + ".part")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            run_aws([
                "s3api",
                "get-object",
                "--bucket", BUCKET,
                "--key", key,
                "--request-payer", "requester",
                str(temporary),
            ])

            actual_size = temporary.stat().st_size

            if actual_size != expected_size:
                raise RuntimeError(
                    f"Size mismatch: expected={expected_size}, "
                    f"actual={actual_size}"
                )

            os.replace(temporary, destination)

            return "downloaded"

        except (subprocess.CalledProcessError, OSError, RuntimeError) as exc:
            print(
                f"[RETRY] {key} "
                f"attempt={attempt}/{MAX_RETRIES}: {exc}",
                file=sys.stderr,
            )

            temporary.unlink(missing_ok=True)

            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 30))

    return "failed"


# ============================================================
# MAIN
# ============================================================

def main():
    start = datetime.strptime(
        START_DATE, "%Y-%m-%d"
    ).replace(tzinfo=timezone.utc)

    end = datetime.strptime(
        END_DATE, "%Y-%m-%d"
    ).replace(tzinfo=timezone.utc)

    if start >= end:
        print(
            "[ERROR] START_DATE must be earlier than END_DATE",
            file=sys.stderr,
        )
        return 1

    total_hours = int((end - start).total_seconds() // 3600)

    downloaded = 0
    skipped = 0
    failed = []

    print()
    print("[HYPERLIQUID] Starting historical download")
    print(f"[HYPERLIQUID] Start: {start.date()}")
    print(f"[HYPERLIQUID] End:   {end.date()} (exclusive)")
    print(f"[HYPERLIQUID] Expected archives: {total_hours:,}")
    print(f"[HYPERLIQUID] Destination: {DOWNLOAD_ROOT}")
    print()

    current_day = start

    while current_day < end:
        try:
            objects = get_day_objects(current_day)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            print(
                f"[ERROR] Failed to list {current_day.date()}: {exc}",
                file=sys.stderr,
            )
            return 1

        for hour in range(24):
            key = f"{PREFIX}/{current_day:%Y%m%d}/{hour}.lz4"

            expected_size = objects.get(key)

            if expected_size is None:
                failed.append(key)
                print(f"[MISSING] {key}")
                continue

            destination = (
                DOWNLOAD_ROOT
                / f"{current_day:%Y%m%d}"
                / f"{hour}.lz4"
            )

            status = download_archive(
                key,
                destination,
                expected_size,
            )

            if status == "downloaded":
                downloaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed.append(key)

            completed = downloaded + skipped + len(failed)

            print(
                f"[{completed:,}/{total_hours:,}] "
                f"{status.upper():10} {key}",
                flush=True,
            )

        current_day += timedelta(days=1)

    print()
    print("============================================================")
    print("HYPERLIQUID HISTORICAL DOWNLOAD SUMMARY")
    print("============================================================")
    print(f"Downloaded: {downloaded:,}")
    print(f"Skipped:    {skipped:,}")
    print(f"Failed:     {len(failed):,}")
    print(f"Total:      {total_hours:,}")
    print("============================================================")

    if failed:
        print("\nMissing or failed archives:")
        for key in failed:
            print(f"  {key}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())