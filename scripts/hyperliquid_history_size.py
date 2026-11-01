#!/usr/bin/env python3

"""
============================================================
HYPERLIQUID HISTORICAL ARCHIVE SIZE ESTIMATOR
============================================================

Queries AWS S3 metadata to estimate the compressed size and
download cost of Hyperliquid historical node_fills_by_block data.

Historical period:
    November 1, 2025 through October 31, 2026.

Features:
    - Counts hourly historical archive files.
    - Calculates total compressed data size.
    - Identifies missing hourly archives.
    - Estimates AWS data transfer and request costs.
    - Does not download any historical archive data.

Uses the AWS CLI with the configured 'admin' profile and
Requester Pays authorization.

============================================================
"""

import json
import subprocess

from datetime import datetime, timedelta, timezone


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "hl-mainnet-node-data"

PREFIX = "node_fills_by_block/hourly"

AWS_PROFILE = "admin"

START_DATE = datetime(2025, 11, 1, tzinfo=timezone.utc)

END_DATE = datetime(2026, 11, 1, tzinfo=timezone.utc)

# Approximate standard S3 internet transfer price per GB.
# Actual pricing depends on AWS Region and applicable allowances.
TRANSFER_COST_PER_GB = 0.09

# Approximate S3 GET request price per 1,000 requests.
GET_COST_PER_1000 = 0.0004

# Approximate S3 LIST request price per 1,000 requests.
LIST_COST_PER_1000 = 0.005

# Estimated cost: Less than $0.01 in AWS S3 LIST requests to query one year of archive metadata (no downloads).


# ============================================================
# FETCH S3 OBJECT METADATA
# ============================================================

def list_day_objects(day):
    prefix = f"{PREFIX}/{day:%Y%m%d}/"

    command = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--bucket",
        BUCKET,
        "--prefix",
        prefix,
        "--request-payer",
        "requester",
        "--profile",
        AWS_PROFILE,
        "--output",
        "json",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
    )

    return json.loads(result.stdout).get("Contents", [])


# ============================================================
# ESTIMATE HISTORICAL ARCHIVE SIZE
# ============================================================

def main():
    total_bytes = 0
    total_files = 0
    list_requests = 0

    missing_files = []

    current_day = START_DATE

    expected_files = (
        (END_DATE - START_DATE).days * 24
    )

    print()
    print("[HYPERLIQUID HISTORICAL] scanning AWS S3 metadata...")
    print(f"[HYPERLIQUID HISTORICAL] start={START_DATE.date()}")
    print(f"[HYPERLIQUID HISTORICAL] end={END_DATE.date()} (exclusive)")
    print()

    while current_day < END_DATE:
        objects = list_day_objects(current_day)

        list_requests += 1

        files = {
            obj["Key"].split("/")[-1]: obj["Size"]
            for obj in objects
        }

        for hour in range(24):
            filename = f"{hour}.lz4"

            if filename in files:
                total_bytes += files[filename]
                total_files += 1
            else:
                missing_files.append(
                    f"{current_day:%Y%m%d}/{filename}"
                )

        print(
            f"[HYPERLIQUID HISTORICAL] scanned "
            f"{current_day:%Y-%m-%d} | "
            f"archives={total_files:,}"
        )

        current_day += timedelta(days=1)

    # ============================================================
    # CALCULATE SIZE
    # ============================================================

    total_gib = total_bytes / (1024 ** 3)
    total_gb = total_bytes / (1000 ** 3)

    # ============================================================
    # ESTIMATE AWS COST
    # ============================================================

    transfer_cost = total_gb * TRANSFER_COST_PER_GB

    get_cost = (
        total_files / 1000
    ) * GET_COST_PER_1000

    list_cost = (
        list_requests / 1000
    ) * LIST_COST_PER_1000

    total_cost = (
        transfer_cost
        + get_cost
        + list_cost
    )

    # ============================================================
    # DISPLAY RESULTS
    # ============================================================

    print()
    print("============================================================")
    print("HYPERLIQUID HISTORICAL YEAR ESTIMATE")
    print("============================================================")

    print(
        f"Period:         {START_DATE.date()} "
        f"to {(END_DATE - timedelta(days=1)).date()}"
    )

    print(f"Expected files: {expected_files:,}")
    print(f"Available:      {total_files:,}")
    print(f"Missing:        {len(missing_files):,}")

    print()
    print(f"Compressed:     {total_gib:,.2f} GiB")
    print(f"Compressed:     {total_gb:,.2f} GB")

    print()
    print(f"Transfer cost:  ${transfer_cost:,.2f}")
    print(f"GET requests:   ${get_cost:,.4f}")
    print(f"LIST requests:  ${list_cost:,.4f}")
    print(f"Estimated total: ${total_cost:,.2f} USD")

    if missing_files:
        print()
        print("Missing archives:")

        for filename in missing_files[:20]:
            print(f"  {filename}")

        if len(missing_files) > 20:
            print(
                f"  ... and {len(missing_files) - 20:,} more"
            )

    print()
    print("No historical archive data was downloaded.")
    print("============================================================")
    print()


if __name__ == "__main__":
    main()