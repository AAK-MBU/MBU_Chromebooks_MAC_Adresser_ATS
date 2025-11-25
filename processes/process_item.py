"""Module to handle item processing"""

import os
import logging

import requests

import time
import random

from datetime import datetime

from mbu_dev_shared_components.database.connection import RPAConnection
from mbu_dev_shared_components.google.api.auth import GoogleTokenFetcher

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly"]


def process_item(item_data: dict, item_reference: str):
    """Main ATS entrypoint. Fetch token, retrieve devices, process them."""

    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    tvp_rows = []

    # Read certificate
    p12_key_path = os.getenv("GOOGLE_DLP_KEY")
    if not p12_key_path:
        raise RuntimeError("Environment variable GOOGLE_DLP_KEY is not set")

    # Load constants
    rpa_conn = RPAConnection(db_env="PROD", commit=False)
    with rpa_conn:
        admin_email = rpa_conn.get_constant("google_dlp_admin_email").get("value", "")
        app_email = rpa_conn.get_constant("google_dlp_app_email").get("value", "")

    # Authenticate
    logger.info("Requesting OAuth token ...")
    token_fetcher = GoogleTokenFetcher(p12_key_path, SCOPES, app_email, admin_email)
    access_token = token_fetcher.get_google_token().json()["access_token"]
    logger.info("OAuth token acquired successfully")

    logger.info("Fetching all chromebooks ...")
    # Fetch all devices
    devices = get_all_chromebooks(access_token)

    for d in devices:
        last_sync_raw = d.get("lastSync")

        last_sync_date = None

        if last_sync_raw:
            try:
                parsed_dt = datetime.fromisoformat(last_sync_raw.replace("Z", "+00:00"))

                last_sync_date = parsed_dt.date()

            except Exception:
                pass

        tvp_rows.append((
            d.get("deviceId"),
            d.get("serialNumber"),
            d.get("macAddress") or None,
            d.get("model") or None,
            d.get("status"),
            last_sync_date,
            d.get("orgUnitPath"),
        ))

    logger.info(f"Retrieved and formatted {len(tvp_rows)} Chromebooks.")

    # import sys
    # sys.exit()

    with RPAConnection(db_env="PROD", commit=True) as conn:
        cursor = conn.cursor   # correct

        cursor.fast_executemany = True

        logger.info("Creating temp table ...")
        # 1. Create temp table
        cursor.execute("""
            CREATE TABLE #ChromebookTemp (
                device_id NVARCHAR(300) NOT NULL,
                serial_number NVARCHAR(300) NULL,
                mac_address NVARCHAR(300) NULL,
                model NVARCHAR(300) NULL,
                status NVARCHAR(50) NULL,
                last_sync DATE NULL,
                org_unit NVARCHAR(300) NULL
            );
        """)

        logger.info("Bulk inserting rows into temp table ...")
        cursor.executemany("""
            INSERT INTO #ChromebookTemp
                (device_id, serial_number, mac_address, model, status, last_sync, org_unit)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, tvp_rows)

        logger.info("Calling stored procedure ...")
        # 3. Call stored procedure (uses the temp table)
        cursor.execute("EXEC rpa.sp_upsert_chromebooks")

    logger.info("SQL upsert completed successfully.")


def get_all_chromebooks(access_token: str) -> list[dict]:
    """Fetch all ChromeOS devices with proper pagination."""

    base_url = (
        "https://admin.googleapis.com/admin/directory/v1/"
        "customer/my_customer/devices/chromeos?projection=FULL&maxResults=300"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    all_devices = []
    next_page = None
    page_number = 1

    logger.info("Fetching list of ChromeOS devices…")

    while True:
        url = base_url + (f"&pageToken={next_page}" if next_page else "")
        resp = safe_request(url=url, headers=headers)

        payload = resp.json()
        page_devices = payload.get("chromeosdevices", [])

        print(f"first device on page:\n{page_devices[0]}")

        logger.info(f"Fetched page {page_number} ({len(page_devices)} devices)")

        all_devices.extend(page_devices)

        next_page = payload.get("nextPageToken")
        if not next_page:
            break

        page_number += 1

    logger.info(f"Total devices retrieved: {len(all_devices)}")
    return all_devices


def safe_request(url: str, headers: dict, max_retries: int = 5, timeout: int = 60):
    """GET with exponential backoff and proper retry logic."""

    backoff = 1

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code == 429:
                logger.warning(f"429 rate limit hit on attempt {attempt}")

            elif resp.status_code == 403:
                if "rateLimitExceeded" in resp.text:
                    logger.warning(f"403 rate-limit hit on attempt {attempt}")
                else:
                    logger.error("403 Forbidden (non-retry)")
                    return resp

            elif 400 <= resp.status_code < 500:
                logger.error(f"Client error {resp.status_code}")
                return resp

            elif 500 <= resp.status_code < 600:
                logger.warning(f"Server error {resp.status_code}")

            else:
                logger.error(f"Unexpected status code {resp.status_code}")
                return resp

        except requests.RequestException as e:
            logger.warning(f"Network error: {e} (attempt {attempt})")

        if attempt == max_retries:
            raise RuntimeError(f"Failed after {max_retries} attempts: {url}")

        sleep_for = backoff + random.random()
        logger.info(f"Sleeping {sleep_for:.2f}s before retry")
        time.sleep(sleep_for)
        backoff *= 2

    raise RuntimeError("safe_request() fell through unexpectedly")
