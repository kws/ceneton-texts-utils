import hashlib
import signal
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import requests

from ceneton_texts_utils.url_database import (
    URLDatabase,
    URLDatabaseEntry,
    URLDatabaseEntryMetadata,
)


@contextmanager
def shutdown_hook(callback):
    """Context manager that registers signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, executing shutdown hook...")
        try:
            callback()
            print("Shutdown hook completed successfully.")
        except Exception as e:
            print(f"Error in shutdown hook: {e}")
        sys.exit(0)

    # Store original handlers
    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    try:
        yield
    finally:
        # Restore original handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def download_url(entry: URLDatabaseEntry):
    if entry.skip:
        print(f"Skipping {entry.url} because it is marked as skipped")
        return

    metadata = entry.metadata
    if metadata is None:
        metadata = URLDatabaseEntryMetadata(last_attempt=0, last_status=0)

    metadata.last_attempt = datetime.now(tz=timezone.utc)

    if metadata.etag:
        response = requests.head(entry.url)
        if response.status_code < 300:
            etag = response.headers.get("ETag")
            if etag == metadata.etag:
                print(f"Skipping {entry.url} because it is already up to date")
                return metadata

        else:
            print(f"Error {response.status_code} when checking {entry.url}")
            metadata.last_status = response.status_code
            entry.save_metadata(metadata)
            return metadata

    response = requests.get(entry.url)
    metadata.last_status = response.status_code
    if response.status_code == 200:
        metadata.etag = response.headers.get("ETag")
        metadata.last_modified = response.headers.get("Last-Modified")
        metadata.content_length = int(response.headers.get("Content-Length", 0))

        content = response.content
        metadata.sha256 = hashlib.sha256(content).hexdigest()

        entry.save_metadata(metadata)
        entry.save_content(content)

    return metadata


def download_all_urls(database: URLDatabase, min_interval_minutes: int = 0):
    """Download all URLs in the database, optionally skipping recently checked entries.

    Args:
        database: The URL database to process
        min_interval_minutes: Skip entries that were last_checked within this many
            minutes. Use 0 to disable this filtering (default).

    """
    with shutdown_hook(lambda: database.save_database()):
        now = datetime.now(tz=timezone.utc)
        cutoff_time = (
            now - timedelta(minutes=min_interval_minutes)
            if min_interval_minutes > 0
            else None
        )

        written = 0
        skipped_recent = 0

        for entry in database.database_entries.values():
            # Skip entries that were checked recently
            if cutoff_time and entry.last_checked and entry.last_checked > cutoff_time:
                skipped_recent += 1
                continue

            metadata = download_url(entry)
            if metadata is None:
                continue

            data = {
                "last_checked": now,
                "last_status": metadata.last_status,
            }
            database.update_entry(entry.text_id, **data)
            written += 1
            if written % 50 == 0:
                database.save_database()

        if skipped_recent > 0:
            print(
                f"Skipped {skipped_recent} entries that were checked within the last "
                f"{min_interval_minutes} minutes"
            )

        # Ensure database is saved at the end
        database.save_database()
