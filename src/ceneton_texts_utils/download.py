import hashlib
import signal
import sys
from contextlib import contextmanager
from datetime import datetime, timezone

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


def download_all_urls(database: URLDatabase):
    with shutdown_hook(lambda: database.save_database()):
        written = 0
        for entry in database.database_entries.values():
            metadata = download_url(entry)
            if metadata is None:
                continue

            data = {
                "last_checked": datetime.now(tz=timezone.utc),
                "last_status": metadata.last_status,
            }
            database.update_entry(entry.text_id, **data)
            written += 1
            if written % 50 == 0:
                database.save_database()

        # Ensure database is saved at the end
        database.save_database()
