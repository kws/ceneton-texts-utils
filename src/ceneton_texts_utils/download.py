import hashlib
from datetime import datetime, timezone

import requests

from ceneton_texts_utils.url_database import (
    URLDatabase,
    URLDatabaseEntry,
    URLDatabaseEntryMetadata,
)


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
                return
        else:
            print(f"Error {response.status_code} when checking {entry.url}")
            metadata.last_status = response.status_code
            entry.save_metadata(metadata)

    response = requests.get(entry.url)
    metadata.last_status = response.status_code
    if response.status_code == 200:
        metadata.etag = response.headers.get("ETag")
        metadata.last_modified = response.headers.get("Last-Modified")
        metadata.content_length = int(response.headers.get("Content-Length", 0))

        content = response.content
        metadata.sha256 = hashlib.sha256(content).hexdigest()

        entry.save_content(content)

    entry.save_metadata(metadata)


def download_all_urls(database: URLDatabase):
    for entry in database.database_entries.values():
        download_url(entry)
