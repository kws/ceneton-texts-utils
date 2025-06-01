import csv
import sqlite3
from pathlib import Path

from tqdm import tqdm

from ceneton_texts_utils.url_database import URLDatabase, URLDatabaseEntry


class SlugIndexer:
    def __init__(self, url_database: URLDatabase):
        self.url_database = url_database
        self.urls_by_slug = {entry.source_slug: entry for entry in url_database}

    def get_by_slug(self, slug: str) -> URLDatabaseEntry | None:
        # First check if we have a successful URL for the primary entry.
        entry = self.urls_by_slug.get(f"ceneton:{slug}", None)
        if entry is not None and entry.last_status == 200:
            return entry

        # If not, see if we have a successful URL for the mapping entry.
        for entry in self.url_database:
            if entry.original_slug == f"ceneton:{slug}" and entry.last_status == 200:
                return entry

        return None


def index_ceneton(
    sqlite_path: Path,
    url_path: Path,
    output_path: Path,
    database_table: str | None = None,
):
    assert sqlite_path.exists(), "Ceneton database not found"
    assert url_path.exists(), "URL database not found"

    slug_indexer = SlugIndexer(URLDatabase(url_path))

    with sqlite3.connect(sqlite_path) as conn:
        if database_table is None:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            database_table = cursor.fetchone()[0]
            assert database_table is not None, "No tables found in database"

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT http, nummer, auteurva, titel, jaarnr, genre, oorspr_auteur_va, drukkerva, plaats_van_uitgave FROM {database_table} where http is not null order by nummer"  # noqa: E501
        )

        output_columns = [
            "nummer",
            "text_id",
            "auteurva",
            "titel",
            "jaarnr",
            "genre",
            "oorspr_auteur_va",
            "drukkerva",
            "plaats_van_uitgave",
        ]

        cursor.row_factory = sqlite3.Row

        all_rows = []
        for row in tqdm(cursor.fetchall()):
            slug = row["http"]
            entry = slug_indexer.get_by_slug(slug)
            if entry:
                row = dict(row)
                row["text_id"] = entry.text_id
                row = {k: v for k, v in row.items() if k in output_columns}
                all_rows.append(row)

        with open(output_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(all_rows)
