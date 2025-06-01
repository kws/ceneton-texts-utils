import csv
from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3
from typing import Any
from datetime import datetime
import yaml

CENETON_ROOT_URL = "https://www.let.leidenuniv.nl/Dutch/Ceneton/"

@dataclass
class URLDatabaseEntryMetadata:
    last_attempt: datetime
    last_status: int
    etag: str | None = None
    last_modified: str | None = None
    content_length: int | None = None
    sha256: str | None = None


@dataclass(frozen=True)
class URLDatabaseEntry:
    text_id: int
    url: str
    source_slug: str
    database_path: Path
    skip: str | None = None
    comments: str | None = None

    @property
    def archive_folder(self) -> Path:
        hundreds = self.text_id // 100
        return self.database_path.parent / f"{hundreds*100:04d}-{(hundreds+1)*100:04d}" / f"{self.text_id:04d}"
    
    @property
    def metadata_path(self) -> Path:
        return self.archive_folder / "metadata.yml"
    
    @property
    def metadata(self) -> URLDatabaseEntryMetadata:
        if self.metadata_path.exists():
            return URLDatabaseEntryMetadata(**yaml.safe_load(self.metadata_path.read_text()))

    @property
    def content_path(self) -> Path:
        return self.archive_folder / "content.html"

    @property
    def content(self) -> bytes:
        with open(self.content_path, "rb") as f:
            return f.read()

    def save_metadata(self, metadata: URLDatabaseEntryMetadata):
        self.archive_folder.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_path, "w") as f:
            yaml.dump(asdict(metadata), f)

    def save_content(self, content: bytes):
        self.archive_folder.mkdir(parents=True, exist_ok=True)
        with open(self.content_path, "wb") as f:
            f.write(content)

    



class URLDatabase:
    def __init__(self, database_path: str | Path, create_if_missing: bool = False):
        self.database_path = Path(database_path)
        if create_if_missing and not self.database_path.exists():
            create_new_database(self.database_path)

        assert self.database_path.exists(), f"Database file {self.database_path} does not exist"

        self.database_entries: dict[int, URLDatabaseEntry] = {}
        self.database_entries_by_url: dict[str, URLDatabaseEntry] = {}

        self.refresh()

    def refresh(self):
        with open(self.database_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                id = int(row.pop("text_id"))
                entry = URLDatabaseEntry(text_id=id, database_path=self.database_path, **row)
                self.database_entries[id] = entry
        self.__reindex()

    def __reindex(self):
        self.database_entries_by_url.clear()
        for entry in self.database_entries.values():
            self.database_entries_by_url[entry.url] = entry

    def save_database(self):
        entries = sorted(self.database_entries.values(), key=lambda x: x.text_id)

        skip_headers = ["database_path"]
        headers = [h for h in URLDatabaseEntry.__dataclass_fields__.keys() if h not in skip_headers]

        with open(self.database_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for entry in entries:
                entry_dict = {k: v for k, v in asdict(entry).items() if k in headers}
                writer.writerow(entry_dict)

    def add_entry(self, **values: Any) -> URLDatabaseEntry:
        self.__reindex()
        assert "url" in values, "url is a required field"
        assert values["url"] is not None, "url cannot be None"
        assert values["url"] not in self.database_entries_by_url, f"Entry with url {values['url']} already exists"
        assert not "text_id" in values, "text_id is a reserved field and should not be provided"

        if len(self.database_entries) == 0:
            next_id = 1
        else:
            next_id = max(self.database_entries.keys()) + 1

        values["text_id"] = next_id
        entry = URLDatabaseEntry(database_path=self.database_path, **values)
        self.database_entries[entry.text_id] = entry

        self.__reindex()
        return entry

    def get_entry(self, text_id: int) -> URLDatabaseEntry | None:
        return self.database_entries.get(text_id)

    def get_entry_by_url(self, url: str) -> URLDatabaseEntry | None:
        self.__reindex()
        return self.database_entries_by_url.get(url)

    def update_entry(self, text_id: int, **values: Any):
        entry = self.database_entries.get(text_id)
        if entry is None:
            raise ValueError(f"Entry with text_id {text_id} does not exist")
        self.database_entries[text_id] = entry
        self.__reindex()

    def __contains__(self, url: str | int) -> bool:
        if isinstance(url, int):
            return url in self.database_entries
        else:
            return url in self.database_entries_by_url
        
    def __iter__(self):
        return iter(self.database_entries.values())

def create_new_database(database_path: str | Path):
    database_path = Path(database_path)
    assert not database_path.exists(), f"Database file {database_path} already exists"

    with open(database_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=URLDatabaseEntry.__dataclass_fields__.keys())
        writer.writeheader()


def _centeton_to_url(ceneton_slug: str) -> str:
    ceneton_slug = ceneton_slug.strip()
    url = f"{CENETON_ROOT_URL}{ceneton_slug}"

    if '#' in url:
        url = url.split('#')[0]

    if not url.endswith(".html"):
        url = f"{url}.html"

    return url

def populate_from_sqlite(database: URLDatabase, sqlite_path: str | Path, database_table: str|None = None):
    sqlite_path = Path(sqlite_path)
    assert sqlite_path.exists(), f"SQLite file {sqlite_path} does not exist"

    with sqlite3.connect(sqlite_path) as conn:
        if database_table is None:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            database_table = cursor.fetchone()[0]
            assert database_table is not None, "No tables found in database"

        cursor = conn.cursor()
        cursor.execute(f"SELECT http FROM {database_table} where http is not null order by http")
        # We want to fetch as dictionaries
        for row in cursor.fetchall():
            slug = row[0]
            url = _centeton_to_url(slug)

            if url in database:
                print(f"Skipping {url} because it already exists")
                continue

            entry = database.add_entry(url=url, source_slug="ceneton:"+slug)
            print(f"Added", entry)
        database.save_database()


def populate_from_mappings(database: URLDatabase, csv_path: str | Path):
    csv_path = Path(csv_path)
    assert csv_path.exists(), f"CSV file {csv_path} does not exist"

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ceneton_slug = row["ceneton_slug"]   
            corrected_slug = row["corrected_slug"]
            if not corrected_slug.strip():
                print(f"Skipping {ceneton_slug} because it has no corrected slug")
                continue

            url = _centeton_to_url(corrected_slug)
            if url in database:
                print(f"Skipping {url} because it already exists")
                continue

            database.add_entry(url=url, source_slug="corrected:"+corrected_slug, comments=f"Original slug: {ceneton_slug}")
        database.save_database()
