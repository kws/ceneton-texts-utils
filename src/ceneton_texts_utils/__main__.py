from pathlib import Path

import click
from tqdm import tqdm

from ceneton_texts_utils.download import download_all_urls
from ceneton_texts_utils.indexer import index_ceneton
from ceneton_texts_utils.url_database import (
    URLDatabase,
    populate_from_mappings,
    populate_from_sqlite,
)
from ceneton_texts_utils.w3m import W3M


@click.group()
def cli():
    pass


@cli.command()
@click.argument("sqlite_path", type=click.Path(exists=True))
@click.option(
    "--database-folder", type=click.Path(exists=True, file_okay=False), default="."
)
@click.option("--create", is_flag=True, default=False)
def sqlite(sqlite_path: str, database_folder: str, create: bool):
    database = URLDatabase(f"{database_folder}/index.csv", create_if_missing=create)
    populate_from_sqlite(database, sqlite_path)


@cli.command()
@click.argument("csv_path", type=click.Path(exists=True))
@click.option(
    "--database-folder", type=click.Path(exists=True, file_okay=False), default="."
)
@click.option("--create", is_flag=True, default=False)
def mappings(csv_path: str, database_folder: str, create: bool):
    database = URLDatabase(f"{database_folder}/index.csv", create_if_missing=create)
    populate_from_mappings(database, csv_path)


@cli.command()
@click.option(
    "--database-folder", type=click.Path(exists=True, file_okay=False), default="."
)
@click.option(
    "-m",
    "--min-interval-minutes",
    type=int,
    default=0,
    help="Skip entries that were last_checked within this many minutes",
)
def download(database_folder: str, min_interval_minutes: int):
    database = URLDatabase(f"{database_folder}/index.csv")
    download_all_urls(database, min_interval_minutes)


@cli.command()
@click.option(
    "--database-folder", type=click.Path(exists=True, file_okay=False), default="."
)
@click.option(
    "-e",
    "--entry-ids",
    type=int,
    multiple=True,
    default=None,
    help="Comma-separated list of entry IDs",
)
def w3m(database_folder: str, entry_ids: list[int] | None):
    database = URLDatabase(f"{database_folder}/index.csv")
    w3m = W3M()

    if entry_ids:
        entries = [database.get_entry(entry_id) for entry_id in entry_ids]
    else:
        entries = [e for e in database if e.content_path.exists()]

    for entry in tqdm(entries):
        w3m.convert_entry(entry)


@cli.command()
@click.argument("sqlite_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path(writable=True))
@click.option(
    "--database-folder", type=click.Path(exists=True, file_okay=False), default="."
)
@click.option("--database-table", type=str, default=None)
def index(
    sqlite_path: str, output_path: str, database_folder: str, database_table: str | None
):
    database_path = Path(database_folder) / "index.csv"

    index_ceneton(Path(sqlite_path), database_path, Path(output_path), database_table)


if __name__ == "__main__":
    cli()
