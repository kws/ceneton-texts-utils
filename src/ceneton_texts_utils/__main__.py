import click

from ceneton_texts_utils.url_database import URLDatabase, populate_from_mappings, populate_from_sqlite
from ceneton_texts_utils.download import download_all_urls

@click.group()  
def cli():
    pass

@cli.command()
@click.argument("sqlite_path", type=click.Path(exists=True))
@click.option("--database-folder", type=click.Path(exists=True, file_okay=False), default=".")
@click.option("--create", is_flag=True, default=False)
def sqlite(sqlite_path: str, database_folder: str, create: bool):
    database = URLDatabase(f"{database_folder}/index.csv", create_if_missing=create)
    populate_from_sqlite(database, sqlite_path)

@cli.command()
@click.argument("csv_path", type=click.Path(exists=True))
@click.option("--database-folder", type=click.Path(exists=True, file_okay=False), default=".")
@click.option("--create", is_flag=True, default=False)
def mappings(csv_path: str, database_folder: str, create: bool):
    database = URLDatabase(f"{database_folder}/index.csv", create_if_missing=create)
    populate_from_mappings(database, csv_path)

@cli.command()
@click.option("--database-folder", type=click.Path(exists=True, file_okay=False), default=".")
def download(database_folder: str):
    database = URLDatabase(f"{database_folder}/index.csv")
    download_all_urls(database)

if __name__ == "__main__":
    cli()