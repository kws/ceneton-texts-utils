# Ceneton Texts Utils

This repository contains utilities for downloading, processing, and maintaining an archival copy of the Ceneton transcripts: a comprehensive census of Dutch-language theatrical works up to the year 1803. Developed and maintained by the Department of Dutch at Leiden University, Ceneton ("Census Nederlands Toneel") is a major scholarly resource documenting over 12,500 plays, including metadata, bibliographic details, and textual editions.

The original datasource is available at https://www.let.leidenuniv.nl/Dutch/Ceneton/. 

## Repository Structure

This project is split into three repositories:

- **[ceneton-database](https://github.com/kws/ceneton-database)**: Contains the original Ceneton dataset exported from FileMaker Pro to XML format for long-term accessibility

- **[ceneton-texts-utils](https://github.com/kws/ceneton-texts-utils)** (this repository): Contains the code and utilities for downloading, processing, and maintaining the archive

- **[ceneton-texts](https://github.com/kws/ceneton-texts)**: Contains the actual archived texts with unique and immutable IDs and version control to make them easier to reference

## Download logic

The project consists of several commands that can be run via the CLI:

### 1. Database Population

Extract URLs from various sources and add them to the database:

**From SQLite database:**

```bash
python -m ct-utils sqlite <sqlite_path> [--database-folder .] [--create]
```

**From CSV mappings:**

```bash
python -m ct-utils mappings <csv_path> [--database-folder .] [--create]
```

### 2. Download Process

Once the database is populated, start the downloading:

```bash
python -m ct-utils download [--database-folder .]
```

The download process works as follows:

1. Iterate through all entries in the database
2. For each URL:
   - Skip if the entry is marked with `skip=True`
   - Check existing metadata to see if we have an ETag
   - If ETag exists, do a HEAD request to check if content has changed
   - If content is unchanged (same ETag), skip the download
   - If content is new or changed, download the full content
   - Save both the content (`content.html`) and metadata (`metadata.yml`) to the archive folder
   - Track download attempts, status codes, ETags, and content hashes for efficient re-downloading

## The database format

The 'database' is just a CSV with the following columns:

- text_id: The numeric "primary key" for the text
- url: The URL of the text
- source_slug: The slug of the text on the CENETON website. This starts with a 'tag' to indicate the source, for now either 'ceneton:' or 'exceptions:' followed by the slug value.
- skip: A boolean to indicate if the text should be skipped. Used to maintain database identifiers, but not download.
- comments: Any user comments about the entry.

## The archive folder

The utilities in this repository create and maintain an archive with the following structure (stored in the [ceneton-texts](https://github.com/kws/ceneton-texts) repository):

```text
archive/
├── index.csv
├── 0000-0100/
│   ├── 0001/
│   │   ├── metadata.yml
│   │   └── content.html
│   ├── 0002/
│   │   ├── metadata.yml
│   │   └── content.html
│   └── ... (more numbered folders)
├── 0100-0200/
│   ├── 0100/
│   │   ├── metadata.yml
│   │   └── content.html
│   └── ... (more numbered folders)
└── ... (additional hundred-range folders)
```

The archive uses a hierarchical structure where text files are grouped into "hundreds" folders (e.g., `0000-0100`, `0100-0200`, etc.) to avoid having too many folders in a single directory. Within each hundreds folder, individual texts are stored in folders named by their 4-digit `text_id`.
