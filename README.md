# Texas Codes Statutes Sync Tool

A Java CLI utility that downloads Texas statute code ZIP archives, extracts their chapter files, and syncs them to local disk, SFTP, or WebDAV.

## What this project does

- Pulls the statute code download index from the Texas Legislature metadata feed.
- Resolves and downloads each code ZIP archive.
- Extracts safe files from each archive and writes them into per-code folders.
- Tracks sync state using `.download_complete.json` marker files for incremental runs.
- Optionally posts webhook events for each extracted file and each completed code.
- Writes rotating logs to `logs/texascodesstatutes.log`.

## Requirements

- Java 23
- Maven 3.9+
- Network access to:
  - `https://statutes.capitol.texas.gov`
  - `https://tcss.legis.texas.gov`

## Build

```bash
mvn -DskipTests package
```

This produces a runnable JAR at `target/texascodesstatutes-1.0-SNAPSHOT.jar`.

## Run

### Show help

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar --help
```

### Default local sync

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar \
  --target=local \
  --data-dir=./data
```

### Dry run (no writes)

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar --dry-run
```

### Force full refresh

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar --force
```

## Configuration file

You can keep settings in a properties file and pass it with `--config=...`.

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar --config=./texascodesstatutes.properties
```

Use `texascodesstatutes.example.properties` as a template.

Notes:

- CLI options always override values loaded from the config file.
- Equivalent key styles are accepted (dotted, dashed, underscored).

## Core options

- `--target=local|sftp|webdav`
- `--config=/path/config.properties`
- `--data-dir=PATH`
- `--metadata-url=URL`
- `--source-base=URL`
- `--webhook-url=URL`
- `--force`
- `--dry-run`
- `--allow-partial`
- `--help`

Downloads are throttled with a 3-second delay between ZIP requests.

## SFTP mode

Required/typical options:

- `--target=sftp`
- `--sftp-host=HOST`
- `--sftp-port=22`
- `--sftp-user=USER`
- Auth via either:
  - `--sftp-password=PASSWORD` (or `SFTP_PASSWORD` env var)
  - `--sftp-private-key=/path/key` (+ optional passphrase)
- `--sftp-root=/remote/path`

Example:

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar \
  --target=sftp \
  --sftp-host=sftp.example.com \
  --sftp-user=alice \
  --sftp-root=/remote/path/texas-codes
```

## WebDAV mode

Required/typical options:

- `--target=webdav`
- `--webdav-url=https://server/path`
- `--webdav-user=USER`
- `--webdav-password=PASSWORD` (or `WEBDAV_PASSWORD` env var)

Example:

```bash
java -jar target/texascodesstatutes-1.0-SNAPSHOT.jar \
  --target=webdav \
  --webdav-url=https://webdav.example.com/dav/texas-codes/ \
  --webdav-user=alice
```

## Output structure

For each code, files are written into a code-specific folder and include a marker file:

```text
data/
  penal_code/
    ch_1_general_provisions.docx
    ch_2_burdens_of_proof.docx
    .download_complete.json
```

Marker files store source metadata (URL, ETag/Last-Modified/content length), extracted file list, and corruption status to support change detection.

## Webhook payloads

If `--webhook-url` is provided, the tool sends JSON POST events:

- `event: "file_downloaded"` for each extracted file
- `event: "code_download_completed"` when each code finishes

Payload fields include:

- `code`
- `codeName`
- `statuteName`
- `newFileName`
- `relativePath`
- `timestampUtc`
- plus `fileCount` and `partial` on completion events

Non-2xx webhook responses are logged as warnings; sync continues.

## Logging

- Logs are written to `logs/texascodesstatutes.log`.
- Rotation policy: 10 MB per file, up to 5 archived log files.
- Console output is mirrored into the rotating log.

## Exit behavior

- Exit code `0`: successful run with no failed codes.
- Exit code `1`: interrupted execution, argument/config errors, or one or more failed code syncs.

## Development notes

- Main entry point: `group.chapmanlaw.texascodesstatutes.app`
- Build config: `pom.xml`
- Sample config: `texascodesstatutes.example.properties`

