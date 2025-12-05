# PRC Court Documents Importer (HTML)

[中文說明](README.zh-TW.md)

A Python tool to convert Chinese court documents from HTML/JSONL format to zhwikisource-compatible wikitext.

## Overview

This tool processes Chinese court document dumps, converting them from their original HTML format into properly formatted wikitext suitable for upload to Chinese Wikisource (zhwikisource).

## Features

- **Streaming Processing**: Handles large JSONL files efficiently without loading everything into memory
- **HTML Parsing**: Extracts document structure from HTML content, handling various formatting styles
- **Wikitext Generation**: Produces properly formatted wikitext with templates (`{{header}}`, `{{署名}}`, `{{印}}`, etc.)
- **CJK Text Cleanup**: Removes OCR artifacts like spurious spaces between Chinese characters
- **Signature Formatting**: Proper spacing for judge/clerk titles and names following zhwikisource conventions
- **Filtering**: Process only specific document types (判决书, 裁定书, etc.)
- **Resumable**: Checkpoint support for interrupting and resuming large batch jobs
- **Location Inference**: Automatically determines document location from court name

## Installation

```bash
# Clone the repository
git clone https://github.com/QZGao/prc-court-docs-importer-html.git
cd prc-court-docs-importer-html

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# or: source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

## Phase 1: Conversion

### Usage

#### Basic Conversion

```bash
# Convert all documents in a JSONL file
python -m convert input.jsonl

# Output files will be created:
# - input_converted.jsonl  (successful conversions)
# - input_failed.jsonl     (failed conversions with error details)
# - input_checkpoint.json  (progress checkpoint)
```

#### Filtering by Document Type

```bash
# Convert only 判决书 (judgments)
python -m convert input.jsonl --filter "*判决书"

# Convert multiple types (semicolon-separated)
python -m convert input.jsonl --filter "*判决书;*事令"
```

#### Limiting Output

```bash
# Convert only the first 100 matching documents
python -m convert input.jsonl --filter "*判决书" --limit 100
```

#### Custom Output Paths

```bash
python -m convert input.jsonl \
    --output converted.jsonl \
    --errors failed.jsonl \
    --checkpoint progress.json
```

#### Human-Readable Output

```bash
# Generate a TXT file with formatted wikitext for review
python -m convert input.jsonl --filter "*判决书" --limit 50 --txt output.txt
```

#### Saving Original JSON

```bash
# Save the original JSON for each processed document (useful for debugging)
python -m convert input.jsonl --filter "*判决书" --limit 50 --original originals.jsonl
```

#### Resuming Interrupted Jobs

```bash
# If interrupted (Ctrl+C), resume from checkpoint
python -m convert --resume input_checkpoint.json

# Can also set a new limit when resuming
python -m convert --resume input_checkpoint.json --limit 200
```

### CLI Options

| Option | Short | Description |
|--------|-------|-------------|
| `input` | | Input JSONL file path |
| `--output` | `-o` | Output JSONL file path |
| `--errors` | `-e` | Error log JSONL file path |
| `--filter` | `-f` | Glob pattern(s) to filter documents by title |
| `--limit` | `-l` | Maximum successful conversions |
| `--resume` | `-r` | Resume from checkpoint file |
| `--checkpoint` | `-c` | Checkpoint file path |
| `--txt` | `-t` | Human-readable TXT output path |
| `--original` | | Save original JSON for processed documents |

### Input Format

The tool expects JSONL (JSON Lines) or pretty-printed JSON files with court document records. Each record should contain:

| Field | Description |
|-------|-------------|
| `s1` | Document title |
| `s2` | Court name |
| `s7` | Case number (文书号) |
| `s22` | Full hierarchy string (court + doc type + case number) |
| `qwContent` | HTML content of the document |
| `wsKey` | Document unique identifier |

### Output Format

Each line contains a JSON object with:

```json
{
  "title": "Document title",
  "wenshu_id": "Unique identifier",
  "court": "Court name",
  "doc_type": "Document type",
  "doc_id": "Case number",
  "wikitext": "Complete wikitext content"
}
```

## Phase 2: Upload

The upload phase takes converted JSONL files and uploads them to zhwikisource with rate limiting and conflict resolution.

### Configuration

Create a `.env` file in the project root with your bot credentials:

```env
MW_BOT_USERNAME=YourUsername
MW_BOT_PASSWORD=BotName@your_bot_password_here
```

To get bot credentials:

1. Log in to [zh.wikisource.org](https://zh.wikisource.org)
2. Go to [Special:BotPasswords](https://zh.wikisource.org/wiki/Special:BotPasswords)
3. Create a new bot password with appropriate permissions (edit, createpage, movepage)

### Upload Usage

#### Basic Upload

```bash
# Upload all documents from converted JSONL
python -m upload converted.jsonl

# Output log files will be created:
# - uploaded_YYYYMMDD_HHMMSS.log        (successful uploads)
# - upload_failed_YYYYMMDD_HHMMSS.jsonl (failed uploads)
# - skipped_YYYYMMDD_HHMMSS.log         (skipped pages)
# - overwritable_YYYYMMDD_HHMMSS.jsonl  (pages that can be manually overwritten)
```

#### Limiting Upload Count

```bash
# Upload only the first 10 documents (for testing)
python -m upload converted.jsonl --max 10
```

#### Custom Rate Limiting

```bash
# Set custom interval between edits (default: 10 seconds, pywikibot's default)
python -m upload converted.jsonl --interval 5

# Set custom maxlag parameter (default: 5)
python -m upload converted.jsonl --maxlag 10
```

#### Disable Conflict Resolution

```bash
# Skip all pages that already exist (don't attempt resolution)
python -m upload converted.jsonl --no-resolve
```

#### Custom Log Directory

```bash
# Save logs to a specific directory
python -m upload converted.jsonl --log-dir ./logs
```

### Upload CLI Options

| Option | Short | Description |
|--------|-------|-------------|
| `input` | | Input converted JSONL file path |
| `--interval` | `-i` | Minimum seconds between edits (default: 10) |
| `--maxlag` | `-m` | Maxlag parameter for API (default: 5) |
| `--max` | `-n` | Maximum documents to upload |
| `--no-resolve` | | Disable conflict resolution |
| `--log-dir` | | Directory for log files |

### Conflict Resolution

When a page with the same title already exists, the uploader can automatically resolve certain conflicts:

1. **Existing `{{versions}}` page**: Adds a new entry with case-specific title
2. **Existing `{{header}}` page from same court**:
   - Moves existing page to a case-specific title
   - Creates a `{{versions}}` page at the original title
   - Uploads the new document to its case-specific title

Conflicts that cannot be resolved automatically are logged to the skipped log file.

### Rate Limiting

Rate limiting is handled by pywikibot's built-in throttle:

- **Edit interval**: Uses pywikibot's `put_throttle` setting (default 10 seconds, configurable via `--interval`)
- **Maxlag handling**: Pywikibot automatically respects server-side maxlag responses
- **Throttle file**: Pywikibot writes `throttle.ctrl` to manage throttling state

## License

MIT License - See [LICENSE](LICENSE) for details.

Code partially adopted from [prc-court-docs-importer-csv](https://github.com/QZGao/prc-court-docs-importer-csv).
