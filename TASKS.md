# TASKS.md

This file describes the full specification for implementing the **court-document conversion and upload pipeline**.

## 1. Project Overview

Implement a complete system that processes a very large corpus of Chinese court documents (raw HTML embedded in JSONL), converts them to **clean, normalized wikitext**, and then prepares them for upload to **zhwikisource** using a Toolforge-based bot.

The overall workflow has **two phases**:

1. **Local Conversion Phase (runs on my personal computer)** (`convert/`)
   - Stream a large corpus JSONL (8GB per month).  
   - Parse each JSON line, extract HTML, convert to structured wikitext.  
   - Write all successful conversions to a new JSONL file with fields:  
     `title, wenshuID, court, type, docID, wikitext`  
   - Log failures to a separate JSONL file for later manual inspection.

2. **Toolforge Upload Phase** (`upload/`)  
   - A Toolforge bot will read the *converted* JSONL in batches.  
   - It will upload pages to zhwikisource at a controlled rate  
     (e.g., one edit every 3 seconds).  
   - The bot must obey API constraints, throttling, and backoff logic.  
   - All uploads are queued; the converter never blocks waiting for uploads.

No central orchestrator is needed; the two phases are independent scripts.

## 2. File Formats

### 2.1 Input Corpus JSONL (Very Large)

Each line is a JSON object containing:

- `s1` (title)
- `s2` (court; 省 may be omitted, must be inferred back from s22)
- `s7` (docID)
- `s22` (court (always contain 省) + type + docID combined; reduce court and docID to get type)
- `wsKey` (wenshuID)
- `qwContent` (FULL HTML string)
- other metadata fields not relevant for conversion.

Please inspect `test_cases.json` to learn about the example inputs.

### 2.2 Output Converted JSONL (Compact)

Each line must contain:

```json
{
  "title": "...",
  "wenshuID": "...",
  "court": "...",
  "type": "...",
  "docID": "...",
  "wikitext": "..."
}
```

Wikitext must be clean, readable, and idiomatic for zhwikisource:

```wikitext
{{header
|title = <<TITLE>>
|noauthor = <<COURT>>
|type = 中华人民共和国<<TYPE>>
|lawmaker = 
|section = 
|previous = 
|next = 
|year = <<YEAR>>
|month = <<MONTH>>
|day = <<DAY>>
|loc = <<LOCATION>>
|from = 
|notes = 
|edition = 
}}

<center><b>
{{larger|<<COURT>>}}

{{larger|<<TYPE>>}}
</b></center>

<div align="right">
<<DOC_ID>>
</div>

<<BODY>>

{{署名|
<<JUDGES>>

{{印|<<COURT>>|center=国徽}}

<<DATE_CN>>

<<CLERKS>>
}}

<<FURTHER_NOTES>>

{{PD-PRC-exempt}}
```

This wikitext conversion must be dealt at a separate code module.

The right-aligned text after the main text body will be treated as {{署名}} content, while text after {{署名}} will be treated as further notes. Year, month, day must be extracted from the Chinese date string inside {{署名}}.

The location must be inferred from the court name. (separate code file inside the converter module)

### 2.3 Failure Log JSONL

Each line records a parse failure:

```json
{
  "error_stage": "...",
  "error_message": "...",
  "raw_json": {...},
  "timestamp": "ISO-8601"
}
```

The converter **must not crash** on bad inputs; failures must be logged and processing continues.

## 3. Local Conversion Phase — Requirements

Implement a streaming converter with the following characteristics (`convert/`):

### 3.1 Streaming Architecture

- Must handle 8GB+ input JSONL efficiently.  
- Read line-by-line; never load entire dataset.  
- For each line:
  - Parse JSON
  - Extract `qwContent` HTML
  - Normalize and convert to wikitext
  - Append successful output to `converted.jsonl`
  - Append failures to `failed.jsonl`

### 3.2 HTML Normalizer (Core Logic)

Implement a **deterministic HTML-to-block-segmentation engine**:

- Parse HTML with BeautifulSoup or lxml.
- Detect meaningful structural cues:
  - `text-align: center` → headers
  - `text-align: right` → docIDs / signatures / dates
  - `text-indent` / margins → paragraph
  - `<table>` recognition and extraction
- Clean inline CSS noise: font, color, style.
- Extract paragraphs in reading order.
- Extract metadata tables:
  - Handle merged cells `colspan`, `rowspan`
  - Convert “label → value” table patterns to wikitext tables
- Extract signatures at the end:
  - Recognize right-aligned signatures
  - Normalize spacing
- Reconstruct a clean, readable wikitext document.

### 3.3 Wikitext Rendering

Produce standard zhwikisource-friendly wikitext:

- Title: title, and inside `{{header|...}}`
- Paragraphs: separated by blank lines. If indented, use `{{gap}}` at the paragraph start to indicate indentation.
- Lists: use `*` for unordered lists, and `#` for ordered lists, but most lists in the actual HTMLs are formatted as simple paragraphs rather than true lists, so true lists that need to be handled are rare.
- Signatures: inside `{{署名|...}}`
- Tables: use standard wikitext table syntax.

### 3.4 Deterministic Behavior

- The conversion must be stateless and reproducible.
- Everything must be pure logic based on HTML structure.

## 4. Failure Logging

A conversion attempt must always produce one of:

1. A successful JSON entry appended to `converted.jsonl`, or  
2. A failure entry appended to `failed.jsonl`.

Failures must contain:

- stage (`html_parse`, `table_parse`, `block_detect`, `render`)
- full error message
- timestamp
- original JSON line for reproducibility

The converter **never stops** on errors.

## 5. Performance Requirements

- Should process **at least 50–200 documents/sec** locally on a modern machine.
- Must be able to support multiprocessing safely, but don't implement multiprocessing yet:
  - Worker procedure handle conversion.
  - Main procedure writes outputs (to avoid corruption).
  - Workers return `(success, result_dict)` or `(failure, error_info)`.

## 6. Toolforge Upload Phase — Requirements

Implement a second script that runs **on Toolforge** (`upload/`):

### 6.1 Input

- Reads the *converted* JSONL file produced locally.
- Each entry includes: title, wenshuID, court, type, docID, wikitext.

### 6.2 Upload Logic

- Apply a strict rate limit: **one edit every 3 seconds**.
- Use MediaWiki API (mwclient or Pywikibot).
- For each document:
  - Construct page title using consistent naming convention.
  - Upload wikitext.
  - Mark edits as bot edits.
  - Handle errors with retry & exponential backoff.

### 6.3 Maxlag Handling

- Include `maxlag=5` in API requests.
- On `maxlag` errors, wait 30–120 seconds before retrying.

### 6.4 Logging (On Toolforge)

Maintain logs (JSONL, with title, wenshuID, timestamp):

- `uploaded.log` — successfully uploaded pages
- `upload_failed.jsonl` — upload failures
- `skipped.log` — pages that already exist or are duplicates

### 6.5 Duplicate resolution

This is a complex multi-step check that resolves some duplicates automatically instead of skipping them. This is already implemented in a separate project, so just reuse that logic from `https://github.com/QZGao/prc-court-docs-importer-csv/blob/main/core/conflict_resolution.py` and integrate it into the upload phase.

## 7. Acceptance Criteria

A complete solution must:

- Convert the full October 2024 dataset (~10k docs) without crash.
- Produce valid JSONL outputs.
- Generate readable, consistent, idiomatic zhwikisource wikitext.
- Upload at controlled speed without hitting API rate limits.
- Recover from all failures gracefully.
- Maintain a well-structured codebase with clear separation of concerns.
