# filing-daemon

An event-driven local file ingestion and categorization daemon with a token-authenticated local IPC control plane.

## Project Structure
- `src/filing_daemon.py` — File watcher, text extraction, SQLite metadata/rule tracking, and IPC socket server
- `tests/test_filing.py` — Unit tests for text extraction and category prediction
- `requirements.txt` — Python dependencies
- `README.md` — Project overview and setup instructions

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
python src/filing_daemon.py