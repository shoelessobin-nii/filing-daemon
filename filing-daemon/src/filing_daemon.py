#!/usr/bin/env python3
"""
filing-daemon: Self-Healing, Event-Driven File Ingestion & Adaptive Categorization Daemon
with Token-Authenticated Unix Domain Socket IPC Control Plane.
"""

import hashlib
import json
import logging
import os
import shutil
import signal
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
import socket
import threading

from pypdf import PdfReader
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BASE_DIR = Path("./smart_filing_system")
INBOX_DIR = BASE_DIR / "inbox"
ARCHIVE_DIR = BASE_DIR / "archive"
BACKUP_DIR = BASE_DIR / "backups"
DB_PATH = BASE_DIR / "filing_system.db"
LOG_PATH = BASE_DIR / "daemon.log"
SOCKET_PATH = BASE_DIR / "filing_daemon.sock"
POLL_INTERVAL = 5

BASE_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class TextExtractor:
    @staticmethod
    def extract(file_path: Path) -> str:
        suffix = file_path.suffix.lower()
        text = ""
        try:
            if suffix == ".pdf":
                reader = PdfReader(str(file_path))
                for page in reader.pages:
                    extracted = page.extract_text()
                    if extracted:
                        text += extracted + "\n"
            elif suffix in [".txt", ".md", ".csv", ".log", ".json"]:
                text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            logging.error(f"Failed to extract text from {file_path.name}: {e}")
        return text.lower()


class InboxHandler(FileSystemEventHandler):
    def __init__(self, daemon_instance):
        self.daemon = daemon_instance

    def on_created(self, event):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        time.sleep(0.5)
        self.daemon.process_single_file(file_path)


class SelfHealingFilingDaemon:
    def __init__(self):
        self.running = True
        self.conn = None
        self.start_time = time.time()
        self.files_processed_session = 0
        self.unix_sock = None

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _init_db(self):
        if not self.conn:
            self.conn = sqlite3.connect(DB_PATH)
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT,
                    category TEXT,
                    stored_path TEXT,
                    backup_path TEXT,
                    sha256 TEXT,
                    filed_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS learned_rules (
                    pattern TEXT PRIMARY KEY,
                    category TEXT,
                    confidence INTEGER DEFAULT 1
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL,
                    role TEXT CHECK(role IN ('admin', 'user')) NOT NULL DEFAULT 'user',
                    permissions TEXT NOT NULL DEFAULT 'read',
                    added_at TEXT
                )
            """)
            self.conn.commit()

    def heal_environment(self):
        INBOX_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        self._init_db()

        cursor = self.conn.cursor()
        cursor.execute("PRAGMA integrity_check;")
        result = cursor.fetchone()[0]
        if result != "ok":
            logging.error(f"Database integrity check failed: {result}")

    def _backup_database(self):
        if DB_PATH.exists():
            backup_file = BACKUP_DIR / f"db_backup_{int(time.time())}.db"
            shutil.copy2(DB_PATH, backup_file)

    def _get_hash(self, file_path: Path) -> str:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def predict_category(self, file_path: Path) -> str:
        self._init_db()
        cursor = self.conn.cursor()
        filename = file_path.name
        ext = file_path.suffix.lower()
        stem = file_path.stem

        raw_text = TextExtractor.extract(file_path)
        filename_tokens = [t.lower() for t in stem.replace("-", "_").split("_") if len(t) > 3]
        content_words = [w.strip(".,()[]{}:;\"'") for w in raw_text.split() if len(w) > 4]
        content_tokens = list(set(content_words))

        scores = {}
        all_patterns = [ext] + filename_tokens + content_tokens
        for pattern in all_patterns:
            if not pattern:
                continue
            cursor.execute("SELECT category, confidence FROM learned_rules WHERE pattern = ?", (pattern,))
            row = cursor.fetchone()
            if row:
                cat, conf = row
                weight = 2 if pattern in content_tokens else 1
                scores[cat] = scores.get(cat, 0) + (conf * weight)

        if scores:
            return max(scores, key=scores.get)

        text_corpus = f"{filename} {raw_text}"
        if any(kw in text_corpus for kw in ["invoice", "bill", "total due", "payment", "receipt"]):
            return "invoices"
        elif any(kw in text_corpus for kw in ["contract", "agreement", "nda", "party of the first part"]):
            return "legal"
        elif any(kw in text_corpus for kw in ["resume", "cv", "curriculum vitae", "experience"]):
            return "resumes"
        elif any(kw in text_corpus for kw in ["statement", "balance", "account", "bank"]):
            return "financial"

        ext_defaults = {
            ".pdf": "documents",
            ".txt": "documents",
            ".csv": "spreadsheets",
            ".xlsx": "spreadsheets",
            ".jpg": "images",
            ".png": "images",
        }
        return ext_defaults.get(ext, "uncategorized")

    def _reinforce_rule(self, filename: str, category: str):
        cursor = self.conn.cursor()
        stem = Path(filename).stem
        ext = Path(filename).suffix.lower()
        patterns = [ext] + [t.lower() for t in stem.replace("-", "_").split("_") if len(t) > 3]

        for pattern in patterns:
            if not pattern:
                continue
            cursor.execute(
                """
                INSERT INTO learned_rules (pattern, category, confidence)
                VALUES (?, ?, 1)
                ON CONFLICT(pattern) DO UPDATE SET
                confidence = CASE WHEN category = ? THEN confidence + 1 ELSE MAX(1, confidence - 1) END,
                category = CASE WHEN confidence <= 1 THEN ? ELSE category END
                """,
                (pattern, category, category, category),
            )
        self.conn.commit()

    def process_single_file(self, item: Path):
        if not item.is_file():
            return
        try:
            category = self.predict_category(item)
            dest_dir = ARCHIVE_DIR / category
            dest_dir.mkdir(parents=True, exist_ok=True)

            dest_path = dest_dir / item.name
            backup_path = BACKUP_DIR / f"{int(time.time())}_{item.name}"

            file_hash = self._get_hash(item)
            shutil.copy2(item, backup_path)
            shutil.move(str(item), str(dest_path))

            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO files (filename, category, stored_path, backup_path, sha256, filed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    item.name,
                    category,
                    str(dest_path),
                    str(backup_path),
                    file_hash,
                    datetime.utcnow().isoformat(),
                ),
            )
            self.conn.commit()
            self._reinforce_rule(item.name, category)
            self.files_processed_session += 1
            logging.info(f"FILED (Content-Aware): '{item.name}' -> '{category}/'")
        except Exception as e:
            logging.error(f"Failed to process '{item.name}': {e}")

    def learn_from_corrections(self):
        self._init_db()
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, filename, category, stored_path FROM files")
        records = cursor.fetchall()

        for rec in records:
            file_id, filename, recorded_cat, stored_path = rec
            current_path = Path(stored_path)

            found_new_cat = None
            for cat_dir in ARCHIVE_DIR.iterdir():
                if cat_dir.is_dir() and cat_dir.name != recorded_cat:
                    candidate = cat_dir / filename
                    if candidate.exists():
                        found_new_cat = cat_dir.name
                        current_path = candidate
                        break

            if found_new_cat:
                logging.info(f"Detected manual correction for '{filename}': '{recorded_cat}' -> '{found_new_cat}'")
                cursor.execute(
                    "UPDATE files SET category = ?, stored_path = ? WHERE id = ?",
                    (found_new_cat, str(current_path), file_id)
                )
                self.conn.commit()
                self._reinforce_rule(filename, found_new_cat)

    def _authenticate_request(self, request: dict) -> tuple[bool, str, dict]:
        token = request.get("token")
        if not token:
            return False, "", None
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        cursor = self.conn.cursor()
        cursor.execute("SELECT username, role, permissions FROM users WHERE token_hash = ?", (token_hash,))
        user = cursor.fetchone()
        if not user:
            return False, "", None
        return True, user[0], {"role": user[1], "permissions": user[2]}

    def _start_socket_server(self):
        try:
            if SOCKET_PATH.exists():
                SOCKET_PATH.unlink()
            self.unix_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.unix_sock.bind(str(SOCKET_PATH))
            self.unix_sock.listen(5)
            os.chmod(SOCKET_PATH, 0o600)
            logging.info(f"IPC Control plane listening on Unix socket: {SOCKET_PATH}")

            while self.running:
                try:
                    self.unix_sock.settimeout(1.0)
                    client_sock, _ = self.unix_sock.accept()
                    threading.Thread(target=self._handle_client_command, args=(client_sock,), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logging.error(f"Socket accept error: {e}")
        except Exception as server_err:
            logging.critical(f"Failed to start IPC server: {server_err}")
        finally:
            if self.unix_sock:
                try:
                    self.unix_sock.close()
                except Exception:
                    pass
            if SOCKET_PATH.exists():
                SOCKET_PATH.unlink()

    def _handle_client_command(self, client_sock: socket.socket):
        try:
            data = client_sock.recv(4096).decode("utf-8").strip()
            if not data:
                return

            request = json.loads(data)
            command = request.get("command")
            authenticated_commands = ["admin_add_user", "admin_list_users", "heal", "backup", "stop"]

            if command in authenticated_commands:
                is_auth, username, user_info = self._authenticate_request(request)
                if not is_auth:
                    response = {"status": "error", "message": "Authentication failed: Invalid or missing token."}
                    client_sock.sendall(json.dumps(response).encode("utf-8"))
                    return
                if command in ["admin_add_user", "admin_list_users", "stop"] and user_info["role"] != "admin":
                    response = {"status": "error", "message": "Forbidden: Administrator role required."}
                    client_sock.sendall(json.dumps(response).encode("utf-8"))
                    return

            response = {"status": "error", "message": "Unknown command"}
            self._init_db()

            if command == "status":
                cursor = self.conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM files")
                total_indexed = cursor.fetchone()[0]
                response = {
                    "status": "ok",
                    "uptime_seconds": int(time.time() - self.start_time),
                    "files_processed_session": self.files_processed_session,
                    "total_indexed_files": total_indexed,
                    "inbox_path": str(INBOX_DIR),
                    "archive_path": str(ARCHIVE_DIR)
                }
            elif command == "heal":
                self.heal_environment()
                response = {"status": "ok", "message": "Environment healing completed."}
            elif command == "backup":
                self._backup_database()
                response = {"status": "ok", "message": "Database backup created."}
            elif command == "stop":
                response = {"status": "ok", "message": "Daemon shutdown initiated."}
                client_sock.sendall(json.dumps(response).encode("utf-8"))
                client_sock.close()
                self.running = False
                return

            client_sock.sendall(json.dumps(response).encode("utf-8"))
        except Exception as e:
            err_resp = {"status": "error", "message": str(e)}
            try:
                client_sock.sendall(json.dumps(err_resp).encode("utf-8"))
            except Exception:
                pass
        finally:
            client_sock.close()

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received shutdown signal ({signum}). Terminating daemon...")
        self.running = False
        if self.unix_sock:
            try:
                self.unix_sock.close()
            except Exception:
                pass
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        sys.exit(0)

    def run(self):
        logging.info("Starting Self-Healing Learning Filing Daemon with inotify & IPC...")
        self.heal_environment()

        ipc_thread = threading.Thread(target=self._start_socket_server, daemon=True)
        ipc_thread.start()

        event_handler = InboxHandler(self)
        observer = Observer()
        observer.schedule(event_handler, path=str(INBOX_DIR), recursive=False)
        observer.start()

        try:
            while self.running:
                self.learn_from_corrections()
                self._backup_database()
                time.sleep(POLL_INTERVAL)
        except Exception as fatal_err:
            logging.critical(f"Unexpected loop error: {fatal_err}")
        finally:
            observer.stop()
            observer.join()
            if self.conn:
                self.conn.close()
            logging.info("Daemon stopped cleanly.")


if __name__ == "__main__":
    daemon = SelfHealingFilingDaemon()
    daemon.run()
