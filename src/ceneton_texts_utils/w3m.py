import hashlib
import subprocess
from pathlib import Path

from ceneton_texts_utils.url_database import URLDatabaseEntry


class W3M:
    def __init__(self, w3m_path: str | None = None):
        self.w3m_path = w3m_path or "w3m"

    def convert(self, html_path: Path, text_path: Path) -> str:
        html_path_str = html_path.absolute().resolve().as_posix()

        s = subprocess.run(
            f"{self.w3m_path} -dump {html_path_str}", shell=True, capture_output=True
        )
        s.check_returncode()

        text = s.stdout.decode("utf-8")
        return text

    def convert_entry(self, entry: URLDatabaseEntry) -> str:
        text_path = entry.archive_folder / "content.txt"
        if text_path.exists():
            text = text_path.read_text()
            hashlib.sha256(text.encode("utf-8")).hexdigest()

            content = entry.content
            hashlib.sha256(content).hexdigest()

            # SKIP IF NOT UPDATED

        text = self.convert(entry.content_path, text_path)

        text_path.write_text(text)

        # entry.save_content(text.encode("utf-8"))

        return text
