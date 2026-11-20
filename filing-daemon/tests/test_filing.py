from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import filing_daemon


def test_text_extractor_reads_txt(tmp_path):
    sample = tmp_path / "notes.txt"
    sample.write_text("Quarterly Invoice Total Due", encoding="utf-8")
    assert "invoice" in filing_daemon.TextExtractor.extract(sample)


def test_predict_category_keyword_fallback(tmp_path):
    sample = tmp_path / "summary.txt"
    sample.write_text("Bank account statement and balance", encoding="utf-8")
    assert filing_daemon.SelfHealingFilingDaemon().predict_category(sample) == "financial"
