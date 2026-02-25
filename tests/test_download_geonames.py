"""
Tests for src/download_geonames.py
"""

import zipfile
from unittest.mock import MagicMock, patch

import pytest

import download_geonames as dg


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_returns_parsed_dict(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("download:\n  url_data: http://example.com\n  data_dir: /tmp\n")
        result = dg.load_config(str(cfg))
        assert result == {"download": {"url_data": "http://example.com", "data_dir": "/tmp"}}

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises((FileNotFoundError, OSError)):
            dg.load_config(str(tmp_path / "nonexistent.yaml"))


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------

class TestDownloadFile:
    def _head_resp(self, status_code=200, content_length=100):
        r = MagicMock()
        r.status_code = status_code
        r.headers = {"Content-Length": str(content_length)}
        return r

    def _get_resp(self, content=b"x" * 100):
        r = MagicMock()
        r.headers = {"Content-Length": str(len(content))}
        r.iter_content = MagicMock(return_value=[content])
        r.raise_for_status = MagicMock()
        return r

    def test_returns_false_on_http_error(self, tmp_path):
        with patch("download_geonames.requests") as mock_req:
            mock_req.head.return_value = self._head_resp(status_code=404)
            result = dg.download_file("http://example.com/file.txt", tmp_path / "file.txt")
        assert result is False

    def test_skips_when_local_size_matches_remote(self, tmp_path):
        dest = tmp_path / "file.txt"
        dest.write_bytes(b"x" * 100)
        with patch("download_geonames.requests") as mock_req:
            mock_req.head.return_value = self._head_resp(content_length=100)
            result = dg.download_file("http://example.com/file.txt", dest)
        assert result is False
        mock_req.get.assert_not_called()

    def test_downloads_when_file_missing(self, tmp_path):
        dest = tmp_path / "file.txt"
        with patch("download_geonames.requests") as mock_req:
            mock_req.head.return_value = self._head_resp(content_length=100)
            mock_req.get.return_value = self._get_resp(b"x" * 100)
            result = dg.download_file("http://example.com/file.txt", dest)
        assert result is True
        assert dest.exists()
        assert dest.read_bytes() == b"x" * 100

    def test_downloads_when_local_size_differs(self, tmp_path):
        dest = tmp_path / "file.txt"
        dest.write_bytes(b"x" * 50)  # smaller than remote
        with patch("download_geonames.requests") as mock_req:
            mock_req.head.return_value = self._head_resp(content_length=100)
            mock_req.get.return_value = self._get_resp(b"x" * 100)
            result = dg.download_file("http://example.com/file.txt", dest)
        assert result is True

    def test_downloads_when_remote_size_is_zero(self, tmp_path):
        """When Content-Length is 0, size-check is skipped and file is re-downloaded."""
        dest = tmp_path / "file.txt"
        dest.write_bytes(b"old content")
        with patch("download_geonames.requests") as mock_req:
            mock_req.head.return_value = self._head_resp(content_length=0)
            mock_req.get.return_value = self._get_resp(b"new content")
            result = dg.download_file("http://example.com/file.txt", dest)
        assert result is True


# ---------------------------------------------------------------------------
# unzip_file
# ---------------------------------------------------------------------------

class TestUnzipFile:
    def test_extracts_single_file(self, tmp_path):
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("hello.txt", "hello world")
        dg.unzip_file(zip_path, tmp_path)
        assert (tmp_path / "hello.txt").read_text() == "hello world"

    def test_extracts_multiple_files(self, tmp_path):
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("a.txt", "aaa")
            zf.writestr("b.txt", "bbb")
        dg.unzip_file(zip_path, tmp_path)
        assert (tmp_path / "a.txt").read_text() == "aaa"
        assert (tmp_path / "b.txt").read_text() == "bbb"


# ---------------------------------------------------------------------------
# strip_header
# ---------------------------------------------------------------------------

class TestStripHeader:
    def test_removes_first_line(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("header line\nline1\nline2\n")
        dest = tmp_path / "dest.txt"
        dg.strip_header(src, dest, lines_to_skip=1)
        assert dest.read_text() == "line1\nline2\n"

    def test_removes_multiple_lines(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("h1\nh2\ndata\n")
        dest = tmp_path / "dest.txt"
        dg.strip_header(src, dest, lines_to_skip=2)
        assert dest.read_text() == "data\n"

    def test_zero_skip_keeps_all_lines(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("line1\nline2\n")
        dest = tmp_path / "dest.txt"
        dg.strip_header(src, dest, lines_to_skip=0)
        assert dest.read_text() == "line1\nline2\n"


# ---------------------------------------------------------------------------
# strip_comments_and_tail
# ---------------------------------------------------------------------------

class TestStripCommentsAndTail:
    def test_removes_hash_comment_lines(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("# comment\nline1\n# another comment\nline2\ntail1\ntail2\n")
        dest = tmp_path / "dest.txt"
        dg.strip_comments_and_tail(src, dest, tail_lines=2)
        assert dest.read_text() == "line1\nline2\n"

    def test_removes_only_tail_when_no_comments(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("line1\nline2\nline3\n")
        dest = tmp_path / "dest.txt"
        dg.strip_comments_and_tail(src, dest, tail_lines=1)
        assert dest.read_text() == "line1\nline2\n"

    def test_zero_tail_keeps_all_non_comment_lines(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("# comment\nline1\nline2\n")
        dest = tmp_path / "dest.txt"
        dg.strip_comments_and_tail(src, dest, tail_lines=0)
        assert dest.read_text() == "line1\nline2\n"

    def test_only_comment_lines_results_in_empty_output(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("# c1\n# c2\n")
        dest = tmp_path / "dest.txt"
        dg.strip_comments_and_tail(src, dest, tail_lines=0)
        assert dest.read_text() == ""
