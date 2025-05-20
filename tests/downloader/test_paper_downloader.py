import os
import tempfile
from krawl.downloader.paper_downloader import PaperDownloader

def test_download_pdf():
    url = "https://arxiv.org/pdf/2106.14834.pdf"
    with tempfile.TemporaryDirectory(prefix="paper_downloader_test_") as test_dir:
        target_path = os.path.join(test_dir, "arxiv_2106.14834.pdf")
        downloader = PaperDownloader()
        success = downloader.download_pdf(url, target_path)
        assert success
        assert os.path.exists(target_path)
        assert os.path.getsize(target_path) > 0

def test_download_pdfs():
    test_urls = [
        ("https://aclanthology.org/2022.acl-long.1.pdf", "2022.acl-long.1.pdf"),
        ("https://aclanthology.org/2022.acl-long.2.pdf", "2022.acl-long.2.pdf"),
        ("https://aclanthology.org/2022.acl-long.3.pdf", "2022.acl-long.3.pdf"),
        ("https://aclanthology.org/2022.acl-long.4.pdf", "2022.acl-long.4.pdf"),
    ]
    with tempfile.TemporaryDirectory(prefix="paper_downloader_test_") as test_dir:
        url_path_list = [(url, os.path.join(test_dir, fname)) for url, fname in test_urls]
        downloader = PaperDownloader()
        results = downloader.download_pdfs(url_path_list, wait_time=0.1)
        for (url, path, success) in results:
            assert success
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0 