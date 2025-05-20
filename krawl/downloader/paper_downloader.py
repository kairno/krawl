import os
import requests
import time

class PaperDownloader:
    def download_pdf(self, url, target_path, skip_if_exists=True):
        """
        Download a single PDF from url to target_path.
        Returns True if successful, False otherwise.
        """
        if skip_if_exists and os.path.exists(target_path):
            print(f"[SKIP] {target_path} already exists.")
            return True
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            with open(target_path, "wb") as f:
                f.write(resp.content)
            print(f"[OK] Downloaded: {target_path}")
            return True
        except Exception as e:
            print(f"[FAIL] Could not download {url} to {target_path}: {e}")
            return False

    def download_pdfs(self, url_path_list, skip_if_exists=True, wait_time=1.0):
        """
        Download multiple PDFs from a list of (url, target_path) tuples.
        Returns a list of (url, target_path, success) results.
        wait_time: seconds to wait between downloads (default 1.0)
        """
        results = []
        for idx, (url, target_path) in enumerate(url_path_list):
            success = self.download_pdf(url, target_path, skip_if_exists=skip_if_exists)
            results.append((url, target_path, success))
            if idx < len(url_path_list) - 1:
                time.sleep(wait_time)
        return results

if __name__ == "__main__":

    # RUN: python -m krawl.downloader.paper_downloader

    import tempfile
    import shutil
    
    # Create a dedicated temp subdirectory
    test_dir = tempfile.mkdtemp(prefix="paper_downloader_test_")

    # Example URLs (replace with real open-access PDF URLs for a real test)
    test_urls = [
        ("https://aclanthology.org/2022.acl-long.1.pdf", os.path.join(test_dir, "2022.acl-long.1.pdf")),
        ("https://aclanthology.org/2022.acl-long.2.pdf", os.path.join(test_dir, "2022.acl-long.2.pdf")),
        ("https://aclanthology.org/2022.acl-long.3.pdf", os.path.join(test_dir, "2022.acl-long.3.pdf")),
        ("https://aclanthology.org/2022.acl-long.4.pdf", os.path.join(test_dir, "2022.acl-long.4.pdf")),
    ]
    downloader = PaperDownloader()
    print(f"Downloading {len(test_urls)} test PDFs to {test_dir}...")
    results = downloader.download_pdfs(test_urls, wait_time=1.0)
    print("\nResults:")
    for url, path, success in results:
        print(f"{url} -> {path}: {'OK' if success else 'FAIL'}")

    # delete only the test subdirectory
    print(f"Deleting {test_dir}...")
    shutil.rmtree(test_dir)