import os
import requests
import time
from typing import List, Tuple, Union # For type hinting
from enum import Enum, auto

class DownloadStatus(Enum):
    SUCCESS_DOWNLOADED = auto()
    SUCCESS_SKIPPED_EXISTS = auto()
    FAILED_DOWNLOAD_ERROR = auto()
    FAILED_DIRECTORY_CREATION = auto()

class PaperDownloader:
    def download_pdf(self, url: str, target_path: str, skip_if_exists: bool = True) -> DownloadStatus:
        """
        Download a single PDF from url to target_path.
        
        Returns:
            DownloadStatus: An enum indicating the outcome of the download attempt.
        """
        if skip_if_exists and os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            print(f"[SKIP] File '{target_path}' already exists and is not empty.")
            return DownloadStatus.SUCCESS_SKIPPED_EXISTS
        
        # Ensure the directory for the target path exists
        try:
            dir_name = os.path.dirname(target_path)
            if dir_name: # Ensure dir_name is not empty (e.g. if target_path is just a filename)
                os.makedirs(dir_name, exist_ok=True)
        except Exception as e:
            print(f"[FAIL] Could not create directory for '{target_path}': {e}")
            return DownloadStatus.FAILED_DIRECTORY_CREATION

        try:
            print(f"[INFO] Attempting to download: '{target_path}' from '{url}'")
            with requests.Session() as session:
                # Add a common user-agent
                headers = {
                    'User-Agent': 'krawl/0.1 (Paper Downloader; +https://github.com/your_repo/krawl)'
                }
                resp = session.get(url, headers=headers, timeout=60, stream=True) # Increased timeout
                resp.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
                
                with open(target_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192 * 4): # Download in larger chunks
                        f.write(chunk)
            
            print(f"[OK] Downloaded: '{target_path}'")
            return DownloadStatus.SUCCESS_DOWNLOADED
        except requests.exceptions.RequestException as e: # More specific exception for network/HTTP errors
            print(f"[FAIL] Network/HTTP error downloading '{url}' to '{target_path}': {e}")
            self._cleanup_failed_download(target_path)
            return DownloadStatus.FAILED_DOWNLOAD_ERROR
        except Exception as e: # Catch other potential errors
            print(f"[FAIL] Unexpected error downloading '{url}' to '{target_path}': {e}")
            self._cleanup_failed_download(target_path)
            return DownloadStatus.FAILED_DOWNLOAD_ERROR

    def _cleanup_failed_download(self, target_path: str):
        """Attempts to remove a partially downloaded file."""
        if os.path.exists(target_path):
            try:
                # Check if the file is empty or very small, indicative of a failed download
                if os.path.getsize(target_path) < 1024: # Arbitrary small size
                     print(f"[INFO] Cleaning up potentially incomplete file: '{target_path}'")
                os.remove(target_path)
            except OSError as e:
                print(f"[WARN] Could not remove partially downloaded file '{target_path}': {e}")


    def download_pdfs(self, url_path_list: List[Tuple[str, str]], skip_if_exists: bool = True, wait_time: float = 1.0) -> List[Tuple[str, str, DownloadStatus]]:
        """
        Download multiple PDFs from a list of (url, target_path) tuples.
        
        Args:
            url_path_list: A list of tuples, where each tuple is (url, target_path).
            skip_if_exists: If True, skips downloading if the target file already exists.
            wait_time: Seconds to wait between downloads if a download attempt was made (default 1.0).

        Returns:
            A list of (url, target_path, DownloadStatus) tuples.
        """
        results: List[Tuple[str, str, DownloadStatus]] = []
        for i, (url, target_path) in enumerate(url_path_list):
            print(f"\nProcessing task {i+1}/{len(url_path_list)}: URL '{url}' -> Path '{target_path}'")
            
            status = self.download_pdf(url, target_path, skip_if_exists=skip_if_exists)
            results.append((url, target_path, status))
            
            if i < len(url_path_list) - 1: # Don't wait after the last download
                # Only wait if a download attempt was actually made (i.e., not skipped)
                # or if a directory creation failed (which is an attempt before download)
                if status == DownloadStatus.SUCCESS_DOWNLOADED or \
                   status == DownloadStatus.FAILED_DOWNLOAD_ERROR or \
                   status == DownloadStatus.FAILED_DIRECTORY_CREATION:
                    print(f"[INFO] Waiting for {wait_time} seconds before next download (status: {status.name})...")
                    time.sleep(wait_time)
                elif status == DownloadStatus.SUCCESS_SKIPPED_EXISTS:
                    print(f"[INFO] File skipped, no wait needed.")
                # else: # Should not happen if DownloadStatus enum is exhaustive for download_pdf outcomes
                    # print(f"[WARN] Unknown status for waiting: {status.name}")


        return results

if __name__ == "__main__":

    # RUN: python -m krawl.downloader.paper_downloader # Assuming your package structure

    import tempfile
    
    with tempfile.TemporaryDirectory(prefix="paper_downloader_test_") as test_dir:
        print(f"Created temporary directory for testing: {test_dir}")

        # Define download tasks as a list of (url, target_path) tuples
        download_tasks_data = [
            ("https://arxiv.org/pdf/2106.14834.pdf", "arxiv_2106.14834.pdf"),      # Will download
            ("https://aclanthology.org/2022.acl-long.1.pdf", "2022.acl-long.1.pdf"), # Will download
            ("https://aclanthology.org/2022.acl-long.1.pdf", "2022.acl-long.1_duplicate.pdf"), # Test skip: same URL, diff name
            ("https://example.com/nonexistent.pdf", "nonexistent.pdf"),            # URL that will likely fail
            ("https://arxiv.org/pdf/cond-mat/0205245v1.pdf", "cond-mat-0205245v1.pdf"), # Will download
        ]

        # Construct the full target paths
        tasks_to_process: List[Tuple[str, str]] = [
            (url, os.path.join(test_dir, filename)) for url, filename in download_tasks_data
        ]
        
        # Add one task that will be skipped
        first_task_path = tasks_to_process[0][1]
        if not os.path.exists(os.path.dirname(first_task_path)):
             os.makedirs(os.path.dirname(first_task_path))
        with open(first_task_path, "w") as f: # Create a dummy file to test skip
            f.write("dummy content for skip test")
        print(f"[SETUP] Created dummy file for skip test: {first_task_path}")


        downloader = PaperDownloader()
        print(f"\nStarting to download {len(tasks_to_process)} test PDFs to '{test_dir}'...")
        
        processed_results = downloader.download_pdfs(tasks_to_process, wait_time=1.5, skip_if_exists=True)
        
        print("\n--- Download Results ---")
        successful_operations = 0 # Includes actual downloads and successful skips
        actual_downloads = 0

        for url, path, status_obj in processed_results:
            print(f"URL: {url}\n  Path: {path}\n  Status: {status_obj.name}")
            if status_obj == DownloadStatus.SUCCESS_DOWNLOADED:
                successful_operations +=1
                actual_downloads += 1
            elif status_obj == DownloadStatus.SUCCESS_SKIPPED_EXISTS:
                successful_operations +=1
            print("-" * 20)
        
        print(f"\nSummary: {successful_operations}/{len(processed_results)} operations successful.")
        print(f"         {actual_downloads} actual new files downloaded.")
        print(f"Files were processed in (and automatically cleaned up from): {test_dir}")

    print("\nTemporary directory and its contents have been removed.")
