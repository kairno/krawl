import os
import requests
import time
from typing import List, Tuple # For type hinting

class PaperDownloader:
    def download_pdf(self, url: str, target_path: str, skip_if_exists: bool = True) -> bool:
        """
        Download a single PDF from url to target_path.
        Returns True if successful, False otherwise.
        """
        if skip_if_exists and os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            print(f"[SKIP] File '{target_path}' already exists and is not empty.")
            return True
        
        # Ensure the directory for the target path exists
        try:
            # Check if target_path is a file path and get its directory
            dir_name = os.path.dirname(target_path)
            if dir_name: # Ensure dir_name is not empty (e.g. if target_path is just a filename)
                os.makedirs(dir_name, exist_ok=True)
        except Exception as e:
            print(f"[FAIL] Could not create directory for '{target_path}': {e}")
            return False

        try:
            with requests.Session() as session:
                resp = session.get(url, timeout=30, stream=True)
                resp.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
                
                with open(target_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192): # Download in chunks
                        f.write(chunk)
            
            print(f"[OK] Downloaded: '{target_path}' from '{url}'")
            return True
        except Exception as e: # Simplified error handling
            print(f"[FAIL] Could not download '{url}' to '{target_path}': {e}")
            # Attempt to clean up partially downloaded file
            if os.path.exists(target_path):
                try:
                    os.remove(target_path)
                    # print(f"[INFO] Removed partially downloaded file: '{target_path}'") # Optional: can be noisy
                except OSError:
                    pass # Silently ignore if removal fails
            return False

    def download_pdfs(self, url_path_list: List[Tuple[str, str]], skip_if_exists: bool = True, wait_time: float = 1.0) -> List[Tuple[str, str, bool]]:
        """
        Download multiple PDFs from a list of (url, target_path) tuples.
        
        Args:
            url_path_list: A list of tuples, where each tuple is (url, target_path).
            skip_if_exists: If True, skips downloading if the target file already exists.
            wait_time: Seconds to wait between downloads (default 1.0).

        Returns:
            A list of (url, target_path, success_status) tuples.
        """
        results: List[Tuple[str, str, bool]] = []
        for i, (url, target_path) in enumerate(url_path_list):
            print(f"\nProcessing task {i+1}/{len(url_path_list)}: URL '{url}' -> Path '{target_path}'")
            success = self.download_pdf(url, target_path, skip_if_exists=skip_if_exists)
            results.append((url, target_path, success))
            
            if i < len(url_path_list) - 1: # Don't wait after the last download
                if success: # Optional: only wait if the download was attempted (not skipped early)
                    print(f"[INFO] Waiting for {wait_time} seconds before next download...")
                    time.sleep(wait_time)
                else: # If failed, maybe wait a bit less or not at all? For now, same wait.
                    print(f"[INFO] Waiting for {wait_time} seconds (after fail) before next download...")
                    time.sleep(wait_time)

        return results

if __name__ == "__main__":

    # RUN: python -m krawl.downloader.paper_downloader

    import tempfile
    
    with tempfile.TemporaryDirectory(prefix="paper_downloader_test_") as test_dir:
        print(f"Created temporary directory for testing: {test_dir}")

        # Define download tasks as a list of (url, target_path) tuples
        download_tasks_data = [
            ("https://arxiv.org/pdf/2106.14834.pdf", "arxiv_2106.14834.pdf"),
            ("https://aclanthology.org/2022.acl-long.1.pdf", "2022.acl-long.1.pdf"),
            ("https://aclanthology.org/2022.acl-long.2.pdf", "2022.acl-long.2.pdf"),
            ("https://example.com/nonexistent.pdf", "nonexistent.pdf"), # URL that will likely fail
            ("https://arxiv.org/pdf/cond-mat/0205245v1.pdf", "cond-mat-0205245v1.pdf"),
        ]

        # Construct the full target paths
        tasks_to_process: List[Tuple[str, str]] = [
            (url, os.path.join(test_dir, filename)) for url, filename in download_tasks_data
        ]

        downloader = PaperDownloader()
        print(f"\nStarting to download {len(tasks_to_process)} test PDFs to '{test_dir}'...")
        
        processed_results = downloader.download_pdfs(tasks_to_process, wait_time=1.0)
        
        print("\n--- Download Results ---")
        successful_downloads = 0
        for url, path, success in processed_results:
            status = "OK" if success else "FAIL"
            print(f"URL: {url}\n  Path: {path}\n  Status: {status}")
            if success:
                successful_downloads +=1
            print("-" * 20)
        
        print(f"\nSummary: {successful_downloads}/{len(processed_results)} downloads successful.")
        print(f"Files were downloaded to (and automatically cleaned up from): {test_dir}")

    print("\nTemporary directory and its contents have been removed.")
