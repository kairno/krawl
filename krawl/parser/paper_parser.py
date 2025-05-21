import glob
from pathlib import Path
from dataclasses import dataclass
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import subprocess
import time
from grobid_client.grobid_client import GrobidClient
import os
import traceback
import requests 
import shutil 
import tempfile 

class GrobidManager:
    def __init__(self, container_name="grobid_container", image="grobid/grobid:0.8.2", config_path="./krawl/parser/config/config.json"):
        self.container_name = container_name
        self.image = image
        self.config_path = config_path # Path to grobid-client-python config file
        self.client = None
        self._container_started_by_this_instance = False # Track if this instance started the container

    def _is_grobid_api_alive(self):
        """Checks if the GROBID API is responsive."""
        try:
            url = f"http://localhost:8070/api/isalive"
            
            with requests.Session() as s:
                resp = s.get(url, timeout=10) 

            is_alive_text = resp.text.strip().lower()
            print(f"[HEALTH CHECK] GROBID /api/isalive response: '{is_alive_text}' (status {resp.status_code})")
            
            if resp.status_code == 200:
                return is_alive_text == 'true'
            else:
                print(f"[HEALTH CHECK FAIL] GROBID API isalive check failed with status: {resp.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"[HEALTH CHECK FAIL] ConnectionError when checking GROBID API. Service might not be ready or port not exposed.")
            return False
        except requests.exceptions.Timeout:
            print(f"[HEALTH CHECK FAIL] Timeout when checking GROBID API.")
            return False
        except Exception as e:
            print(f"[HEALTH CHECK FAIL] Exception during GROBID API health check: {e}")
            traceback.print_exc()
            return False

    def is_container_running_and_healthy(self):
        """Checks if the named container is running and GROBID API is alive."""
        try:
            inspect_cmd = ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name]
            result = subprocess.run(inspect_cmd, capture_output=True, text=True, check=False)
            
            if result.returncode != 0 or result.stdout.strip() != "true":
                return False
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            return False
        except Exception as e:
            print(f"Error checking Docker container status for '{self.container_name}': {e}")
            traceback.print_exc()
            return False
        
        print(f"Container '{self.container_name}' is reported as running by Docker. Checking API health...")
        return self._is_grobid_api_alive()

    def fetch_container_logs(self):
        """Fetches and prints the last logs from the container."""
        print(f"Fetching logs for container '{self.container_name}'...")
        try:
            log_cmd = ["docker", "logs", "--tail", "100", self.container_name] 
            log_result = subprocess.run(log_cmd, capture_output=True, text=True, check=False)
            
            if log_result.stdout:
                print(f"--- Last 100 lines of STDOUT logs for {self.container_name} ---")
                print(log_result.stdout.strip())
                print("--- End of STDOUT logs ---")
            else:
                print(f"No stdout logs from container '{self.container_name}'.")

            if log_result.stderr:
                print(f"--- Last 100 lines of STDERR logs for {self.container_name} ---")
                print(log_result.stderr.strip())
                print("--- End of STDERR logs ---")
            
            if not log_result.stdout and not log_result.stderr:
                print("No logs (stdout/stderr) captured from the container.")
                
        except FileNotFoundError:
            print("Docker command not found. Cannot fetch logs.")
        except Exception as e:
            print(f"Error fetching Docker logs for '{self.container_name}': {e}")
            traceback.print_exc()

    def start(self):
        if self.is_container_running_and_healthy():
            print(f"GROBID container '{self.container_name}' is already running and healthy.")
            try:
                self.client = GrobidClient(config_path=self.config_path)
                print("GrobidClient initialized successfully using existing healthy service.")
            except Exception as e:
                print(f"Failed to initialize GrobidClient with config '{self.config_path}' despite service appearing healthy: {e}")
                traceback.print_exc()
                raise RuntimeError(f"GrobidClient initialization failed. Service was healthy but client could not init.") from e
            return

        print(f"Attempting to start GROBID Docker container '{self.container_name}' with image '{self.image}'...")
        self._container_started_by_this_instance = False 

        try:
            inspect_result = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
            if inspect_result.returncode == 0:
                print(f"Container '{self.container_name}' already exists. Removing it before starting a new one.")
                subprocess.run(["docker", "rm", "-f", self.container_name], check=True, capture_output=True, text=True)
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            raise RuntimeError("Docker not found, cannot manage GROBID container.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to remove existing container '{self.container_name}': {e.stderr.strip()}")

        cmd = [
            "docker", "run", "--rm", 
            "--gpus", "all", 
            "--init", 
            "--ulimit", "core=0",
            "-p", "8070:8070", 
            "-d", 
            "--name", self.container_name, 
            self.image
        ]

        try:
            print(f"Running docker command: {' '.join(cmd)}")
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"GROBID container '{self.container_name}' start command issued. Docker run stdout: {proc.stdout.strip()}")
            if proc.stderr:
                print(f"Docker run stderr: {proc.stderr.strip()}")
            self._container_started_by_this_instance = True 
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            raise RuntimeError("Docker not found, cannot start GROBID.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to start GROBID Docker container '{self.container_name}'.")
            print(f"Return code: {e.returncode}")
            print(f"Stdout: {e.stdout.strip()}")
            print(f"Stderr: {e.stderr.strip()}")
            traceback.print_exc()
            self.fetch_container_logs() 
            raise RuntimeError(f"Failed to start GROBID container. Check Docker errors and container logs.") from e

        print("Waiting for GROBID service to become ready...")
        time.sleep(10) 

        max_retries = 30 
        wait_interval = 5 
        for i in range(max_retries):
            print(f"Health check attempt {i+1}/{max_retries} for '{self.container_name}'...")
            if self.is_container_running_and_healthy():
                print(f"GROBID service in container '{self.container_name}' is up and healthy after ~{ (i * wait_interval) + 10 } seconds.")
                try:
                    if not self._is_grobid_api_alive():
                        print("GROBID API was healthy but became unresponsive just before client initialization.")
                        self.fetch_container_logs()
                        self.stop() 
                        raise RuntimeError("GROBID service became unresponsive after initial health checks.")
                    
                    self.client = GrobidClient(config_path=self.config_path)
                    print("GrobidClient initialized successfully.")
                    return 
                except Exception as e:
                    print(f"Failed to initialize GrobidClient with config '{self.config_path}' after service startup: {e}")
                    traceback.print_exc()
                    self.fetch_container_logs()
                    self.stop() 
                    raise RuntimeError(f"GrobidClient initialization failed after service startup.") from e
            
            if i < max_retries -1 : 
                 print(f"GROBID not yet healthy. Waiting for {wait_interval} seconds...")
                 time.sleep(wait_interval)

        print(f"GROBID service in container '{self.container_name}' did not become healthy in time.")
        self.fetch_container_logs()
        self.stop() 
        raise RuntimeError(f"GROBID service in '{self.container_name}' did not start or become healthy in time. Check container logs.")

    def stop(self):
        if self.client:
            self.client = None 

        print(f"Attempting to stop GROBID Docker container '{self.container_name}'...")
        try:
            inspect_proc = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
            if inspect_proc.returncode != 0:
                print(f"Container '{self.container_name}' does not exist or already removed. No stop action needed.")
                return

            print(f"Stopping container '{self.container_name}'...")
            stop_proc = subprocess.run(["docker", "stop", self.container_name], capture_output=True, text=True, check=False, timeout=30)
            if stop_proc.returncode == 0:
                print(f"Container '{self.container_name}' stopped successfully.")
            else:
                print(f"Warning: 'docker stop {self.container_name}' exited with code {stop_proc.returncode}. Stderr: {stop_proc.stderr.strip()}")
                final_check = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
                if final_check.returncode != 0:
                     print(f"Container '{self.container_name}' is confirmed to be gone after stop attempt (likely due to --rm or already stopped).")
                else:
                     print(f"Container '{self.container_name}' may still be running or in an error state. Consider manual 'docker rm -f {self.container_name}'.")
            
            if self._container_started_by_this_instance:
                time.sleep(2) 
                final_check = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
                if final_check.returncode == 0:
                    print(f"Container '{self.container_name}' (started by this instance) still exists after stop. Forcing removal.")
                    subprocess.run(["docker", "rm", "-f", self.container_name], capture_output=True, text=True, check=False)

        except FileNotFoundError:
            print("Docker command not found. Cannot stop/remove container.")
        except subprocess.TimeoutExpired:
            print(f"Timeout trying to stop container '{self.container_name}'. It might be unresponsive. Consider manual 'docker rm -f {self.container_name}'.")
        except Exception as e:
            print(f"An unexpected error occurred while stopping/removing container '{self.container_name}': {e}")
            traceback.print_exc()
        finally:
            self._container_started_by_this_instance = False 


def read_tei(tei_file):
    with open(tei_file, "r", encoding="utf-8") as tei:
        soup = BeautifulSoup(tei, "lxml") 
    return soup

def elem_to_text(elem, default=""):
    if elem:
        return elem.get_text(separator=" ", strip=True)
    else:
        return default

@dataclass
class TEIFile:
    filename: str

    def __post_init__(self):
        self.soup = read_tei(self.filename)
        self._text = None
        self._title = None
        self._abstract = None

    def basename(self):
        stem = Path(self.filename).stem
        if stem.endswith(".tei"): 
            return stem[:-4] 
        return stem

    @property
    def title(self):
        if self._title is None:
            title_elem = self.soup.select_one("teiHeader > fileDesc > titleStmt > title")
            self._title = elem_to_text(title_elem)
        return self._title

    @property
    def abstract(self):
        if self._abstract is None:
            abstract_elem = self.soup.select_one("teiHeader > profileDesc > abstract")
            self._abstract = elem_to_text(abstract_elem, default=None) 
        return self._abstract

    @property
    def text(self):
        if self._text is None:
            divs_text = []
            body = self.soup.find("body")
            if body:
                for div_candidate in body.find_all("div"):
                    div_text = div_candidate.get_text(separator=" ", strip=True)
                    divs_text.append(div_text)
            
            if not divs_text and body: 
                self._text = body.get_text(separator=" ", strip=True)
            else:
                self._text = " ".join(divs_text)
        return self._text


def get_dataframe(path_to_extraction_folder, k=None):
    tei_files_pattern = str(Path(path_to_extraction_folder) / "*.tei.xml")
    list_files = glob.glob(tei_files_pattern)
    
    if not list_files:
        print(f"No *.tei.xml files found in {path_to_extraction_folder}")
        return pd.DataFrame(columns=["ACL_id", "title", "abstract", "full_text"])

    if k is not None:
        list_files = list_files[:k]
    
    df = pd.DataFrame(list_files, columns=["path"])

    tqdm.pandas(desc="Parsing TEI files")
    df["tei"] = df["path"].progress_apply(lambda p: TEIFile(p))
    
    def extract_tei_data(tei_file_obj):
        return {
            "ACL_id": tei_file_obj.basename(),
            "title": tei_file_obj.title,
            "abstract": tei_file_obj.abstract,
            "full_text": tei_file_obj.text
        }
    
    extracted_data = df["tei"].progress_apply(extract_tei_data)
    df = pd.concat([df.drop(columns=['tei']), extracted_data.apply(pd.Series)], axis=1)
    
    df = df.drop(columns=["path"], errors='ignore') 
    return df


class PaperParser:
    def __init__(self, input_pdf_dir, output_dir, consolidate_citations=False, tei_coordinates=False, force=False, config_path="./krawl/parser/config/config.json", processing_batch_size=1):
        self.input_pdf_dir = str(input_pdf_dir) 
        self.output_dir = str(output_dir)
        self.consolidate_citations = consolidate_citations
        self.tei_coordinates = tei_coordinates
        self.force = force
        self.processing_batch_size = processing_batch_size
        
        self.grobid = GrobidManager(config_path=config_path)

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        try:
            self.grobid.start() 
            
            client = self.grobid.client
            if not client:
                print("GROBID client not initialized after start method. This should have been caught by GrobidManager.start(). Aborting.")
                raise RuntimeError("GROBID client not initialized.")

            all_pdf_files_glob = glob.glob(os.path.join(self.input_pdf_dir, "*.pdf"))
            if not all_pdf_files_glob:
                print(f"No PDF files found in input directory: {self.input_pdf_dir}")
                return 

            pdf_files_to_process = []
            skipped_count = 0
            if not self.force:
                print("Checking for already processed files (force=False)...")
                for pdf_path in all_pdf_files_glob:
                    pdf_filename_stem = Path(pdf_path).stem
                    # Expected TEI output filename based on GROBID's default naming convention
                    expected_tei_filename = f"{pdf_filename_stem}.grobid.tei.xml"
                    expected_tei_path = Path(self.output_dir) / expected_tei_filename
                    if expected_tei_path.exists():
                        print(f"Skipping '{pdf_path}': Output '{expected_tei_path}' already exists.")
                        skipped_count += 1
                    else:
                        pdf_files_to_process.append(pdf_path)
                print(f"Found {len(all_pdf_files_glob)} total PDFs. Skipped {skipped_count} already processed files.")
            else:
                print("Force processing enabled. All PDF files will be processed.")
                pdf_files_to_process = all_pdf_files_glob
            
            if not pdf_files_to_process:
                print("No new PDF files to process.")
                return

            total_pdf_to_process_count = len(pdf_files_to_process)
            print(f"Found {total_pdf_to_process_count} PDF(s) to process in '{self.input_pdf_dir}'. Processing in batches of {self.processing_batch_size}.")

            processed_pdf_count_successfully = 0

            for i in range(0, total_pdf_to_process_count, self.processing_batch_size):
                batch_pdf_files = pdf_files_to_process[i:i + self.processing_batch_size]
                current_batch_number = (i // self.processing_batch_size) + 1
                total_batches = (total_pdf_to_process_count + self.processing_batch_size - 1) // self.processing_batch_size
                
                print(f"\nProcessing batch {current_batch_number}/{total_batches} ({len(batch_pdf_files)} PDFs)...")

                with tempfile.TemporaryDirectory(prefix="grobid_batch_") as temp_batch_dir:
                    print(f"Created temporary batch directory: {temp_batch_dir}")
                    
                    copied_files_for_batch = []
                    for pdf_path_in_batch in batch_pdf_files:
                        try:
                            base_filename = os.path.basename(pdf_path_in_batch)
                            temp_pdf_target_path = os.path.join(temp_batch_dir, base_filename)
                            shutil.copy2(pdf_path_in_batch, temp_pdf_target_path) 
                            copied_files_for_batch.append(temp_pdf_target_path)
                        except Exception as copy_e:
                            print(f"Error copying PDF '{pdf_path_in_batch}' to temporary batch directory: {copy_e}")
                    
                    if not copied_files_for_batch:
                        print(f"No PDF files were successfully copied to temporary batch directory '{temp_batch_dir}' for batch {current_batch_number}. Skipping this batch.")
                        continue

                    print(f"Copied {len(copied_files_for_batch)} PDFs to temporary directory for processing.")

                    try:
                        client.process(
                            "processFulltextDocument", 
                            temp_batch_dir, 
                            output=self.output_dir, 
                            consolidate_citations=self.consolidate_citations,
                            tei_coordinates=self.tei_coordinates,
                            force=self.force # This force is for GROBID client, not our script's skip logic
                        )
                        processed_pdf_count_successfully += len(copied_files_for_batch) 
                        print(f"Batch {current_batch_number}/{total_batches} submitted to GROBID.")
                    except requests.exceptions.ConnectionError as conn_err:
                        print(f"ConnectionError during GROBID processing for batch {current_batch_number}: {conn_err}")
                        print("This usually means the GROBID service became unresponsive or shut down unexpectedly.")
                        self.grobid.fetch_container_logs() 
                        raise RuntimeError(f"GROBID service connection failed during processing batch {current_batch_number}.") from conn_err
                    except Exception as proc_err: 
                        print(f"Error during GROBID client.process for batch {current_batch_number}: {proc_err}")
                        self.grobid.fetch_container_logs()
                        raise 

            final_tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
            print(f"\nFinished processing all batches.")
            print(f"Successfully submitted {processed_pdf_count_successfully} PDF(s) to GROBID across all batches.")
            print(f"Total TEI XML files in '{self.output_dir}': {final_tei_count}.")

        except RuntimeError as e:
            print(f"A runtime error occurred during PaperParser execution: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred in PaperParser.run: {e}")
            traceback.print_exc()
            self.grobid.fetch_container_logs() 
            raise
        finally:
            print("PaperParser run finished. Attempting to stop GROBID manager...")
            self.grobid.stop()

    def summary(self):
        pdf_count = len(glob.glob(os.path.join(self.input_pdf_dir, "*.pdf")))
        tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
        return {"pdf_count": pdf_count, "tei_count": tei_count, "output_dir": self.output_dir}


if __name__ == "__main__":

    # RUN: python -m krawl.parser.paper_parser

    print("Starting GROBID TEI Parser script...")
    
    input_pdf_dir_path = "./tests/test_data/pdfs"
    output_dir_path = "./tests/test_data/parses"

    print(f"Input PDF directory: {input_pdf_dir_path}")
    print(f"Output TEI directory: {output_dir_path}")

    parser = PaperParser(
        input_pdf_dir=input_pdf_dir_path,
        output_dir=output_dir_path,
        consolidate_citations=True,
        tei_coordinates=True,
        force=False, 
        config_path="./krawl/parser/config/config.json",
        processing_batch_size=1
    )
    
    try:
        parser.run()
        summary_info = parser.summary()
        print("Processing summary:")
        for key, value in summary_info.items():
            print(f"  {key}: {value}")
    except RuntimeError as e:
        print(f"Script failed with RuntimeError: {e}")
    except Exception as e:
        print(f"Script failed with an unexpected error: {e}")
        traceback.print_exc()

