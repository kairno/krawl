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


class GrobidManager:
    def __init__(self, container_name="grobid_container", image="grobid/grobid:0.8.2", config_path="./krawl/parser/config/config.json"):
        self.container_name = container_name
        self.image = image
        self.config_path = config_path
        self.client = None

    def _is_grobid_api_alive(self):
        """Checks if the GROBID API is responsive."""
        try:
            url = f"http://localhost:8070/api/isalive"
            # It's good practice to ensure your config_path leads to a config where
            # grobid_server and grobid_port are set, in case GrobidClient uses them.
            # For the health check, we directly use localhost:8070 as it's standard.
            
            with requests.Session() as s:
                resp = s.get(url, timeout=10) # Increased timeout slightly

            # print(f"GROBID /api/isalive response: '{resp.text.strip()}' (status {resp.status_code})") # For debugging
            if resp.status_code == 200:
                return resp.text.strip().lower() == 'true'
            else:
                # print(f"GROBID API isalive check failed with status: {resp.status_code}") # For debugging
                return False
        except requests.exceptions.ConnectionError:
            # print(f"ConnectionError when checking GROBID API. Service might not be ready or port not exposed.") # For debugging
            return False
        except requests.exceptions.Timeout:
            # print(f"Timeout when checking GROBID API.") # For debugging
            return False
        except Exception as e:
            print(f"Exception during GROBID API health check: {e}")
            traceback.print_exc()
            return False

    def is_container_running_and_healthy(self):
        """Checks if the named container is running and GROBID API is alive."""
        try:
            # Check if the container is running via Docker inspect
            inspect_cmd = ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name]
            result = subprocess.run(inspect_cmd, capture_output=True, text=True, check=False)
            
            if result.returncode != 0 or result.stdout.strip() != "true":
                # print(f"Container '{self.container_name}' not running or does not exist.") # For debugging
                # if result.stderr:
                # print(f"Docker inspect stderr: {result.stderr.strip()}") # For debugging
                return False
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            return False
        except Exception as e:
            print(f"Error checking Docker container status for '{self.container_name}': {e}")
            traceback.print_exc()
            return False
        
        # If container is reported as running by Docker, check GROBID API health
        return self._is_grobid_api_alive()

    def fetch_container_logs(self):
        """Fetches and prints the last logs from the container."""
        print(f"Fetching logs for container '{self.container_name}'...")
        try:
            log_cmd = ["docker", "logs", "--tail", "50", self.container_name]
            log_result = subprocess.run(log_cmd, capture_output=True, text=True, check=False) # Don't check=True, container might be gone or stopped
            
            if log_result.stdout:
                print(f"--- Last 50 lines of logs for {self.container_name} ---")
                print(log_result.stdout.strip())
                print("--- End of logs ---")
            else:
                print(f"No stdout logs from container '{self.container_name}'.")

            if log_result.stderr:
                print(f"--- Stderr from 'docker logs {self.container_name}' (may include stdout if container writes logs to stderr) ---")
                print(log_result.stderr.strip())
                print("--- End of stderr for logs ---")
            
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
                print("GrobidClient initialized successfully.")
            except Exception as e:
                print(f"Failed to initialize GrobidClient with config '{self.config_path}': {e}")
                traceback.print_exc()
                raise RuntimeError(f"GrobidClient initialization failed even though service appears healthy.") from e
            return

        print(f"Attempting to start GROBID Docker container '{self.container_name}' with image '{self.image}'...")
        
        # Check if a container with the same name exists (even if stopped) and remove it
        try:
            inspect_result = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
            if inspect_result.returncode == 0: # Container exists
                print(f"Container '{self.container_name}' already exists. Removing it before starting a new one.")
                subprocess.run(["docker", "rm", "-f", self.container_name], check=True, capture_output=True, text=True)
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            raise RuntimeError("Docker not found, cannot manage GROBID container.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to remove existing container '{self.container_name}': {e.stderr.strip()}")
            # Decide if this is fatal. For now, let's attempt to proceed.

        # Construct the docker run command from user's original script
        cmd = [
            "docker", "run", "--rm", 
            "--gpus", "all",  # If you don't have GPUs or nvidia-docker, this might cause issues.
            "--init", 
            "--ulimit", "core=0",
            "-p", "8070:8070", 
            "-d", # Detached mode
            "--name", self.container_name, 
            self.image
        ]

        try:
            print(f"Running docker command: {' '.join(cmd)}")
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"GROBID container '{self.container_name}' started. Docker run stdout: {proc.stdout.strip()}")
            if proc.stderr: # Some info might go to stderr even on success
                print(f"Docker run stderr: {proc.stderr.strip()}")
        except FileNotFoundError:
            print("Docker command not found. Please ensure Docker is installed and in PATH.")
            raise RuntimeError("Docker not found, cannot start GROBID.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to start GROBID Docker container '{self.container_name}'.")
            print(f"Return code: {e.returncode}")
            print(f"Stdout: {e.stdout.strip()}")
            print(f"Stderr: {e.stderr.strip()}") # This is often the most informative part
            traceback.print_exc()
            # Attempt to get logs even if run command failed, container might have briefly started
            self.fetch_container_logs() 
            raise RuntimeError(f"Failed to start GROBID container. Check Docker errors above and container logs.") from e

        print("Waiting for GROBID service to become ready...")
        time.sleep(5) # Initial grace period for container to initialize

        max_retries = 30 # e.g., 30 * 5s = 150 seconds total wait time
        for i in range(max_retries):
            if self.is_container_running_and_healthy(): # Checks both container and API
                print(f"GROBID service in container '{self.container_name}' is up and healthy after ~{ (i+1)*5 + 5 } seconds.")
                try:
                    self.client = GrobidClient(config_path=self.config_path)
                    print("GrobidClient initialized successfully.")
                except Exception as e:
                    print(f"Failed to initialize GrobidClient with config '{self.config_path}': {e}")
                    traceback.print_exc()
                    self.stop(force_remove_if_started_by_script=True) # Clean up
                    raise RuntimeError(f"GrobidClient initialization failed after service startup.") from e
                return
            print(f"Waiting for GROBID... (attempt {i+1}/{max_retries})")
            time.sleep(5)

        print(f"GROBID service in container '{self.container_name}' did not become healthy in time.")
        self.fetch_container_logs() # Crucial for diagnosing internal GROBID issues
        self.stop(force_remove_if_started_by_script=True) # Attempt to clean up the container we tried to start
        raise RuntimeError(f"GROBID service in '{self.container_name}' did not start in time. Check container logs above.")

    def stop(self, force_remove_if_started_by_script=False):
        # This method is called to stop the container managed by this instance.
        # force_remove_if_started_by_script is a flag to indicate if we should ensure removal
        # if this GrobidManager instance was the one that started it and it failed.
        
        if self.client:
            self.client = None # Clear client instance

        print(f"Attempting to stop and/or remove GROBID Docker container '{self.container_name}'...")
        try:
            # Check if container exists
            inspect_proc = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
            if inspect_proc.returncode != 0:
                print(f"Container '{self.container_name}' does not exist or already removed. No action needed.")
                return

            # If it exists, try to stop it
            print(f"Stopping container '{self.container_name}'...")
            stop_proc = subprocess.run(["docker", "stop", self.container_name], capture_output=True, text=True, check=False)
            if stop_proc.returncode == 0:
                print(f"Container '{self.container_name}' stopped successfully.")
            else:
                # It might already be stopped, or there could be an error.
                # If 'docker run' used --rm, it should be gone after stopping.
                # Check if it's truly an error or if it just means "already stopped".
                # print(f"Warning: 'docker stop {self.container_name}' exited with code {stop_proc.returncode}. Stderr: {stop_proc.stderr.strip()}")
                # Check if it's gone after the stop attempt
                still_exists_check = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
                if still_exists_check.returncode != 0:
                     print(f"Container '{self.container_name}' is confirmed to be gone after stop attempt (likely due to --rm or already stopped).")
                else:
                     print(f"Container '{self.container_name}' may not have stopped cleanly. Stderr from stop: {stop_proc.stderr.strip()}")


            # The 'docker run' command includes '--rm', so explicit removal after a successful stop
            # is usually not necessary. However, if force_remove_if_started_by_script is true,
            # or if the stop command failed and the container is lingering, we might want to force remove.
            # For now, relying on '--rm'. If you need more aggressive cleanup:
            if force_remove_if_started_by_script:
                print(f"Ensuring container '{self.container_name}' is removed (force_remove_if_started_by_script=True)...")
                rm_proc = subprocess.run(["docker", "rm", "-f", self.container_name], capture_output=True, text=True, check=False)
                if rm_proc.returncode == 0:
                    print(f"Container '{self.container_name}' forcefully removed.")
                else:
                    # Check if it was already gone
                    final_check = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, check=False)
                    if final_check.returncode != 0:
                        print(f"Container '{self.container_name}' was already gone before force remove attempt.")
                    else:
                        print(f"Failed to forcefully remove container '{self.container_name}'. Stderr: {rm_proc.stderr.strip()}")
        
        except FileNotFoundError:
            print("Docker command not found. Cannot stop/remove container.")
        except subprocess.CalledProcessError as e: # Should be rare with check=False
            print(f"A Docker command failed unexpectedly during stop/cleanup of '{self.container_name}': {e.stderr.strip()}")
            traceback.print_exc()
        except Exception as e:
            print(f"An unexpected error occurred while stopping/removing container '{self.container_name}': {e}")
            traceback.print_exc()

def read_tei(tei_file):
    with open(tei_file, "r", encoding="utf-8") as tei:
        soup = BeautifulSoup(tei, "lxml") # Requires lxml parser
    return soup

def elem_to_text(elem, default=""):
    if elem:
        return elem.get_text(separator=" ", strip=True) # Using get_text for robustness
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
        if stem.endswith(".tei"): # Handles "*.tei.xml" by removing ".tei"
            return stem[:-4] 
        return stem # Fallback if not ending with ".tei"

    @property
    def title(self):
        if self._title is None:
            # More specific search for title within teiHeader -> fileDesc -> titleStmt
            title_elem = self.soup.select_one("teiHeader > fileDesc > titleStmt > title")
            self._title = elem_to_text(title_elem)
        return self._title

    @property
    def abstract(self):
        if self._abstract is None:
            # More specific search for abstract
            abstract_elem = self.soup.select_one("teiHeader > profileDesc > abstract")
            # GROBID often puts abstract text directly in <abstract><p>...</p></abstract> or <abstract><div>...</div></abstract>
            # elem_to_text will extract all text from children.
            self._abstract = elem_to_text(abstract_elem, default=None) 
        return self._abstract

    @property
    def text(self):
        if self._text is None:
            divs_text = []
            # Find body, then find all text content not in figures, tables, formulas if desired
            body = self.soup.find("body")
            if body:
                # Example: Get all text from <p> elements directly under <div> not typed as "acknowledgement" or "annex"
                # This is a simple approach; TEI structure can be complex.
                # The original code's logic for divs without "type" is kept here.
                for div_candidate in body.find_all("div"):
                    # Filter out divs that are typically part of front/back matter if not desired
                    # e.g. div type="acknowledgements", type="annex", type="references"
                    # The original code only checked for presence of `type` attribute.
                    # if not div_candidate.get("type"): # Original logic
                    # A more robust way might be to select specific divs or exclude by type.
                    # For now, sticking to a general text extraction from body:
                    div_text = div_candidate.get_text(separator=" ", strip=True)
                    divs_text.append(div_text)
            
            # If the above is too broad, revert to original or refine selection:
            # Original logic for text extraction:
            # if body:
            # for div in body.find_all("div"):
            # if not div.get("type"): # Only process divs that do not have a 'type' attribute
            # div_text = div.get_text(separator=" ", strip=True)
            # divs_text.append(div_text)
            
            if not divs_text and body: # Fallback: get all text from body if no divs were processed
                self._text = body.get_text(separator=" ", strip=True)
            else:
                self._text = " ".join(divs_text)
        return self._text


def get_dataframe(path_to_extraction_folder, k=None):
    tei_files_pattern = str(Path(path_to_extraction_folder) / "*.tei.xml")
    list_files = glob.glob(tei_files_pattern)
    
    if not list_files:
        print(f"No *.tei.xml files found in {path_to_extraction_folder}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["ACL_id", "title", "abstract", "full_text"])

    if k is not None:
        list_files = list_files[:k]
    
    df = pd.DataFrame(list_files, columns=["path"])

    tqdm.pandas(desc="Parsing TEI files")
    df["tei"] = df["path"].progress_apply(lambda p: TEIFile(p))
    
    # It's often more efficient to extract all properties at once if TEIFile objects are heavy
    # For example, create a function that returns a dict from TEIFile
    def extract_tei_data(tei_file_obj):
        return {
            "ACL_id": tei_file_obj.basename(),
            "title": tei_file_obj.title,
            "abstract": tei_file_obj.abstract,
            "full_text": tei_file_obj.text
        }
    
    extracted_data = df["tei"].progress_apply(extract_tei_data)
    df = pd.concat([df, extracted_data.apply(pd.Series)], axis=1)
    
    df = df.drop(["tei", "path"], axis=1)
    return df


class PaperParser:
    def __init__(self, input_pdf_dir, output_dir, consolidate_citations=False, tei_coordinates=False, force=False, config_path="./krawl/parser/config/config.json"):
        self.input_pdf_dir = str(input_pdf_dir) # Ensure paths are strings
        self.output_dir = str(output_dir)
        self.consolidate_citations = consolidate_citations
        self.tei_coordinates = tei_coordinates
        self.force = force
        
        self.grobid = GrobidManager(config_path=config_path)

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        try:
            self.grobid.start() # This will raise RuntimeError if it fails to start GROBID
            
            client = self.grobid.client
            if not client:
                # This case should ideally be prevented by self.grobid.start() raising an error.
                print("GROBID client not initialized after start method. Aborting.")
                raise RuntimeError("GROBID client not initialized.")

            print(f"Processing PDFs in '{self.input_pdf_dir}' to TEI XML in '{self.output_dir}' ...")
            # Ensure input_pdf_dir and output_dir are absolute paths or resolvable by GrobidClient
            # GrobidClient might expect existing directories.
            
            # Check if input_pdf_dir contains PDFs
            pdf_files = glob.glob(os.path.join(self.input_pdf_dir, "*.pdf"))
            if not pdf_files:
                print(f"No PDF files found in input directory: {self.input_pdf_dir}")
                # Depending on desired behavior, either return or raise an error
                return 

            client.process(
                "processFulltextDocument", # service name for grobid-client-python
                self.input_pdf_dir, # input directory
                output=self.output_dir, # output directory
                consolidate_citations=self.consolidate_citations,
                tei_coordinates=self.tei_coordinates,
                force=self.force
            )
            
            # Count after processing
            pdf_count_processed = len(pdf_files) # Number of PDFs found and attempted
            tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
            print(f"Attempted to process {pdf_count_processed} PDF(s). Generated {tei_count} TEI XML files in '{self.output_dir}'.")

        except RuntimeError as e:
            print(f"A runtime error occurred during PaperParser execution: {e}")
            # Potentially re-raise or handle as needed
            raise
        except Exception as e:
            print(f"An unexpected error occurred in PaperParser.run: {e}")
            traceback.print_exc()
            raise
        finally:
            # Ensure Grobid is stopped only if this instance started it,
            # or if it's meant to be a managed, short-lived instance.
            # The current GrobidManager.stop() is designed to stop the container it knows by name.
            print("PaperParser run finished. Attempting to stop GROBID manager...")
            self.grobid.stop()

    def summary(self):
        pdf_count = len(glob.glob(os.path.join(self.input_pdf_dir, "*.pdf")))
        tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
        return {"pdf_count": pdf_count, "tei_count": tei_count, "output_dir": self.output_dir}


if __name__ == "__main__":
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
        force=True, # Force re-processing
        config_path="./krawl/parser/config/config.json"
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

