import glob
from pathlib import Path
from dataclasses import dataclass, field
from tqdm import tqdm
import pandas as pd
import subprocess
import os
import traceback
import shutil

class NougatProcessor:
    def __init__(self, nougat_command="nougat",
                 nougat_cli_batch_size=None,
                 recompute=False,
                 full_precision=False,
                 no_markdown=False,
                 no_skipping=True,
                 model_tag=None): 
        self.nougat_command = self_find_nougat_command(nougat_command)
        if not self.nougat_command:
            raise FileNotFoundError(
                f"Nougat command '{nougat_command}' not found after checking preferred path, "
                "NOUGAT_COMMAND_PATH, system PATH, and common user locations. "
                "Please ensure Nougat is installed and accessible."
            )
        self.nougat_cli_batch_size = nougat_cli_batch_size
        self.recompute = recompute
        self.full_precision = full_precision
        self.no_markdown = no_markdown
        self.no_skipping = no_skipping
        self.model_tag = model_tag 
        self._check_nougat_command()

    def _check_nougat_command(self):
        try:
            result = subprocess.run([self.nougat_command, "--help"], capture_output=True, check=True, text=True)
            print(f"Nougat command '{self.nougat_command}' found and appears functional (via --help).")
        except FileNotFoundError as e:
            print(f"Error: Nougat command '{self.nougat_command}' not found during --help check.")
            raise RuntimeError(
                f"Nougat command '{self.nougat_command}' not found. "
                "Ensure Nougat is installed and path is correct."
            ) from e
        except subprocess.CalledProcessError as e:
            print(f"Error when checking Nougat command '{self.nougat_command}' with --help.")
            print(f"Return code: {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}")
            raise RuntimeError(
                f"Nougat command '{self.nougat_command}' did not respond as expected to --help."
            ) from e
        except Exception as e:
            print(f"Unexpected error while checking Nougat command '{self.nougat_command}': {e}")
            traceback.print_exc()
            raise

    def process_pdf(self, pdf_path: str, output_dir: str):
        pdf_path_obj = Path(pdf_path)
        output_dir_obj = Path(output_dir)
        output_dir_obj.mkdir(parents=True, exist_ok=True)

        cmd = [
            self.nougat_command,
            str(pdf_path_obj),
            "--out", str(output_dir_obj),
        ]
        if self.model_tag: # Add model tag if specified
            cmd.extend(["--model", self.model_tag])
        if self.nougat_cli_batch_size:
            cmd.extend(["--batchsize", str(self.nougat_cli_batch_size)])
        if self.recompute:
            cmd.append("--recompute")
        if self.full_precision:
            cmd.append("--full-precision")
        if self.no_markdown:
            cmd.append("--no-markdown")
        if self.no_skipping:
            cmd.append("--no-skipping")

        print(f"Executing Nougat for {pdf_path_obj.name}: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=1800) # 30 min timeout
            if result.stdout:
                print(f"Nougat stdout for {pdf_path_obj.name}:\n{result.stdout}")
            if result.stderr:
                print(f"Nougat stderr for {pdf_path_obj.name}:\n{result.stderr}")
            expected_mmd_path = output_dir_obj / f"{pdf_path_obj.stem}.mmd"
            if not expected_mmd_path.exists():
                print(f"Warning: Nougat succeeded but output {expected_mmd_path} not found.")
        except subprocess.CalledProcessError as e:
            print(f"Error processing {pdf_path_obj.name} with Nougat.")
            print(f"Return code: {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}")
            raise RuntimeError(f"Nougat processing failed for {pdf_path_obj.name}") from e
        except subprocess.TimeoutExpired as e:
            print(f"Timeout processing {pdf_path_obj.name} after {e.timeout}s.")
            if e.stdout: print(f"Stdout: {e.stdout.decode(errors='ignore')}")
            if e.stderr: print(f"Stderr: {e.stderr.decode(errors='ignore')}")
            raise RuntimeError(f"Nougat processing timed out for {pdf_path_obj.name}") from e
        except Exception as e:
            print(f"Unexpected error processing {pdf_path_obj.name}: {e}")
            traceback.print_exc()
            raise

def self_find_nougat_command(preferred_command="nougat"):
    if shutil.which(preferred_command):
        return preferred_command
    env_path = os.environ.get("NOUGAT_COMMAND_PATH")
    if env_path and shutil.which(env_path):
        print(f"Using Nougat command from NOUGAT_COMMAND_PATH: {env_path}")
        return env_path
    path_nougat = shutil.which("nougat")
    if path_nougat:
        return path_nougat
    try:
        user_base = subprocess.check_output(["python", "-m", "site", "--user-base"], text=True).strip()
        nougat_user_path = Path(user_base) / "bin" / "nougat"
        if nougat_user_path.exists() and os.access(nougat_user_path, os.X_OK):
            print(f"Found Nougat at user pip install location: {nougat_user_path}")
            return str(nougat_user_path)
    except Exception:
        pass
    return None

def read_mmd(mmd_file_path: str) -> list[str]:
    with open(mmd_file_path, "r", encoding="utf-8") as f:
        return f.readlines()

@dataclass
class MMDFile:
    filepath: str
    raw_lines: list[str] = field(default_factory=list, repr=False)
    _title: str = field(init=False, default="")
    _abstract: str = field(init=False, default="")
    _full_text: str = field(init=False, default="")

    def __post_init__(self):
        if not self.raw_lines and self.filepath:
            try:
                self.raw_lines = read_mmd(self.filepath)
            except FileNotFoundError:
                print(f"Error: MMD file not found at {self.filepath}")
                self.raw_lines = []
        self._parse_content()

    def _parse_content(self):
        lines = self.raw_lines
        if not lines: return

        title_lines = []
        title_search_limit = min(10, len(lines))
        for i, line in enumerate(lines[:title_search_limit]):
            stripped_line = line.strip()
            if not stripped_line:
                if not title_lines: continue
                else: break
            if stripped_line.startswith("# ") and not title_lines:
                title_lines.append(stripped_line[2:])
            elif not stripped_line.startswith("##") and len(title_lines) < 3:
                title_lines.append(stripped_line)
            elif title_lines: break
        self._title = " ".join(title_lines).strip()

        abstract_lines = []
        potential_abstract_keywords = ["abstract", "summary"]
        abstract_start_idx = -1
        abstract_heading_search_limit = min(max(10, len(lines) // 3), len(lines))

        for i, line in enumerate(lines[:abstract_heading_search_limit]):
            stripped = line.strip()
            lower_stripped = stripped.lower()
            is_heading = False
            if any(f"# {kw}" in lower_stripped for kw in potential_abstract_keywords) or \
               any(f"## {kw}" in lower_stripped for kw in potential_abstract_keywords) or \
               lower_stripped in potential_abstract_keywords:
                is_heading = True

            if is_heading:
                if len(stripped) < 30 or stripped.startswith("#"):
                    abstract_start_idx = i + 1
                else:
                    abstract_start_idx = i
                    abstract_lines.append(stripped.lstrip("# ").strip())
                break
        
        if abstract_start_idx != -1:
            for i in range(abstract_start_idx, len(lines)):
                line_to_check = lines[i]
                stripped_line_to_check = line_to_check.strip()
                is_new_major_section = (line_to_check.startswith("# ") or 
                                       (line_to_check.startswith("## ") and 
                                        not any(kw in line_to_check.lower() for kw in potential_abstract_keywords)))
                is_double_blank = (not stripped_line_to_check and abstract_lines and not lines[i-1].strip())

                if is_new_major_section or is_double_blank: break
                
                if stripped_line_to_check: abstract_lines.append(stripped_line_to_check)
                elif abstract_lines: abstract_lines.append("")
            
            temp_abstract = "\n".join(abstract_lines).strip()
            self._abstract = " ".join(temp_abstract.splitlines()).strip()

        self._full_text = "".join(lines)

    def basename(self):
        stem = Path(self.filepath).stem
        if stem.endswith(".mmd"): return stem[:-4]
        return stem

    @property
    def title(self): return self._title
    @property
    def abstract(self): return self._abstract if self._abstract else None
    @property
    def text(self): return self._full_text

def get_nougat_dataframe(path_to_extraction_folder, k=None):
    mmd_files_pattern = str(Path(path_to_extraction_folder) / "*.mmd")
    list_files = glob.glob(mmd_files_pattern)
    if not list_files:
        print(f"No *.mmd files found in {path_to_extraction_folder}")
        return pd.DataFrame(columns=["ACL_id", "title", "abstract", "full_text"])
    if k is not None: list_files = list_files[:k]
    all_data = [{"ACL_id": MMDFile(fp).basename(),
                 "title": MMDFile(fp).title,
                 "abstract": MMDFile(fp).abstract,
                 "full_text": MMDFile(fp).text}
                for fp in tqdm(list_files, desc="Parsing MMD files")]
    return pd.DataFrame(all_data)

class NougatPaperParser:
    def __init__(self, input_pdf_dir, output_mmd_dir,
                 force_process=False,
                 nougat_cli_batch_size=None,
                 nougat_full_precision=False,
                 nougat_no_markdown=False,
                 nougat_no_skipping=True,
                 nougat_model_tag=None): 
        self.input_pdf_dir = str(input_pdf_dir)
        self.output_mmd_dir = str(output_mmd_dir)
        self.force_process = force_process
        self.nougat_processor = NougatProcessor(
            nougat_cli_batch_size=nougat_cli_batch_size,
            recompute=self.force_process,
            full_precision=nougat_full_precision,
            no_markdown=nougat_no_markdown,
            no_skipping=nougat_no_skipping,
            model_tag=nougat_model_tag 
        )

    def run(self):
        os.makedirs(self.output_mmd_dir, exist_ok=True)
        all_pdf_files = glob.glob(os.path.join(self.input_pdf_dir, "*.pdf"))
        if not all_pdf_files:
            print(f"No PDF files found in {self.input_pdf_dir}"); return

        pdf_files_to_process, skipped = [], 0
        print(f"Checking PDFs in '{self.input_pdf_dir}' for MMD output in '{self.output_mmd_dir}'...")
        for pdf_path_str in all_pdf_files:
            pdf_p, mmd_p = Path(pdf_path_str), Path(self.output_mmd_dir) / f"{Path(pdf_path_str).stem}.mmd"
            if not self.force_process and mmd_p.exists() and mmd_p.stat().st_size > 0:
                print(f"Skipping '{pdf_p.name}': Output '{mmd_p}' exists.")
                skipped += 1
            else: pdf_files_to_process.append(str(pdf_p))
        
        print(f"Found {len(all_pdf_files)} PDFs. Skipped {skipped}. Processing {len(pdf_files_to_process)}.")
        if not pdf_files_to_process: print("No new PDFs to process."); return
        
        success, failed = 0, 0
        for i, pdf_fp_str in enumerate(tqdm(pdf_files_to_process, desc="Processing PDFs")):
            pdf_fp = Path(pdf_fp_str)
            print(f"\nProcessing PDF {i+1}/{len(pdf_files_to_process)}: {pdf_fp.name}")
            try:
                self.nougat_processor.process_pdf(str(pdf_fp), self.output_mmd_dir)
                success += 1
            except RuntimeError as e: print(f"Failed to process {pdf_fp.name}: {e}"); failed += 1
            except Exception as e: print(f"Unexpected error for {pdf_fp.name}: {e}"); traceback.print_exc(); failed += 1
        
        print(f"\nFinished. Successfully processed: {success}. Failed: {failed}.")
        print(f"Total MMD files in '{self.output_mmd_dir}': {len(glob.glob(os.path.join(self.output_mmd_dir, '*.mmd')))}.")

    def summary(self):
        return {"pdf_count": len(glob.glob(os.path.join(self.input_pdf_dir, "*.pdf"))),
                "mmd_count": len(glob.glob(os.path.join(self.output_mmd_dir, "*.mmd"))),
                "output_mmd_dir": self.output_mmd_dir}

if __name__ == "__main__":

    # RUN: CUDA_VISIBLE_DEVICES=0 python -m krawl.parser.nougat_parser

    print("Starting Nougat MMD Parser script...")
    input_pdf_dir = "./tests/test_data/pdfs" # Ensure this path is correct
    output_mmd_dir = "./tests/test_data/parses" # Ensure this path is correct
    Path(input_pdf_dir).mkdir(parents=True, exist_ok=True)
    Path(output_mmd_dir).mkdir(parents=True, exist_ok=True)
    print(f"Input PDF directory: {input_pdf_dir}\nOutput MMD directory: {output_mmd_dir}")

    # --- Configuration ---
    PROCESS_ALL_FORCE = False
    NOUGAT_BATCH_SIZE = None
    NOUGAT_FULL_PRECISION = False
    NOUGAT_NO_MARKDOWN = False
    NOUGAT_NO_SKIPPING = True
    NOUGAT_MODEL_TAG = "0.1.0-small"  # 0.1.0-base" "0.1.0-small" 

    try:
        parser = NougatPaperParser(
            input_pdf_dir=input_pdf_dir,
            output_mmd_dir=output_mmd_dir,
            force_process=PROCESS_ALL_FORCE,
            nougat_cli_batch_size=NOUGAT_BATCH_SIZE,
            nougat_full_precision=NOUGAT_FULL_PRECISION,
            nougat_no_markdown=NOUGAT_NO_MARKDOWN,
            nougat_no_skipping=NOUGAT_NO_SKIPPING,
            nougat_model_tag=NOUGAT_MODEL_TAG 
        )
        parser.run()
        summary = parser.summary()
        print("\nProcessing summary:"); [print(f"  {k}: {v}") for k, v in summary.items()]
        
        print(f"\nGenerating DataFrame from MMD files in '{output_mmd_dir}'...")
        df = get_nougat_dataframe(output_mmd_dir)
        if not df.empty:
            print("\nDataFrame generated:"); print(df.head())
            print(f"\nShape: {df.shape}. Titles found: {df['title'].notna().sum()}/{len(df)}. Abstracts: {df['abstract'].notna().sum()}/{len(df)}.")
        else: print("No MMD files processed into DataFrame or directory empty.")
    except FileNotFoundError as e: print(f"Critical Error: {e}\nNougat command likely not found. Check installation/PATH."); traceback.print_exc()
    except RuntimeError as e: print(f"Runtime Error: {e}"); traceback.print_exc()
    except Exception as e: print(f"Unexpected Error: {e}"); traceback.print_exc()