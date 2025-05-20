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

CONFIG_PATH = "./krawl/parser/config/config.json"

class GrobidManager:
    def __init__(self, container_name="grobid_container", image="grobid/grobid:0.8.2"):
        self.container_name = container_name
        self.image = image
        self.started = False

    def is_grobid_running(self):
        try:
            client = GrobidClient(CONFIG_PATH)
            up, status = client.ping()
            return up
        except Exception:
            return False

    def start(self):
        if self.is_grobid_running():
            print("GROBID is already running.")
            self.started = False
            return
        print("Starting GROBID Docker container...")
        cmd = [
            "docker", "run", "--rm", "--gpus", "all", "--init", "--ulimit", "core=0",
            "-p", "8070:8070", "-d", "--name", self.container_name, self.image
        ]
        try:
            print(f"Running docker command: {cmd}")
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            print(f"Failed to start GROBID Docker container: {e}")
            raise
        # Wait for GROBID to be ready
        for _ in range(120):
            if self.is_grobid_running():
                print("GROBID is up!")
                self.started = True
                return
            time.sleep(1)
        print("GROBID did not start in time.")
        self.stop()
        raise RuntimeError("GROBID did not start in time.")

    def stop(self):
        try:
            print("Stopping GROBID Docker container...")
            subprocess.run(["docker", "stop", self.container_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.started = False
        except Exception as e:
            print(f"Failed to stop GROBID Docker container: {e}")
        finally:
            print("Stopping all containers...")
            subprocess.run(["docker", "stop", "$(docker ps -q)"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.started = False


def read_tei(tei_file):
    with open(tei_file, "r", encoding="utf-8") as tei:
        soup = BeautifulSoup(tei, "lxml")
        return soup

def elem_to_text(elem, default=""):
    if elem:
        return elem.getText()
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
            return stem[0:-4]
        else:
            return stem

    @property
    def title(self):
        if self._title is None:
            title_elem = self.soup.find("title")
            self._title = elem_to_text(title_elem)
        return self._title

    @property
    def abstract(self):
        if self._abstract is None:
            abstract_elem = self.soup.find("abstract")
            self._abstract = elem_to_text(abstract_elem, default=None)
        return self._abstract

    @property
    def text(self):
        if self._text is None:
            divs_text = []
            body = self.soup.find("body")
            if body:
                for div in body.find_all("div"):
                    if not div.get("type"):
                        div_text = div.get_text(separator=" ", strip=True)
                        divs_text.append(div_text)
            self._text = " ".join(divs_text)
        return self._text


def get_dataframe(path_to_extraction_folder, k=None):
    list_files = glob.glob(str(Path(path_to_extraction_folder) / "*.tei.xml"))
    if k is not None:
        list_files = list_files[:k]
    df = pd.DataFrame(list_files, columns=["path"])

    tqdm.pandas(desc="Parsing TEI files")
    df["tei"] = df["path"].progress_apply(lambda p: TEIFile(p))
    df["ACL_id"] = df["tei"].progress_apply(lambda t: t.basename())
    df["title"] = df["tei"].progress_apply(lambda t: t.title)
    df["abstract"] = df["tei"].progress_apply(lambda t: t.abstract)
    df["full_text"] = df["tei"].progress_apply(lambda t: t.text)

    df = df.drop(["tei", "path"], axis=1)
    return df


class GrobidTEIParser:
    def __init__(self, input_pdf_dir, output_dir, consolidate_citations=False, tei_coordinates=False, force=False):
        self.input_pdf_dir = input_pdf_dir
        self.output_dir = output_dir
        self.consolidate_citations = consolidate_citations
        self.tei_coordinates = tei_coordinates
        self.force = force
        self.grobid = GrobidManager()

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        try:
            self.grobid.start()
            client = GrobidClient(CONFIG_PATH)
            print(f"Processing PDFs in {self.input_pdf_dir} to TEI XML in {self.output_dir} ...")
            client.process(
                "processFulltextDocument",
                self.input_pdf_dir,
                output=self.output_dir,
                consolidate_citations=self.consolidate_citations,
                tei_coordinates=self.tei_coordinates,
                force=self.force
            )
            pdf_count = len(glob.glob(os.path.join(self.input_pdf_dir, "*.pdf")))
            tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
            print(f"Processed {pdf_count} PDFs. Generated {tei_count} TEI XML files in {self.output_dir}.")
        finally:
            self.grobid.stop()

    def summary(self):
        pdf_count = len(glob.glob(os.path.join(self.input_pdf_dir, "*.pdf")))
        tei_count = len(glob.glob(os.path.join(self.output_dir, "*.tei.xml")))
        return {"pdf_count": pdf_count, "tei_count": tei_count}


if __name__ == "__main__":
    
    # RUN: python -m krawl.parser.extract_tei_text

    # Example usage for demonstration
    test_data_dir = "./krawl/tests/test_data"
    output_dir = "./krawl/tests/test_data/demo_tei_out"
    parser = GrobidTEIParser(
        input_pdf_dir=str(test_data_dir),
        output_dir=str(output_dir),
        consolidate_citations=True,
        tei_coordinates=True,
        force=True
    )
    parser.run()
    print(parser.summary()) 