import os
import pandas as pd

from krawl.sources.arxiv_source import ArxivSource
from krawl.downloader.paper_downloader import PaperDownloader
from krawl.parser.nougat_parser import NougatPaperParser

def main(category_id: str, year: str, month: str):

    metadata_path = f"./data/metadata/arxiv/{category_id}-{year}-{month}.json"
    pdf_dir       = f"./data/pdfs/arxiv/{category_id}-{year}-{month}"
    parses_dir    = f"./data/parses/arxiv/{category_id}-{year}-{month}"

    # Metadata
    if not os.path.exists(metadata_path):
        source = ArxivSource()
        papers = source.fetch_papers(category_id, year, month)
        if papers:
            source.export_metadata_to_json(papers, metadata_path)
        else:
            print("[WARN] No papers fetched.") 

    # Download PDFs
    downloader = PaperDownloader()
    pdf_urls = pd.read_json(metadata_path)['pdf_url'].tolist()
    pdf_urls = [(url, os.path.join(pdf_dir, os.path.basename(url)) + ".pdf") for url in pdf_urls if url is not None]
    downloader.download_pdfs(pdf_urls)

    # # Parse PDFs
    # parser = NougatPaperParser(
    #     input_pdf_dir=pdf_dir,
    #     output_mmd_dir=parses_dir,
    #     force_process=False,
    #     nougat_cli_batch_size=None,
    #     nougat_full_precision=False,
    #     nougat_no_markdown=False,
    #     nougat_no_skipping=True,
    #     nougat_model_tag="0.1.0-small" 
    # )
    # parser.run()
    # summary = parser.summary()
    # print(summary)

if __name__ == "__main__":

    # RUN: python -m krawl.scripts.run_arxiv

    category_ids = ["cs.CL", "cs.AI", "cs.LG"]
    years = list(range(2000,2025))
    months = [f"0{i}" if i < 10 else str(i) for i in range(1, 13)]

    for category_id in category_ids:
        for year in years:
            for month in months:
                main(category_id, year, month)