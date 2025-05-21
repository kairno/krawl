import os
import pandas as pd

from krawl.sources.acl_source import ACLSource
from krawl.downloader.paper_downloader import PaperDownloader
from krawl.parser.paper_parser import PaperParser

def main(event_id: str):

    metadata_path = f"./data/metadata/{event_id}.json"
    pdf_dir       = f"./data/pdfs/{event_id}"
    parses_dir    = f"./data/parses/{event_id}"

    # Metadata
    if not os.path.exists(metadata_path):
        source = ACLSource()
        papers = source.fetch_papers(event_id=event_id)
        source.export_metadata_to_json(papers, metadata_path)

    # Download PDFs
    if not os.path.exists(pdf_dir) and len:
        downloader = PaperDownloader()
        pdf_urls = pd.read_json(metadata_path)['pdf_url'].tolist()
        pdf_urls = [(url, os.path.join(pdf_dir, os.path.basename(url))) for url in pdf_urls if url is not None]
        downloader.download_pdfs(pdf_urls)

    # Parse PDFs
    if not os.path.exists(parses_dir):
        parser = PaperParser(
            input_pdf_dir=pdf_dir,
            output_dir=parses_dir,
            consolidate_citations=True,
            tei_coordinates=True,
            force=True, 
            config_path="./krawl/parser/config/config.json"
        )
        parser.run()
        summary_info = parser.summary()
        print(summary_info)

if __name__ == "__main__":

    # RUN: python -m main

    source = ACLSource()
    event_ids = source.get_event_ids(filter_by_str=["tacl", "acl", "naacl", "emnlp"])
    event_ids.sort(key=lambda x: int(x.split('-')[1]), reverse=True)

    for event_id in event_ids:
        main(event_id)