import os
import pandas as pd

from krawl.sources.acl_source import ACLSource
from krawl.downloader.paper_downloader import PaperDownloader
from krawl.parser.nougat_parser import NougatPaperParser

def main(event_id: str):

    metadata_path = f"./data/metadata/acl-anthology/{event_id}.json"
    pdf_dir       = f"./data/pdfs/acl-anthology/{event_id}"
    parses_dir    = f"./data/parses/acl-anthology/{event_id}"

    # Metadata
    if not os.path.exists(metadata_path):
        source = ACLSource()
        papers = source.fetch_papers(event_id=event_id)
        source.export_metadata_to_json(papers, metadata_path)

    # Download PDFs
    downloader = PaperDownloader()
    pdf_urls = pd.read_json(metadata_path)['pdf_url'].tolist()
    pdf_urls = [(url, os.path.join(pdf_dir, os.path.basename(url))) for url in pdf_urls if url is not None]
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

    # RUN: python -m main

    target_event_ids = ["coling", "lrec", "eacl", "tacl", "acl", "naacl", "emnlp"] 

    source = ACLSource()
    event_ids = source.get_event_ids(filter_by_event_ids=target_event_ids)
    event_ids.sort(key=lambda x: int(x.split('-')[1]), reverse=True)

    for event_id in event_ids:
        main(event_id)