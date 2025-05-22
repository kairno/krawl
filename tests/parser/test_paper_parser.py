import os
from krawl.parser.grobid_parser import GROBIDPaperParser

def test_extract_tei_text():
    input_pdf_dir_path = "./tests/test_data/pdfs"
    output_dir_path = "./tests/test_data/parses"

    parser = GROBIDPaperParser(
        input_pdf_dir=input_pdf_dir_path,
        output_dir=output_dir_path,
        consolidate_citations=True,
        tei_coordinates=True,
        force=True, 
        config_path="./krawl/parser/config/config.json"
    )
    parser.run()
    summary_info = parser.summary()

    # Assert that at least one TEI XML file was created
    tei_files = [f for f in os.listdir(output_dir_path) if f.endswith('.tei.xml')]
    assert len(tei_files) > 0, "No TEI XML files were created."

    # Assert that summary matches the file counts
    assert summary_info["tei_count"] == len(tei_files), "Summary TEI count does not match actual file count."
    assert summary_info["pdf_count"] == len([f for f in os.listdir(input_pdf_dir_path) if f.endswith('.pdf')]), "Summary PDF count does not match input file count."
