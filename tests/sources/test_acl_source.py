import pytest
from krawl.sources import ACLSource, PaperMetadata

def test_fetch_papers_by_year():
    source = ACLSource()
    papers = source.fetch_papers(year=2020)
    assert isinstance(papers, list)
    for paper in papers:
        assert isinstance(paper, PaperMetadata)
        assert paper.year == 2020
        assert paper.title
        assert paper.authors
        assert paper.pdf_url
        assert paper.source_name == "acl_anthology"

def test_fetch_papers_with_specific_event_id():
    source = ACLSource()
    papers = source.fetch_papers(event_id="acl_test_2020")
    assert isinstance(papers, list)
    for paper in papers:
        assert isinstance(paper, PaperMetadata)
        assert paper.event_id == "acl_test_2020"
        assert paper.title
        assert paper.authors
        assert paper.pdf_url
        assert paper.source_name == "acl_anthology"

def test_empty_results_for_unlikely_filter():
    source = ACLSource()
    papers = source.fetch_papers(year=2099, event_id="nonsense_event")
    assert papers == []
