import pytest
from krawl.sources import ACLSource, PaperMetadata

def test_fetch_papers_with_specific_event_id():
    source = ACLSource()
    event_id = "acl-2022"  # Use a real event ID from the ACL Anthology
    papers = source.fetch_papers(event_id=event_id)
    assert isinstance(papers, list)
    assert len(papers) > 0  # There should be papers for this event
    for paper in papers:
        assert isinstance(paper, PaperMetadata)
        assert paper.event_id == event_id
        assert paper.title
        assert paper.pdf_url

def test_empty_results_for_unlikely_event_id():
    source = ACLSource()
    event_id = "nonsense_event"
    with pytest.raises(Exception):
        source.fetch_papers(event_id=event_id)
