__all__ = ["PaperMetadata", "ACLSource"]

"""
Source for fetching papers from ACL Anthology.
"""
# TODO: from acl_anthology import Anthology # Uncomment and use in real implementation
from .base_source import BaseSource
from krawl.sources.paper_metadata import PaperMetadata
from acl_anthology import Anthology

class ACLSource(BaseSource):
    def __init__(self):
        self.anthology = Anthology.from_repo()

    def fetch_papers(self, year=None, event_id=None):
        """
        Fetch papers from ACL Anthology by year or event_id.
        Returns a list of PaperMetadata.
        """
        papers = []
        for paper in self.anthology.papers.values():
            if year is not None and paper.year != year:
                continue
            if event_id is not None and paper.venue != event_id:
                continue
            papers.append(
                PaperMetadata(
                    title=str(paper.title),
                    authors=[str(author.name) for author in paper.authors],
                    year=paper.year,
                    pdf_url=paper.pdf_url,
                    event_id=paper.venue,
                )
            )
        return papers 