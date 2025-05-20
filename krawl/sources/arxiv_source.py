__all__ = ["ArxivSource"]

"""
Source for fetching papers from arXiv.
"""
from .base_source import BaseSource
from .paper_metadata import PaperMetadata

class ArxivSource(BaseSource):
    def fetch_papers(self, query):
        # Placeholder: fetch papers from arXiv
        return [] 