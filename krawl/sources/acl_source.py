"""
Source for fetching papers from ACL Anthology.
"""
from .base_source import BaseSource
from krawl.sources.paper_metadata import PaperMetadata
from acl_anthology import Anthology

class ACLSource(BaseSource):
    def __init__(self):
        self.anthology = Anthology.from_repo()

    def _to_paper_metadata(self, paper, event_id=None):
        web_url = getattr(paper, 'web_url', None)
        pdf_url = getattr(paper, 'pdf_url', None)
        if not pdf_url and web_url:
            # Generate pdf_url from web_url by replacing the final '/' with '.pdf'
            if web_url.endswith('/'):
                pdf_url = web_url[:-1] + '.pdf'
            else:
                pdf_url = web_url + '.pdf'
        return PaperMetadata(
            title=str(paper.title),
            authors=[str(author.name) for author in paper.authors],
            year=paper.year,
            pdf_url=pdf_url,
            source_name="acl_anthology",
            event_id=event_id if event_id is not None else getattr(paper, 'venue', None),
            abstract=str(paper.abstract) if getattr(paper, 'abstract', None) else None,
            doi=getattr(paper, 'doi', None),
            bibkey=getattr(paper, 'bibkey', None),
            full_id=getattr(paper, 'full_id', None),
            web_url=web_url,
            awards=getattr(paper, 'awards', None),
            editors=[str(editor.name) for editor in getattr(paper, 'editors', [])] if getattr(paper, 'editors', None) else None,
            month=getattr(paper, 'month', None),
            publisher=getattr(paper, 'publisher', None),
            address=getattr(paper, 'address', None),
            language_name=getattr(paper, 'language_name', None),
            volume_id=getattr(paper, 'volume_id', None),
            collection_id=getattr(paper, 'collection_id', None),
        )

    def fetch_papers(self, event_id=None):
        """
        Fetch papers from ACL Anthology by event_id.
        Returns a list of PaperMetadata.
        """
        if event_id is None:
            raise ValueError("event_id must be provided for ACLSource.fetch_papers")
        papers = []
        event = self.anthology.get_event(event_id)
        for volume in event.volumes():
            for paper in volume.papers():
                papers.append(self._to_paper_metadata(paper, event_id=event_id))
        return papers
    
    def get_event_ids(self, filter_by_str=["tacl", "acl", "naacl", "emnlp"]):
        self.anthology.load_all() 
        self.anthology.events.load() 
        event_ids = list(self.anthology.events.keys())
        if filter_by_str:
            event_ids = [event_id for event_id in event_ids if any(event_id.split('-')[0] == filter_str for filter_str in filter_by_str)]
        return event_ids

if __name__ == "__main__":

    # RUN: python -m krawl.sources.acl_source

    source = ACLSource()
    # event_id = "acl-2022"
    # print(f"Fetching papers for event_id: {event_id}\n")
    # papers = source.fetch_papers(event_id=event_id)
    # print(f"Total papers found: {len(papers)}\n")
    # for i, paper in enumerate(papers[:5]):
    #     print(f"Paper {i+1}: {paper}")

    # # Export metadata to JSON
    # output_filename = "./tests/test_data/metadata/acl_anthology.json"
    # source.export_metadata_to_json(papers, output_filename)
    # print(f"Metadata exported to {output_filename}")

    event_ids = source.get_event_ids()
    # Sort by year (second part of event_id after '-') in descending order
    event_ids.sort(key=lambda x: int(x.split('-')[1]), reverse=True)
    print(event_ids)