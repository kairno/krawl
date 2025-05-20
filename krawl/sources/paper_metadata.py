from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PaperMetadata:
    title: str  # Title of the paper
    authors: List[str]  # List of author names
    year: int  # Year of publication
    pdf_url: Optional[str] = None  # URL to the PDF, if available
    source_name: str = "acl_anthology"  # Source identifier
    event_id: Optional[str] = None  # Event or venue ID
    abstract: Optional[str] = None  # Abstract of the paper
    doi: Optional[str] = None  # Digital Object Identifier
    bibkey: Optional[str] = None  # BibTeX key
    full_id: Optional[str] = None  # Full Anthology ID
    web_url: Optional[str] = None  # Web URL for the paper
    awards: Optional[List[str]] = None  # List of awards
    editors: Optional[List[str]] = None  # List of editor names
    month: Optional[str] = None  # Month of publication
    publisher: Optional[str] = None  # Publisher name
    address: Optional[str] = None  # Address of the publisher
    language_name: Optional[str] = None  # Language of the paper
    volume_id: Optional[str] = None  # Volume ID
    collection_id: Optional[str] = None  # Collection ID 