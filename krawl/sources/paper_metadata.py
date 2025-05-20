from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PaperMetadata:
    title: str
    authors: List[str]
    year: int
    pdf_url: str
    source_name: str = "acl_anthology"
    event_id: Optional[str] = None 