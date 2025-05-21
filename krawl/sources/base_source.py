"""
Base class for all paper sources.
"""

import os
import json
import dataclasses
from typing import List
from krawl.sources.paper_metadata import PaperMetadata

class BaseSource:
    def fetch_papers(self):
        raise NotImplementedError 
    
    def export_metadata_to_json(self, papers_metadata_list: List[PaperMetadata], output_filename: str):
        """
        Exports a list of PaperMetadata objects to a JSON file.

        Args:
            papers_metadata_list: A list of PaperMetadata objects.
            output_filename: The name of the JSON file to create.
        """

        # Create the output directory if it doesn't exist
        output_dir = os.path.dirname(output_filename)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Export metadata to JSON
        papers_as_dicts = []
        for paper_meta in papers_metadata_list:
            if dataclasses.is_dataclass(paper_meta):
                papers_as_dicts.append(dataclasses.asdict(paper_meta))
            else:
                # This case should ideally not be hit if PaperMetadata is always a dataclass
                print(f"Warning: Paper {getattr(paper_meta, 'title', 'Unknown Title')} is not a dataclass. Attempting vars().")
                try:
                    papers_as_dicts.append(vars(paper_meta))
                except TypeError:
                    print(f"Error: Could not serialize paper: {getattr(paper_meta, 'title', 'Unknown Title')}")
                    papers_as_dicts.append({"title": getattr(paper_meta, 'title', 'Unknown Title'), "error": "serialization_failed"})
        
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(papers_as_dicts, f, ensure_ascii=False, indent=4)
            print(f"Successfully saved metadata for {len(papers_metadata_list)} papers to {output_filename}")
        except IOError as e:
            print(f"Error saving metadata to JSON: {e}")
        except TypeError as e:
            print(f"Error serializing metadata to JSON (check data types): {e}")