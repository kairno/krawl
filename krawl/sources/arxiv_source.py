"""
Source for fetching papers from arXiv.
"""
import requests
import feedparser
import time
import re
from datetime import datetime
from typing import List, Optional
from .base_source import BaseSource
from .paper_metadata import PaperMetadata
from calendar import monthrange

class ArxivSource(BaseSource):
    BASE_URL = "http://export.arxiv.org/api/query?"
    DELAY_BETWEEN_REQUESTS = 3.0  # seconds

    def sanitize_filename(self, name: str) -> str:
        return re.sub(r'[\\/*?:"<>|]', '', name)

    def fetch_papers(self, category_id: str, year: str, month: str) -> List[PaperMetadata]:
        # Compose the arXiv search_query string from category_id, year, and month
        # Format: cat:cs.CL+AND+submittedDate:[202301010000+TO+202301312359]
        year_int = int(year)
        month_int = int(month)
        last_day = monthrange(year_int, month_int)[1]
        start_date = f"{year}{month.zfill(2)}010000"
        end_date = f"{year}{month.zfill(2)}{str(last_day).zfill(2)}2359"
        search_query = f"cat:{category_id}+AND+submittedDate:[{start_date}+TO+{end_date}]"
        print(f"[INFO] Using search_query: {search_query}")
        start_index = 0
        results_per_page = 100
        fetched_count = 0
        total_results_for_query = None
        papers: List[PaperMetadata] = []

        while True:
            query_url = f"{self.BASE_URL}search_query={search_query}&start={start_index}&max_results={results_per_page}&sortBy=submittedDate&sortOrder=descending"
            print(f"[INFO] Fetching: {query_url}")
            try:
                response = requests.get(query_url)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Request failed: {e}")
                time.sleep(10)
                if start_index == 0 and fetched_count == 0:
                    print("[ERROR] Initial fetch failed. Aborting for this query.")
                    return papers
                else:
                    print("[WARN] Trying to continue with next page if available.")
                    start_index += results_per_page
                    continue

            feed = feedparser.parse(response.content)

            if total_results_for_query is None and hasattr(feed.feed, "opensearch_totalresults"):
                total_results_for_query = int(feed.feed.opensearch_totalresults)
                print(f"[INFO] Total results available for this query: {total_results_for_query}")

            if not feed.entries:
                print("[INFO] No more entries found for this query.")
                break

            for entry in feed.entries:
                arxiv_id_full = entry.id.split('/abs/')[-1]
                title = entry.title.strip()
                authors = [a.name for a in entry.authors] if hasattr(entry, 'authors') else []
                published_date_str = entry.get("published", entry.get("updated"))
                year = None
                if published_date_str:
                    try:
                        year = int(datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ").year)
                    except ValueError:
                        try:
                            if published_date_str[-3] == ':':
                                published_date_str = published_date_str[:-3] + published_date_str[-2:]
                            year = int(datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%S%z").year)
                        except Exception:
                            pass
                primary_category = None
                if hasattr(entry, 'arxiv_primary_category'):
                    pc = entry.arxiv_primary_category
                    if isinstance(pc, dict):
                        primary_category = pc.get('term')
                    else:
                        primary_category = getattr(pc, 'term', None)
                elif hasattr(entry, 'tags') and entry.tags:
                    tag = entry.tags[0]
                    if isinstance(tag, dict):
                        primary_category = tag.get('term')
                    else:
                        primary_category = getattr(tag, 'term', None)
                pdf_url = None
                for link in entry.links:
                    if link.get('title') == 'pdf' or (link.get('type') == 'application/pdf'):
                        pdf_url = link.href
                        break
                abstract = getattr(entry, 'summary', None)
                doi = getattr(entry, 'arxiv_doi', None)
                web_url = entry.id
                # Compose PaperMetadata
                paper = PaperMetadata(
                    title=title,
                    authors=authors,
                    year=year if year else 0,
                    pdf_url=pdf_url,
                    source_name="arxiv",
                    event_id=None,
                    abstract=abstract,
                    doi=doi,
                    bibkey=None,
                    full_id=arxiv_id_full,
                    web_url=web_url,
                    awards=None,
                    editors=None,
                    month=None,
                    publisher=None,
                    address=None,
                    language_name=None,
                    volume_id=None,
                    collection_id=primary_category
                )
                papers.append(paper)
                fetched_count += 1
            if start_index + len(feed.entries) >= (total_results_for_query or 0):
                print("[INFO] Fetched all available papers according to total_results_for_query.")
                break
            start_index += len(feed.entries)
            print(f"[INFO] Fetched {fetched_count} papers so far. Waiting {self.DELAY_BETWEEN_REQUESTS} seconds before next metadata page...")
            time.sleep(self.DELAY_BETWEEN_REQUESTS)
        print(f"[INFO] Finished fetching. Total papers fetched for this run: {fetched_count}")
        return papers

if __name__ == "__main__":

    # RUN: python -m krawl.sources.arxiv_source --category_id cs.CL --year 2023 --month 01

    import argparse
    parser = argparse.ArgumentParser(description="Fetch arXiv paper metadata for a category, year, and month, and save as JSON.")
    parser.add_argument('--category_id', type=str, required=True, help='arXiv category ID, e.g., cs.CL')
    parser.add_argument('--year', type=str, required=True, help='Year, e.g., 2023')
    parser.add_argument('--month', type=str, required=True, help='Month (01-12)')
    args = parser.parse_args()

    output_path = f'./data/metadata/arxiv/{args.category_id}-{args.year}-{args.month}.json'

    source = ArxivSource()
    papers = source.fetch_papers(args.category_id, args.year, args.month)
    if papers:
        source.export_metadata_to_json(papers, output_path)
    else:
        print("[WARN] No papers fetched.") 