import os
import csv
from typing import List, Dict, Any

class OpenReviewSource:
    """
    Source for fetching reviews from OpenReview and saving them to CSV.
    """
    def __init__(self, data_dir="./data/reviews"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def fetch_reviews(self, venue_id: str, review_name: str = "Official_Review", client=None) -> List[Dict[str, Any]]:
        """
        Fetch all reviews for a given venue using the OpenReview API.
        If openreview-py client is provided, use it; otherwise, raise NotImplementedError.
        Returns a list of review dicts.
        """
        if client is None:
            raise NotImplementedError("Please provide an openreview-py client instance.")
        invitation_id = f"{venue_id}/-/{review_name}"
        reviews = list(client.get_notes(invitation=invitation_id))
        return reviews

    def save_reviews_to_csv(self, reviews: List[Dict[str, Any]], filename: str):
        """
        Save a list of review dicts to a CSV file in the data_dir.
        The CSV will have columns for forum, and all keys in the review content.
        """
        if not reviews:
            print("No reviews to save.")
            return
        # Get all keys from the first review's content
        keylist = list(reviews[0].content.keys())
        keylist.insert(0, 'forum')
        csv_path = os.path.join(self.data_dir, filename)
        with open(csv_path, 'w', newline='', encoding='utf-8') as outfile:
            csvwriter = csv.writer(outfile, delimiter=',')
            csvwriter.writerow(keylist)
            for review in reviews:
                valueList = [getattr(review, 'forum', '')]
                for key in keylist[1:]:
                    value = review.content.get(key, {})
                    if isinstance(value, dict) and 'value' in value:
                        valueList.append(value['value'])
                    else:
                        valueList.append(value if value else '')
                csvwriter.writerow(valueList)
        print(f"Saved {len(reviews)} reviews to {csv_path}") 