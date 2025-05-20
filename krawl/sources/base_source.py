__all__ = ["BaseSource"]

"""
Base class for all paper sources.
"""
class BaseSource:
    def fetch_papers(self):
        raise NotImplementedError 