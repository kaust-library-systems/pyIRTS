#
# Harvest metadata from source.
#

import typer
import requests

class arxiv:
    def __init__(self, URL: str, queryItem: str):
        self.URL = URL,
        self.queryItem = queryItem

    def get_metadata(self):
        arxivBaseURL = "http://export.arxiv.org/api/search_query="
        metadata = requests.get(self.URL)

        
        
def main(source: str):
    
    print(f"Source: {source}")

if __name__ == "__main__":
    typer.run(main)

