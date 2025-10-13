"""
Crossref metadata harvester.

Harvests publication metadata from Crossref API using multiple discovery methods.
"""

from typing import Dict, Any, List, Optional
import time
import os
import requests
import structlog
from datetime import datetime, timedelta
from .base import BaseHarvester

logger = structlog.get_logger()


class CrossrefHarvester(BaseHarvester):
    """Harvester for Crossref metadata."""

    def __init__(self):
        super().__init__('crossref')
        self.api_url = os.getenv('CROSSREF_API', 'https://api.crossref.org/')
        self.delay = int(os.getenv('CROSSREF_DELAY', '1'))
        self.ir_email = os.getenv('IR_EMAIL', '')
        self.institution_abbrev = os.getenv('INSTITUTION_ABBREVIATION', 'KAUST')
        self.institution_city = os.getenv('INSTITUTION_CITY', 'Thuwal')

        # Date constants
        self.one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        self.one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    def harvest(self, harvest_type: str = 'new') -> Dict[str, Any]:
        """
        Harvest Crossref metadata using multiple strategies.

        Args:
            harvest_type: 'new', 'reprocess', 'reharvest'

        Returns:
            Dictionary with changedCount and summary
        """
        self.log(f"Starting Crossref harvest (type: {harvest_type})")

        dois_to_harvest = {}

        if harvest_type == 'reprocess':
            dois_to_harvest['Reprocess'] = self._get_dois_to_reprocess()
        elif harvest_type == 'reharvest':
            dois_to_harvest['Reharvest'] = self._get_dois_to_reharvest()
        else:
            # Normal harvest with multiple discovery methods
            dois_to_harvest.update(self._discover_dois())

        # Process all discovered DOIs
        for harvest_basis, dois in dois_to_harvest.items():
            self.log(f"\nProcessing {len(dois)} DOIs: {harvest_basis}")

            for doi in dois:
                self._process_doi(doi, harvest_basis)
                time.sleep(self.delay)

        summary = self.save_report()
        changed_count = self.record_counts['all'] - self.record_counts['unchanged']

        return {
            'changedCount': changed_count,
            'summary': summary
        }

    def _discover_dois(self) -> Dict[str, List[str]]:
        """
        Discover DOIs using multiple methods.

        Returns:
            Dictionary mapping harvest basis to list of DOIs
        """
        dois = {}

        # Method 1: DOIs with unknown status or needing reharvest
        self.log("Method 1: Checking DOIs needing refresh...")
        dois['DOI needing refresh'] = self._get_dois_needing_refresh()

        # Method 2: New DOIs from any source
        self.log("Method 2: Checking for new DOIs...")
        dois['New DOI from any source'] = self._get_new_dois()

        # Method 3: Query by faculty ORCIDs
        self.log("Method 3: Querying by faculty ORCIDs...")
        dois['DOI from faculty ORCID'] = self._query_by_orcids()

        # Method 4: Query by affiliation
        self.log("Method 4: Querying by affiliation...")
        dois['DOI from affiliation query'] = self._query_by_affiliation()

        # Method 5: Query by funder
        self.log("Method 5: Querying by funder...")
        dois['DOI from funder query'] = self._query_by_funder()

        return dois

    def _get_dois_needing_refresh(self) -> List[str]:
        """Get DOIs that need metadata refresh (>1 year old)."""
        dois = self.db.get_values(
            f"""SELECT DISTINCT LOWER(value) as doi FROM metadata
                WHERE field = 'dc.identifier.doi'
                AND deleted IS NULL
                AND (
                    value IN (
                        SELECT idInSource FROM metadata
                        WHERE source = 'doi' AND field = 'doi.agency.id'
                        AND value = 'crossref' AND deleted IS NULL
                    )
                    OR value IN (
                        SELECT idInSource FROM metadata
                        WHERE source = 'doi' AND field = 'doi.status'
                        AND value = 'unknown' AND deleted IS NULL
                    )
                )
                AND value NOT IN (
                    SELECT idInSource FROM sourceData
                    WHERE source = 'crossref' AND added > '{self.one_year_ago}'
                    AND deleted IS NULL
                )""",
            column='doi'
        )
        self.log(f"  Found {len(dois)} DOIs needing refresh")
        return dois

    def _get_new_dois(self) -> List[str]:
        """Get new DOIs not yet in doi table."""
        dois = self.db.get_values(
            """SELECT DISTINCT LOWER(value) as doi FROM metadata
               WHERE field = 'dc.identifier.doi'
               AND LOWER(value) NOT IN (
                   SELECT LOWER(idInSource) FROM metadata
                   WHERE source = 'doi'
               )""",
            column='doi'
        )
        self.log(f"  Found {len(dois)} new DOIs")
        return dois

    def _query_by_orcids(self) -> List[str]:
        """Query Crossref by faculty ORCIDs."""
        dois = []

        # Get active faculty ORCIDs
        persons = self.db.get_values(
            """SELECT DISTINCT m.idInSource FROM metadata m
               WHERE source = 'local' AND field = 'local.employment.type'
               AND value = 'Faculty' AND deleted IS NULL
               AND parentRowID NOT IN (
                   SELECT parentRowID FROM metadata
                   WHERE source = 'local' AND idInSource = m.idInSource
                   AND field = 'local.date.end' AND deleted IS NULL
               )""",
            column='idInSource'
        )

        for id_in_source in persons:
            orcid = self.db.get_values(
                """SELECT value FROM metadata
                   WHERE source = 'local' AND idInSource = %s
                   AND field = 'dc.identifier.orcid' AND deleted IS NULL
                   LIMIT 1""",
                (id_in_source,),
                column='value',
                single_value=True
            )

            if not orcid:
                continue

            # Query Crossref API
            url = (
                f"{self.api_url}works"
                f"?filter=orcid:{orcid},from-created-date:{self.one_week_ago}"
                f"&select=DOI&mailto={self.ir_email}"
            )

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()

                total = data['message']['total-results']
                if total > 0:
                    self.log(f"  ORCID {orcid}: {total} results")

                    for item in data['message']['items']:
                        doi = item['DOI'].lower()

                        # Check if already in Crossref
                        existing = self.db.query_one(
                            """SELECT idInSource FROM metadata
                               WHERE source = 'crossref' AND field = 'dc.identifier.doi'
                               AND value = %s""",
                            (doi,)
                        )

                        if not existing:
                            dois.append(doi)

            except requests.RequestException as e:
                self.log_error(f"ORCID query failed for {orcid}: {e}")

            time.sleep(self.delay)

        self.log(f"  Found {len(dois)} DOIs from ORCIDs")
        return dois

    def _query_by_affiliation(self) -> List[str]:
        """Query Crossref by institutional affiliation."""
        dois = []

        url = (
            f"{self.api_url}works"
            f"?query.affiliation={self.institution_abbrev}"
            f"&query.affiliation={self.institution_city}"
            f"&filter=from-created-date:{self.one_week_ago}"
            f"&rows=50&mailto={self.ir_email}"
        )

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            total = data['message']['total-results']
            self.log(f"  Affiliation query: {total} results")

            for item in data['message']['items']:
                doi = item['DOI'].lower()

                existing = self.db.query_one(
                    """SELECT idInSource FROM metadata
                       WHERE source = 'crossref' AND field = 'dc.identifier.doi'
                       AND value = %s""",
                    (doi,)
                )

                if not existing:
                    dois.append(doi)

        except requests.RequestException as e:
            self.log_error(f"Affiliation query failed: {e}")

        self.log(f"  Found {len(dois)} DOIs from affiliation")
        return dois

    def _query_by_funder(self) -> List[str]:
        """Query Crossref by institutional funder ID."""
        dois = []

        # First get funder IDs
        url = f"{self.api_url}funders?query={self.institution_abbrev}&mailto={self.ir_email}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            for funder in data['message']['items'][:1]:  # Limit to first result
                funder_id = funder['id']
                self.log(f"  Checking funder: {funder_id}")

                works_url = (
                    f"{self.api_url}works"
                    f"?filter=funder:{funder_id},from-created-date:{self.one_week_ago}"
                    f"&rows=50&mailto={self.ir_email}"
                )

                works_response = requests.get(works_url, timeout=30)
                works_response.raise_for_status()
                works_data = works_response.json()

                for item in works_data['message']['items']:
                    doi = item['DOI'].lower()

                    existing = self.db.query_one(
                        """SELECT idInSource FROM metadata
                           WHERE source = 'crossref' AND field = 'dc.identifier.doi'
                           AND value = %s""",
                        (doi,)
                    )

                    if not existing:
                        dois.append(doi)

                time.sleep(self.delay)

        except requests.RequestException as e:
            self.log_error(f"Funder query failed: {e}")

        self.log(f"  Found {len(dois)} DOIs from funder")
        return dois

    def _get_dois_to_reprocess(self) -> List[str]:
        """Get DOIs to reprocess from existing Crossref data."""
        return self.db.get_values(
            """SELECT DISTINCT value FROM metadata
               WHERE source = 'crossref' AND field = 'dc.identifier.doi'
               AND deleted IS NULL""",
            column='value'
        )

    def _get_dois_to_reharvest(self) -> List[str]:
        """Get all Crossref DOIs to reharvest."""
        return self.db.get_values(
            """SELECT DISTINCT value FROM metadata
               WHERE source = 'crossref' AND field = 'dc.identifier.doi'
               AND deleted IS NULL""",
            column='value'
        )

    def _process_doi(self, doi: str, harvest_basis: str):
        """Process a single DOI."""
        self.log(f"  DOI: {doi}")

        # Retrieve metadata from Crossref
        metadata = self._retrieve_crossref_metadata(doi)

        if not metadata:
            return

        self.record_counts['all'] += 1

        # Process the record
        result = self.process_record(metadata)
        record_type = result['recordType']

        self.log(f"    Status: {record_type}")
        self.record_counts[record_type] += 1

        # Add to processing queue
        process_result = self.add_to_process(
            doi,
            'dc.identifier.doi',
            check_crossref=False,
            harvest_basis=harvest_basis
        )
        self.log(f"    IRTS: {process_result['status']}")

    def _retrieve_crossref_metadata(self, doi: str) -> Optional[Dict[str, Any]]:
        """Retrieve metadata from Crossref API."""
        url = f"{self.api_url}works/{doi}?mailto={self.ir_email}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data['message']
        except requests.RequestException as e:
            self.log_error(f"Failed to retrieve DOI {doi}: {e}")
            return None

    def process_record(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single Crossref record.

        Args:
            metadata: JSON metadata from Crossref API

        Returns:
            Dictionary with idInSource and recordType
        """
        import json

        doi = metadata['DOI'].lower()

        # Save source data
        source_data_json = json.dumps(metadata, ensure_ascii=False)
        result = self.metadata_manager.save_source_data(
            self.source, doi, source_data_json, 'JSON'
        )
        record_type = result['recordType']

        # Build metadata record
        record = {}

        # Map common fields
        field_mappings = {
            'title': 'dc.title',
            'publisher': 'dc.publisher',
            'DOI': 'dc.identifier.doi',
            'type': 'dc.type',
            'ISSN': 'dc.identifier.issn',
            'ISBN': 'dc.identifier.isbn',
            'URL': 'dc.identifier.uri',
            'volume': 'dc.bibliographicCitation.volume',
            'issue': 'dc.bibliographicCitation.issue',
            'page': 'dc.bibliographicCitation.pages'
        }

        for source_field, target_field in field_mappings.items():
            if source_field in metadata:
                value = metadata[source_field]

                # Handle arrays
                if isinstance(value, list):
                    if value:
                        value = value[0]
                    else:
                        continue

                if value:
                    record[target_field] = [{'value': str(value)}]

        # Authors
        if 'author' in metadata:
            authors = []
            for author in metadata['author']:
                if 'family' in author:
                    name = author.get('family', '')
                    if 'given' in author:
                        name = f"{name}, {author['given']}"
                    authors.append({'value': name})
            if authors:
                record['dc.contributor.author'] = authors

        # Date
        if 'published' in metadata or 'published-print' in metadata:
            date_parts = None
            if 'published' in metadata:
                date_parts = metadata['published'].get('date-parts', [[]])[0]
            elif 'published-print' in metadata:
                date_parts = metadata['published-print'].get('date-parts', [[]])[0]

            if date_parts:
                date_str = '-'.join(str(p).zfill(2) for p in date_parts)
                record['dc.date.issued'] = [{'value': date_str}]

        # Save metadata
        self.metadata_manager.save_values(self.source, doi, record, None)

        return {'idInSource': doi, 'recordType': record_type}
