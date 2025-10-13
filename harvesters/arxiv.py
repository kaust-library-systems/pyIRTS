"""
ArXiv metadata harvester.

Harvests preprint metadata from arXiv.org based on faculty names.
"""

from typing import Dict, Any
import time
import os
import requests
from lxml import etree
import structlog
from .base import BaseHarvester

logger = structlog.get_logger()


class ArxivHarvester(BaseHarvester):
    """Harvester for arXiv metadata."""

    def __init__(self):
        super().__init__('arxiv')
        self.api_url = os.getenv('ARXIV_API', 'http://export.arxiv.org/api/query')
        self.delay = int(os.getenv('ARXIV_DELAY', '3'))
        self.current_year = str(time.localtime().tm_year)

    def harvest(self, harvest_type: str = 'new') -> Dict[str, Any]:
        """
        Harvest arXiv metadata.

        Args:
            harvest_type: 'new', 'reharvest', etc.

        Returns:
            Dictionary with changedCount and summary
        """
        self.log(f"Starting arXiv harvest (type: {harvest_type})")

        if harvest_type == 'reharvest':
            self._reharvest()
        else:
            self._harvest_by_faculty_names()

        summary = self.save_report()
        changed_count = self.record_counts['all'] - self.record_counts['unchanged']

        return {
            'changedCount': changed_count,
            'summary': summary
        }

    def _harvest_by_faculty_names(self):
        """Harvest based on active faculty member names."""
        harvest_basis = 'Harvested based on active faculty member name'

        # Get active faculty
        persons = self.db.get_values(
            """SELECT DISTINCT m.idInSource FROM metadata m
               WHERE source = 'local'
               AND field = 'local.employment.type'
               AND value = 'Faculty'
               AND deleted IS NULL
               AND parentRowID NOT IN (
                   SELECT parentRowID FROM metadata
                   WHERE source = 'local'
                   AND idInSource = m.idInSource
                   AND field = 'local.date.end'
                   AND deleted IS NULL
               )""",
            column='idInSource'
        )

        # Names that are too common to search
        skip_names = [
            'Wang, Peng', 'Wu, Ying', 'Gao, Xin', 'Han, Yu',
            'Li, Mo', 'Wang, Di', 'Sun, Ying', 'Zhang, Huabin'
        ]

        for id_in_source in persons:
            name = self.db.get_values(
                """SELECT value FROM metadata
                   WHERE source = 'local' AND idInSource = %s
                   AND field = 'local.person.name' AND deleted IS NULL
                   LIMIT 1""",
                (id_in_source,),
                column='value',
                single_value=True
            )

            if not name:
                continue

            self.log(f"Checking arXiv for: {name}")

            if name in skip_names:
                self.log(f"  Skipped - Name is too common")
                continue

            # Query arXiv API
            xml_response = self._retrieve_arxiv_metadata('name', name)

            if not xml_response:
                self.log(f"  Error retrieving metadata for: {name}")
                continue

            # Parse XML
            try:
                root = etree.fromstring(xml_response.encode('utf-8'))
                ns = {'atom': 'http://www.w3.org/2005/Atom'}

                entries = root.findall('.//atom:entry', namespaces=ns)

                for entry in entries:
                    arxiv_id = entry.find('atom:id', namespaces=ns)
                    if arxiv_id is not None:
                        arxiv_id = arxiv_id.text
                        self.log(f"  Processing: {arxiv_id}")

                    published = entry.find('atom:published', namespaces=ns)
                    if published is not None and self.current_year in published.text:
                        self.record_counts['all'] += 1

                        # Process the record
                        result = self.process_record(entry)
                        record_type = result['recordType']
                        id_in_source = result['idInSource']

                        self.log(f"    arXiv status: {record_type}")
                        self.record_counts[record_type] += 1

                        # Add to processing queue
                        process_result = self.add_to_process(
                            id_in_source,
                            'dc.identifier.arxivid',
                            check_crossref=True,
                            harvest_basis=harvest_basis
                        )
                        self.log(f"    IRTS status: {process_result['status']}")
                    else:
                        self.record_counts['skipped'] += 1
                        self.log(f"    Skipped - Not from current year")

            except etree.XMLSyntaxError as e:
                self.log_error(f"XML parsing error for {name}: {e}")

            # Rate limiting
            time.sleep(self.delay)

    def _reharvest(self):
        """Reharvest existing arXiv IDs."""
        # Get all arXiv IDs from repository
        arxiv_ids = self.db.get_values(
            """SELECT DISTINCT value FROM metadata
               WHERE source = 'repository'
               AND field = 'dc.identifier.arxivid'
               AND deleted IS NULL""",
            column='value'
        )

        self.log(f"Reharvesting {len(arxiv_ids)} arXiv IDs")

        for arxiv_id in arxiv_ids:
            xml_response = self._retrieve_arxiv_metadata('arxivID', arxiv_id)

            if xml_response:
                try:
                    root = etree.fromstring(xml_response.encode('utf-8'))
                    ns = {'atom': 'http://www.w3.org/2005/Atom'}
                    entry = root.find('.//atom:entry', namespaces=ns)

                    if entry is not None:
                        result = self.process_record(entry)
                        record_type = result['recordType']
                        self.record_counts[record_type] += 1
                        self.log(f"  {arxiv_id}: {record_type}")

                except etree.XMLSyntaxError as e:
                    self.log_error(f"XML parsing error for {arxiv_id}: {e}")

            time.sleep(self.delay)

    def process_record(self, entry: etree.Element) -> Dict[str, Any]:
        """
        Process a single arXiv entry.

        Args:
            entry: XML element containing arXiv entry

        Returns:
            Dictionary with idInSource and recordType
        """
        ns = {'atom': 'http://www.w3.org/2005/Atom'}

        # Extract arXiv ID
        arxiv_url = entry.find('atom:id', namespaces=ns).text
        arxiv_id_parts = arxiv_url.replace('http://arxiv.org/abs/', '').split('v')
        arxiv_id = arxiv_id_parts[0]
        arxiv_version = arxiv_id_parts[1] if len(arxiv_id_parts) > 1 else '1'

        # Save source data
        xml_string = etree.tostring(entry, encoding='unicode')
        result = self.metadata_manager.save_source_data(
            self.source, arxiv_id, xml_string, 'XML'
        )
        record_type = result['recordType']

        # Build metadata record
        record = {
            'dc.type': [{'value': 'Preprint'}],
            'dc.publisher': [{'value': 'arXiv'}],
            'dc.version': [{'value': arxiv_version}]
        }

        # Process all child elements
        for element in entry:
            # Remove namespace from tag
            tag = element.tag.replace('{http://www.w3.org/2005/Atom}', '')
            tag = tag.replace('{http://arxiv.org/schemas/atom}', '')

            # Map field
            field = self.field_mapper.map_field(self.source, tag, '')

            # Get value
            value = element.text or ''
            value = value.strip()

            # Transform value
            value = self.field_mapper.transform(self.source, field, element, value)

            if value:
                if field not in record:
                    record[field] = []
                record[field].append({'value': value})

        # Save metadata
        self.metadata_manager.save_values(self.source, arxiv_id, record, None)

        return {'idInSource': arxiv_id, 'recordType': record_type}

    def _retrieve_arxiv_metadata(self, search_type: str, search_value: str) -> str:
        """
        Retrieve metadata from arXiv API.

        Args:
            search_type: 'name', 'arxivID', etc.
            search_value: Value to search for

        Returns:
            XML response string
        """
        if search_type == 'name':
            query = f'au:"{search_value}"'
            params = {
                'search_query': query,
                'start': 0,
                'max_results': 100,
                'sortBy': 'lastUpdatedDate',
                'sortOrder': 'descending'
            }
        elif search_type == 'arxivID':
            params = {
                'id_list': search_value
            }
        else:
            return ''

        try:
            response = requests.get(self.api_url, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.log_error(f"API request failed: {e}")
            return ''
