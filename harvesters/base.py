"""
Base harvester class with common functionality.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import structlog
from datetime import datetime
from core.database import db
from core.metadata import metadata_manager
from core.mapper import field_mapper

logger = structlog.get_logger()


class BaseHarvester(ABC):
    """Base class for all metadata source harvesters."""

    def __init__(self, source_name: str):
        self.source = source_name
        self.db = db
        self.metadata_manager = metadata_manager
        self.field_mapper = field_mapper
        self.report = []
        self.errors = []
        self.record_counts = {
            'all': 0,
            'new': 0,
            'modified': 0,
            'deleted': 0,
            'unchanged': 0,
            'skipped': 0
        }

    @abstractmethod
    def harvest(self, harvest_type: str = 'new') -> Dict[str, Any]:
        """
        Main harvest method to be implemented by each source.

        Args:
            harvest_type: Type of harvest ('new', 'reprocess', 'reharvest', 'requery')

        Returns:
            Dictionary with 'changedCount' and 'summary'
        """
        pass

    @abstractmethod
    def process_record(self, item: Any) -> Dict[str, Any]:
        """
        Process a single record from the source.

        Args:
            item: Raw item from source (XML element, JSON dict, etc.)

        Returns:
            Dictionary with 'idInSource' and 'recordType'
        """
        pass

    def add_to_process(
        self,
        id_in_source: str,
        id_in_source_field: str,
        check_crossref: bool = False,
        harvest_basis: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Add item to processing queue if not already existing.

        Args:
            id_in_source: ID in source system
            id_in_source_field: Standard field name for the ID
            check_crossref: Whether to check for Crossref DOI
            harvest_basis: Reason for harvesting this item

        Returns:
            Dictionary with 'status' and 'idInIRTS'
        """
        # Get basic metadata
        type_value = self.db.get_values(
            """SELECT value FROM metadata
               WHERE source = %s AND idInSource = %s
               AND field = 'dc.type' AND deleted IS NULL
               LIMIT 1""",
            (self.source, id_in_source),
            column='value',
            single_value=True
        )

        doi = self.db.get_values(
            """SELECT value FROM metadata
               WHERE source = %s AND idInSource = %s
               AND field = 'dc.identifier.doi' AND deleted IS NULL
               LIMIT 1""",
            (self.source, id_in_source),
            column='value',
            single_value=True
        )

        title = self.db.get_values(
            """SELECT value FROM metadata
               WHERE source = %s AND idInSource = %s
               AND field = 'dc.title' AND deleted IS NULL
               LIMIT 1""",
            (self.source, id_in_source),
            column='value',
            single_value=True
        )

        date_issued = self.db.get_values(
            """SELECT value FROM metadata
               WHERE source = %s AND idInSource = %s
               AND field = 'dc.date.issued' AND deleted IS NULL
               LIMIT 1""",
            (self.source, id_in_source),
            column='value',
            single_value=True
        )

        # Check for existing records
        add_to_process = True

        # Check by ID in source field
        existing = self._check_existing_records(id_in_source, id_in_source_field)
        if existing:
            add_to_process = False

        # Check by DOI
        if doi and id_in_source_field != 'dc.identifier.doi':
            existing = self._check_existing_records(doi, 'dc.identifier.doi')
            if existing:
                add_to_process = False

            # Check in IRTS
            existing = self._check_existing_records(doi, 'dc.identifier.doi', 'irts')
            if existing:
                add_to_process = False

        # Check by title for non-DOI items
        if not doi and title and type_value:
            title_escaped = self.db.connect().escape_string(title)
            existing = self.db.query(
                f"""SELECT idInSource FROM metadata
                    WHERE source IN ('irts', 'repository')
                    AND field = 'dc.title'
                    AND value = %s
                    AND idInSource IN (
                        SELECT idInSource FROM metadata
                        WHERE source IN ('irts', 'repository')
                        AND field = 'dc.type'
                        AND value = %s
                    )""",
                (title, type_value)
            )
            if existing:
                add_to_process = False

        # Check for existing IRTS entry
        existing = self.db.query(
            """SELECT idInSource FROM metadata
               WHERE source = 'irts'
               AND ((field = 'irts.source' AND value = %s AND deleted IS NULL
                     AND rowID IN (
                         SELECT parentRowID FROM metadata
                         WHERE source = 'irts' AND field = 'irts.idInSource'
                         AND value = %s AND deleted IS NULL
                     ))
                    OR (field = %s AND value = %s AND deleted IS NULL))""",
            (self.source, id_in_source, id_in_source_field, id_in_source)
        )
        if existing:
            add_to_process = False

        if add_to_process:
            # Generate new IRTS ID
            id_in_irts = self._generate_new_id()

            # Save IRTS entry
            result = self.metadata_manager.save_value('irts', id_in_irts, 'irts.source', 1, self.source, None)
            parent_row_id = result['rowID']

            self.metadata_manager.save_value('irts', id_in_irts, 'irts.idInSource', 1, id_in_source, parent_row_id)
            self.metadata_manager.save_value('irts', id_in_irts, 'dc.type', 1, type_value, None)
            self.metadata_manager.save_value('irts', id_in_irts, 'irts.status', 1, 'inProcess', None)
            self.metadata_manager.save_value('irts', id_in_irts, id_in_source_field, 1, id_in_source, None)
            self.metadata_manager.save_value('irts', id_in_irts, 'dc.title', 1, title, None)
            self.metadata_manager.save_value('irts', id_in_irts, 'dc.date.issued', 1, date_issued, None)

            if doi and id_in_source_field != 'dc.identifier.doi':
                self.metadata_manager.save_value('irts', id_in_irts, 'dc.identifier.doi', 1, doi, None)

            if harvest_basis:
                self.metadata_manager.save_value('irts', id_in_irts, 'irts.harvest.basis', 1, harvest_basis, None)

            status = 'inProcess'
            logger.info("added_to_process", source=self.source, id=id_in_source, irts_id=id_in_irts)
        else:
            id_in_irts = ''
            status = 'existing'

        return {'status': status, 'idInIRTS': id_in_irts}

    def _check_existing_records(
        self,
        value: str,
        field: str,
        source: str = 'repository'
    ) -> List[Dict[str, Any]]:
        """Check for existing records by field value."""
        return self.db.query(
            """SELECT idInSource FROM metadata
               WHERE source = %s AND field = %s
               AND value = %s AND deleted IS NULL""",
            (source, field, value)
        )

    def _generate_new_id(self) -> str:
        """Generate new unique ID for IRTS."""
        # Get max existing ID
        result = self.db.query_one(
            """SELECT MAX(CAST(SUBSTRING(idInSource, LENGTH(%s) + 2) AS UNSIGNED)) as max_id
               FROM metadata
               WHERE source = 'irts' AND idInSource LIKE %s""",
            (self.source, f"{self.source}_%")
        )

        if result and result['max_id']:
            next_id = int(result['max_id']) + 1
        else:
            next_id = 1

        return f"{self.source}_{next_id}"

    def save_report(self) -> str:
        """Save harvest report to database and return summary."""
        # Save full report to messages table
        report_text = '\n'.join(self.report)
        self.db.insert('messages', {
            'process': 'harvest',
            'type': 'report',
            'message': f"{self.source} harvest:\n{report_text}",
            'added': datetime.now()
        })

        # Create summary
        summary_lines = [
            f"{self.source} harvest summary:",
            f"  Total: {self.record_counts['all']}",
            f"  New: {self.record_counts['new']}",
            f"  Modified: {self.record_counts['modified']}",
            f"  Unchanged: {self.record_counts['unchanged']}",
            f"  Skipped: {self.record_counts['skipped']}"
        ]

        if self.errors:
            summary_lines.append(f"  Errors: {len(self.errors)}")

        summary = '\n'.join(summary_lines)
        logger.info("harvest_complete", source=self.source, counts=self.record_counts)

        return summary

    def log(self, message: str):
        """Add message to report."""
        self.report.append(message)
        logger.debug("harvest_log", source=self.source, message=message)

    def log_error(self, error: str):
        """Add error to errors list."""
        self.errors.append(error)
        logger.error("harvest_error", source=self.source, error=error)
