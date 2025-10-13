"""
Metadata management for saving and versioning metadata records.

Implements the core logic for saving metadata values with version control,
similar to the PHP saveValue/saveValues functions.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
import structlog
from .database import db

logger = structlog.get_logger()


class MetadataManager:
    """Manages metadata storage with version control."""

    def __init__(self):
        self.db = db

    def save_source_data(
        self,
        source: str,
        id_in_source: str,
        source_data: str,
        format: str
    ) -> Dict[str, Any]:
        """
        Save raw source data (XML/JSON) to sourceData table.

        Args:
            source: Source system name
            id_in_source: ID in source system
            source_data: Raw XML or JSON string
            format: 'XML' or 'JSON'

        Returns:
            Dictionary with recordType ('new', 'modified', 'unchanged')
        """
        # Check for existing source data
        existing = self.db.query_one(
            """SELECT rowID, sourceData FROM sourceData
               WHERE source = %s AND idInSource = %s AND deleted IS NULL""",
            (source, id_in_source)
        )

        if not existing:
            # Insert new source data
            self.db.insert('sourceData', {
                'source': source,
                'idInSource': id_in_source,
                'sourceData': source_data,
                'format': format,
                'added': datetime.now()
            })
            record_type = 'new'
            logger.info("source_data_saved", source=source, id=id_in_source, type=record_type)
        elif existing['sourceData'] != source_data:
            # Update if changed
            row_id = existing['rowID']
            new_row_id = self.db.insert('sourceData', {
                'source': source,
                'idInSource': id_in_source,
                'sourceData': source_data,
                'format': format,
                'added': datetime.now()
            })
            self.db.update(
                'sourceData',
                {'deleted': datetime.now(), 'replacedByRowID': new_row_id},
                {'rowID': row_id}
            )
            record_type = 'modified'
            logger.info("source_data_updated", source=source, id=id_in_source, type=record_type)
        else:
            record_type = 'unchanged'

        return {'recordType': record_type}

    def save_value(
        self,
        source: str,
        id_in_source: str,
        field: str,
        place: int,
        value: Any,
        parent_row_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Save a single metadata value with version control.

        Args:
            source: Source system name
            id_in_source: ID in source system
            field: Metadata field name (e.g., 'dc.title')
            place: Position/order of value (0-indexed)
            value: Metadata value
            parent_row_id: Parent row ID if this is a child field

        Returns:
            Dictionary with rowID and status ('new', 'updated', 'unchanged')
        """
        # Convert boolean to string
        if isinstance(value, bool):
            value = 'TRUE' if value else 'FALSE'

        # Trim string values
        if isinstance(value, str):
            value = value.strip()

        # Build query based on whether there's a parent
        if parent_row_id is None:
            existing = self.db.query_one(
                """SELECT rowID, value FROM metadata
                   WHERE source = %s AND idInSource = %s
                   AND parentRowID IS NULL AND field = %s
                   AND place = %s AND deleted IS NULL""",
                (source, id_in_source, field, place)
            )
        else:
            existing = self.db.query_one(
                """SELECT rowID, value FROM metadata
                   WHERE source = %s AND idInSource = %s
                   AND parentRowID = %s AND field = %s
                   AND place = %s AND deleted IS NULL""",
                (source, id_in_source, parent_row_id, field, place)
            )

        if not existing:
            # Insert new metadata
            row_id = self.db.insert('metadata', {
                'source': source,
                'idInSource': id_in_source,
                'parentRowID': parent_row_id,
                'field': field,
                'place': place,
                'value': value,
                'added': datetime.now()
            })
            status = 'new'
        elif existing['value'] != value:
            # Insert new version and mark old as deleted
            existing_row_id = existing['rowID']
            new_row_id = self.db.insert('metadata', {
                'source': source,
                'idInSource': id_in_source,
                'parentRowID': parent_row_id,
                'field': field,
                'place': place,
                'value': value,
                'added': datetime.now()
            })
            self.db.update(
                'metadata',
                {'deleted': datetime.now(), 'replacedByRowID': new_row_id},
                {'rowID': existing_row_id}
            )

            # Mark children as deleted too
            self._mark_extra_metadata_as_deleted(source, id_in_source, existing_row_id, '', '', [])

            row_id = new_row_id
            status = 'updated'
        else:
            # Unchanged
            row_id = existing['rowID']
            status = 'unchanged'

        return {'rowID': row_id, 'status': status}

    def save_values(
        self,
        source: str,
        id_in_source: str,
        record: Dict[str, Any],
        parent_row_id: Optional[int] = None,
        existing_fields_to_ignore: Optional[List[str]] = None,
        complete_record: bool = True
    ) -> str:
        """
        Recursively save metadata record with nested fields.

        Args:
            source: Source system name
            id_in_source: ID in source system
            record: Dictionary of field -> list of value dictionaries
            parent_row_id: Parent row ID for nested values
            existing_fields_to_ignore: Fields to not mark as deleted
            complete_record: Whether this is a complete record (mark unused fields as deleted)

        Returns:
            Report string with save operations
        """
        report = []
        current_fields = []

        for field, values in record.items():
            current_fields.append(field)

            # Handle flat string values
            if isinstance(values, str):
                values = [{'value': values}]

            # Ensure values is a list
            if not isinstance(values, list):
                values = [values]

            for place, value_dict in enumerate(values):
                if not value_dict or 'value' not in value_dict:
                    continue

                value = value_dict['value']

                if value is None or (isinstance(value, str) and not value.strip()):
                    continue

                # Save the value
                result = self.save_value(source, id_in_source, field, place, value, parent_row_id)
                row_id = result['rowID']
                status = result['status']

                report.append(
                    f"{source} {id_in_source}: {field} {place} child of {parent_row_id} - {status}"
                )

                # Recursively save children
                if 'children' in value_dict and value_dict['children']:
                    child_report = self.save_values(
                        source, id_in_source, value_dict['children'], row_id, None, False
                    )
                    report.append(child_report)

            # Mark extra metadata as deleted (values with place > current count)
            if values:
                last_place = len(values) - 1
                self._mark_extra_metadata_as_deleted(
                    source, id_in_source, parent_row_id, field, last_place, []
                )

        # Mark fields no longer in record as deleted
        if complete_record:
            if existing_fields_to_ignore:
                current_fields.extend(existing_fields_to_ignore)

            self._mark_extra_metadata_as_deleted(
                source, id_in_source, parent_row_id, '', '', current_fields
            )

        return '\n'.join(report)

    def _mark_extra_metadata_as_deleted(
        self,
        source: str,
        id_in_source: str,
        parent_row_id: Optional[int],
        field: str,
        last_place: Any,
        current_fields: List[str]
    ):
        """Mark metadata entries as deleted based on various criteria."""

        if field:
            # Mark entries with place > last_place as deleted
            if parent_row_id is None:
                query = """UPDATE metadata SET deleted = %s
                           WHERE source = %s AND idInSource = %s
                           AND parentRowID IS NULL AND field = %s
                           AND place > %s AND deleted IS NULL"""
                params = (datetime.now(), source, id_in_source, field, last_place)
            else:
                query = """UPDATE metadata SET deleted = %s
                           WHERE source = %s AND idInSource = %s
                           AND parentRowID = %s AND field = %s
                           AND place > %s AND deleted IS NULL"""
                params = (datetime.now(), source, id_in_source, parent_row_id, field, last_place)

            self.db.execute(query, params)
        elif current_fields:
            # Mark fields not in current_fields as deleted
            if parent_row_id is None:
                placeholders = ','.join(['%s'] * len(current_fields))
                query = f"""UPDATE metadata SET deleted = %s
                            WHERE source = %s AND idInSource = %s
                            AND parentRowID IS NULL
                            AND field NOT IN ({placeholders})
                            AND deleted IS NULL"""
                params = (datetime.now(), source, id_in_source, *current_fields)
            else:
                placeholders = ','.join(['%s'] * len(current_fields))
                query = f"""UPDATE metadata SET deleted = %s
                            WHERE source = %s AND idInSource = %s
                            AND parentRowID = %s
                            AND field NOT IN ({placeholders})
                            AND deleted IS NULL"""
                params = (datetime.now(), source, id_in_source, parent_row_id, *current_fields)

            self.db.execute(query, params)
        elif parent_row_id is not None:
            # Mark all children of parent as deleted
            query = """UPDATE metadata SET deleted = %s
                       WHERE source = %s AND idInSource = %s
                       AND parentRowID = %s AND deleted IS NULL"""
            params = (datetime.now(), source, id_in_source, parent_row_id)
            self.db.execute(query, params)


# Global metadata manager instance
metadata_manager = MetadataManager()
