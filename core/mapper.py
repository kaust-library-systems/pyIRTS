"""
Field mapping and transformation utilities.

Maps source-specific field names to standardized Dublin Core fields
and applies transformations to values.
"""

from typing import Any, Optional
import structlog
from .database import db

logger = structlog.get_logger()


class FieldMapper:
    """Handles field mapping and value transformations."""

    def __init__(self):
        self.db = db
        self._mapping_cache = {}
        self._transformation_cache = {}

    def map_field(self, source: str, source_field: str, parent_field: str = '') -> str:
        """
        Map source field name to standard field name.

        Args:
            source: Source system name
            source_field: Field name in source system
            parent_field: Parent field name (for nested fields)

        Returns:
            Mapped standard field name
        """
        cache_key = f"{source}:{parent_field}:{source_field}"

        # Check cache
        if cache_key in self._mapping_cache:
            return self._mapping_cache[cache_key]

        # Query mappings table
        result = self.db.query_one(
            """SELECT standardField FROM mappings
               WHERE source = %s
               AND parentFieldInSource = %s
               AND sourceField = %s""",
            (source, parent_field, source_field)
        )

        if result:
            standard_field = result['standardField']
        elif '.' not in source_field:
            # For non-standard fields, prepend source as namespace
            standard_field = f"{source}.{source_field}"
        else:
            # Already has namespace
            standard_field = source_field

        # Cache the result
        self._mapping_cache[cache_key] = standard_field

        return standard_field

    def transform(
        self,
        source: str,
        field: str,
        element: Any,
        value: Any
    ) -> Any:
        """
        Apply transformations to a metadata value.

        Args:
            source: Source system name
            field: Metadata field name
            element: Original XML/JSON element (for context)
            value: Value to transform

        Returns:
            Transformed value
        """
        # Query transformations table for applicable rules
        cache_key = f"{source}:{field}"

        if cache_key not in self._transformation_cache:
            transformations = self.db.query(
                """SELECT transformationType, transformationParameter, transformationValue
                   FROM transformations
                   WHERE source = %s AND field = %s
                   ORDER BY priority""",
                (source, field)
            )
            self._transformation_cache[cache_key] = transformations

        transformations = self._transformation_cache[cache_key]

        # Apply transformations in order
        for trans in transformations:
            trans_type = trans['transformationType']
            param = trans['transformationParameter']
            trans_value = trans['transformationValue']

            if trans_type == 'replace':
                # Replace substring
                if isinstance(value, str) and param:
                    value = value.replace(param, trans_value or '')

            elif trans_type == 'regex':
                # Regex replacement
                import re
                if isinstance(value, str) and param:
                    try:
                        value = re.sub(param, trans_value or '', value)
                    except re.error as e:
                        logger.warning("regex_error", pattern=param, error=str(e))

            elif trans_type == 'uppercase':
                if isinstance(value, str):
                    value = value.upper()

            elif trans_type == 'lowercase':
                if isinstance(value, str):
                    value = value.lower()

            elif trans_type == 'strip':
                if isinstance(value, str):
                    value = value.strip(param or '')

            elif trans_type == 'prefix':
                if isinstance(value, str) and param:
                    value = param + value

            elif trans_type == 'suffix':
                if isinstance(value, str) and param:
                    value = value + param

        return value

    def clear_cache(self):
        """Clear mapping and transformation caches."""
        self._mapping_cache.clear()
        self._transformation_cache.clear()
        logger.info("mapper_cache_cleared")


# Global field mapper instance
field_mapper = FieldMapper()
