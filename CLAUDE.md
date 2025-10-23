# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pyIRTS is a Python implementation of the IRTS (Institutional Research Tracking System) metadata harvesting system. It harvests research publication metadata from multiple academic sources (arXiv, Crossref, etc.) and stores it in a standardized Dublin Core format with full version control in a SQLite database.

## Common Development Commands

### Setup and Testing
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running Harvests
```bash
# Basic harvest from single source
python harvest.py source

# Harvest from multiple sources
python harvest.py arxiv,crossref

# Different harvest types
python harvest.py arxiv --type new         # Most recent items only (default)
# Possible implementation of options in the future.
# Ignore for now.
python harvest.py arxiv --type reprocess   # Reprocess existing metadata 
python harvest.py arxiv --type reharvest   # Reharvest known items from source
python harvest.py arxiv --type requery     # Full iteration through source
```

## Architecture

### Core Design Principles

**Version Control Philosophy**: The system NEVER deletes metadata permanently. Instead, it:
- Marks old values with a `deleted` timestamp
- Links them to replacement values via `replacedByRowID`
- Maintains complete audit trail of all changes

**Field Mapping Strategy**: Source-specific field names are mapped to Dublin Core standards using the `mappings` database table. The system caches these mappings in memory for performance.

### Module Structure

**core/database.py** - Database connection and query utilities
- `DatabaseConnection` class provides SQLite connection and query methods
- Global `db` instance is used throughout the application
- Automatically initializes schema from `schema.sql` on first connection
- Context manager pattern for automatic commit/rollback
- Helper methods: `insert()`, `update()`, `query()`, `query_one()`, `get_values()`

**core/metadata.py** - Metadata storage with version control
- `MetadataManager` class handles all metadata operations
- `save_value()`: Saves single metadata field with version tracking
- `save_values()`: Recursively saves nested metadata structures
- `save_source_data()`: Stores raw XML/JSON from sources
- `_mark_extra_metadata_as_deleted()`: Handles versioning by marking old values as deleted

**core/mapper.py** - Field mapping and transformations
- `FieldMapper` class maps source fields to Dublin Core
- `map_field()`: Looks up field mappings in database, caches results
- `transform()`: Applies transformation rules (replace, regex, uppercase, lowercase, strip, prefix, suffix)
- Caches both mappings and transformations for performance

**harvesters/base.py** - Base harvester class
- `BaseHarvester`: Abstract class with common functionality
- `add_to_process()`: Deduplication logic - checks for existing records by ID, DOI, or title
- `_generate_new_id()`: Generates unique IRTS IDs in format `{source}_{number}`
- `save_report()`: Saves harvest summary to database
- Tracks record counts (all, new, modified, unchanged, skipped)

**harvest.py** - Main orchestration script
- `HARVESTERS` registry: Maps source names to harvester classes
- `harvest_source()`: Instantiates and runs individual harvester
- `send_email()`: Sends harvest reports via SMTP
- Uses structlog for JSON-formatted logging

### Database Schema

The system uses SQLite with schema defined in `schema.sql`. The database is automatically initialized on first connection. Tables:

**metadata** - Standardized metadata in Dublin Core format
- Version-controlled: `deleted` timestamp and `replacedByRowID` link old to new values
- Hierarchical: `parentRowID` links nested metadata
- Position-tracked: `place` field maintains order

**sourceData** - Raw XML/JSON from sources
- Stores original format ('XML' or 'JSON')
- Also version-controlled like metadata table

**mappings** - Field name mappings
- Maps `sourceField` to `standardField` (Dublin Core)
- Supports nested fields via `parentFieldInSource`

**transformations** - Value transformation rules
- Applied in order by `priority`
- Multiple transformation types: replace, regex, uppercase, lowercase, strip, prefix, suffix

**messages** - Harvest logs and reports

### Configuration

Configuration is via environment variables loaded from `config.env`:
- Database path: `DB_PATH` (default: irts.sqlite)
- Institution details: `INSTITUTION_ABBREVIATION`, `INSTITUTION_CITY`, `IR_EMAIL`
- Email settings: `SMTP_HOST`, `SMTP_FROM`, `SMTP_TO`
- API endpoints: `ARXIV_API`, `CROSSREF_API`
- Rate limiting: `ARXIV_DELAY`, `CROSSREF_DELAY`

### Adding New Harvesters

1. Create new harvester class in `harvesters/` inheriting from `BaseHarvester`
2. Implement `harvest(harvest_type)` and `process_record(item)` methods
3. Register in `harvest.py` HARVESTERS dictionary
4. Add field mappings to database `mappings` table
5. Optionally add transformations to `transformations` table

Example skeleton:
```python
from .base import BaseHarvester

class NewSourceHarvester(BaseHarvester):
    def __init__(self):
        super().__init__('newsource')
        # Initialize API settings from environment

    def harvest(self, harvest_type: str):
        # Main harvest logic
        # Use self.metadata_manager to save data
        # Use self.field_mapper to map/transform fields
        # Use self.log() and self.log_error() for reporting
        return {
            'changedCount': changed_count,
            'summary': self.save_report()
        }

    def process_record(self, item):
        # Process individual record
        # Call metadata_manager.save_source_data()
        # Build metadata record dict
        # Call metadata_manager.save_values()
        return {'idInSource': id_val, 'recordType': type_val}
```

### Metadata Record Structure

When building metadata records to save, use this nested dictionary structure:
```python
record = {
    'dc.title': [{'value': 'Title text'}],
    'dc.contributor.author': [
        {
            'value': 'Author, First',
            'children': {
                'dc.identifier.orcid': [{'value': '0000-0001-2345-6789'}]
            }
        }
    ]
}
metadata_manager.save_values(source, id_in_source, record, parent_row_id=None)
```

### Deduplication Logic

The `add_to_process()` method in `BaseHarvester` checks for existing records in this order:
1. By primary identifier field (e.g., dc.identifier.arxivid)
2. By DOI (dc.identifier.doi) if present
3. By title + type combination for non-DOI items
4. Checks if already exists in IRTS source tracking

Only adds to IRTS processing queue if no existing record found.

### Logging

Uses structlog with JSON output by default (configured in harvest.py):
- `logger.info()`: Important events
- `logger.error()`: Errors
- `logger.debug()`: Detailed debugging

Each harvester also maintains internal report list accessed via `self.log()` and `self.log_error()`.

## Development Notes

- Python 3.8+ required (SQLite3 included in standard library)
- Type hints used throughout for clarity
- SQLite database stored in `irts.sqlite` (file-based, no server required)
- Schema automatically initialized from `schema.sql` on first connection
- Dependencies are minimal and production-stable (see requirements.txt)
- Rate limiting is critical - respect API delays in config.env
- All database operations use parameterized queries (no SQL injection risk)
- Global instances: `db`, `metadata_manager`, `field_mapper` are singletons
