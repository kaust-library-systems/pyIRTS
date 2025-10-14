# pyIRTS - Python IRTS Harvest System

A Python implementation of the IRTS (Institutional Research Tracking System) metadata harvesting system. This system harvests research publication metadata from multiple academic sources and manages it in a standardized Dublin Core format with full version control using SQLite.

## Features

- **Multiple Source Support**: Harvest from arXiv, Crossref, and other academic metadata sources
- **Version Control**: Track all metadata changes with full history
- **Field Mapping**: Automatic mapping from source-specific fields to Dublin Core standard
- **Deduplication**: Intelligent detection of duplicate records across sources
- **Configurable Transformations**: Apply custom transformations to metadata values
- **Rate Limiting**: Respects API rate limits for each source
- **Email Notifications**: Automatic harvest reports via email

## Architecture

### Core Components

```
pyIRTS/
core/
database.py      # Database connection and query utilities
metadata.py      # Metadata saving with version control
mapper.py        # Field mapping and transformations
harvesters/
base.py          # Base harvester class
arxiv.py         # ArXiv harvester
crossref.py      # Crossref harvester
harvest.py           # Main orchestrator script
requirements.txt     # Python dependencies
config.env.template  # Configuration template
```

As a tree view

```
mgarcia@PC-KL-26743:~/Work/pyIRTS$ tree
.
├── LICENSE
├── README.md
├── config.env.template
├── core
│   ├── __init__.py
│   ├── database.py
│   ├── mapper.py
│   └── metadata.py
├── harvest.py
├── harvesters
│   ├── __init__.py
│   ├── arxiv.py
│   ├── base.py
│   └── crossref.py
└── requirements.txt

3 directories, 13 files
```

### Key Concepts

**Version Control**: The system never deletes metadata - it marks old values as deleted and links them to replacement values, maintaining a complete audit trail.

**Field Mapping**: Source-specific field names are mapped to standardized Dublin Core fields using the `mappings` database table.

**Harvest Types**:
- `new`: Harvest most recent items only (default)
- `reprocess`: Reprocess existing metadata without querying sources
- `reharvest`: Reharvest known items from sources
- `requery`: Full iteration through source

## Installation

1. **Clone or copy the code** to your server:
   ```bash
   cd /home/mgarcia/Work/pyIRTS
   ```

2. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the system**:
   ```bash
   cp config.env.template config.env
   nano config.env
   ```

   Update the following settings:
   - Database path (DB_PATH, default: irts.sqlite)
   - Institution details (INSTITUTION_ABBREVIATION, INSTITUTION_CITY)
   - Email settings (IR_EMAIL, SMTP_HOST, SMTP_FROM, SMTP_TO)
   - API endpoints (if different from defaults)

5. **Initialize database** (automatic on first connection):
   ```python
   python3 -c "from core.database import db; db.connect(); print('Connected! Database initialized.')"
   ```

   The database schema is automatically created from `schema.sql` on first connection.

## Usage

### Basic Harvest

Harvest from a single source:
```bash
python harvest.py --source arxiv
```

Harvest from multiple sources:
```bash
python harvest.py --source arxiv,crossref
```

### Harvest Types

Harvest new items only (default):
```bash
python harvest.py --source crossref --type new
```

Reprocess existing metadata:
```bash
python harvest.py --source arxiv --type reprocess
```

Reharvest from source:
```bash
python harvest.py --source crossref --type reharvest
```

### Scheduled Harvests

Add to crontab for automated harvesting:

```bash
# Daily harvest at 2 AM
0 2 * * * cd /home/mgarcia/Work/pyIRTS && ./venv/bin/python harvest.py --source crossref,arxiv

# Frequent DSpace harvests every 10 minutes
*/10 * * * * cd /home/mgarcia/Work/pyIRTS && ./venv/bin/python harvest.py --source dspace
```

## Database Schema

The system uses SQLite with the schema defined in `schema.sql`:

### Main Tables

**metadata**: Standardized metadata in Dublin Core format
- `source`: Source system name
- `idInSource`: ID in source system
- `field`: Metadata field (e.g., 'dc.title')
- `value`: Metadata value
- `place`: Order/position of value
- `parentRowID`: Parent row for nested metadata
- `deleted`: Timestamp when marked as deleted
- `replacedByRowID`: Link to replacement row

**sourceData**: Raw XML/JSON from sources
- `source`: Source system name
- `idInSource`: ID in source system
- `sourceData`: Raw XML or JSON
- `format`: 'XML' or 'JSON'

**mappings**: Field name mappings
- `source`: Source system name
- `sourceField`: Field name in source
- `parentFieldInSource`: Parent field (for nested)
- `standardField`: Standard Dublin Core field

**transformations**: Value transformation rules
- `source`: Source system name
- `field`: Field to transform
- `transformationType`: Type of transformation
- `transformationParameter`: Transformation parameter
- `transformationValue`: Replacement value
- `priority`: Order of application

## Adding New Harvesters

To add a new metadata source:

1. **Create a new harvester class** in `harvesters/`:

```python
from .base import BaseHarvester

class MySourceHarvester(BaseHarvester):
    def __init__(self):
        super().__init__('mysource')
        # Initialize API settings

    def harvest(self, harvest_type: str):
        # Implement discovery logic
        pass

    def process_record(self, item):
        # Process individual record
        pass
```

2. **Register the harvester** in `harvest.py`:

```python
from harvesters.mysource import MySourceHarvester

HARVESTERS = {
    'arxiv': ArxivHarvester,
    'crossref': CrossrefHarvester,
    'mysource': MySourceHarvester,  # Add here
}
```

3. **Add field mappings** to the database:

```sql
INSERT INTO mappings (source, sourceField, parentFieldInSource, standardField)
VALUES ('mysource', 'title', '', 'dc.title');
```

## Extending Functionality

### Custom Transformations

Add transformations to the database:

```sql
INSERT INTO transformations
(source, field, transformationType, transformationParameter, transformationValue, priority)
VALUES ('arxiv', 'dc.identifier.uri', 'prefix', 'https://', '', 1);
```

Supported transformation types:
- `replace`: Replace substring
- `regex`: Regular expression replacement
- `uppercase`: Convert to uppercase
- `lowercase`: Convert to lowercase
- `strip`: Strip characters
- `prefix`: Add prefix
- `suffix`: Add suffix

### Custom Logging

Configure logging in `harvest.py`:

```python
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()  # Human-readable console output
    ]
)
```

## Troubleshooting

### Database Connection Issues

```bash
# Test database connection
python3 -c "from core.database import db; db.connect()"
```

Check config.env settings and ensure database is accessible.

### Import Errors

```bash
# Ensure you're in the virtual environment
source venv/bin/activate

# Verify dependencies
pip list
```

### API Rate Limiting

If hitting rate limits, adjust delays in config.env:
```
ARXIV_DELAY=5
CROSSREF_DELAY=2
```


## Key Features

- **SQLite database** - No database server required, file-based storage
- **Auto-initialization** - Database schema created automatically from `schema.sql`
- **Type hints** - Better code clarity and IDE support
- **Structured logging** - JSON-formatted logs via structlog
- **Version control** - Full audit trail of all metadata changes
- **Deduplication** - Intelligent duplicate detection across sources

## Contributing

To add new features:

1. Create harvester classes inheriting from `BaseHarvester`
2. Add field mappings to database
3. Register harvesters in `harvest.py`
4. Update this README

## License

Internal use for KAUST Library.

## Support

For issues or questions, contact the Institutional Repository team.
