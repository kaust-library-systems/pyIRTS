#!/usr/bin/env python3
"""
Main harvest script for IRTS.

Usage:
    python harvest.py --source arxiv [--type new|reprocess|reharvest]
    python harvest.py --source crossref,europePMC --type new

Arguments:
    --source: Comma-separated list of sources to harvest
    --type: Harvest type (default: new)
        - new: Harvest most recent items
        - reprocess: Reprocess existing metadata
        - reharvest: Reharvest known items from source
        - requery: Full iteration through source
"""

import argparse
import sys
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import structlog
import os
from dotenv import load_dotenv

# Load environment
load_dotenv('config.env')

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()

# Import harvesters
from harvesters.arxiv import ArxivHarvester
from harvesters.crossref import CrossrefHarvester


# Harvester registry
HARVESTERS = {
    'arxiv': ArxivHarvester,
    'crossref': CrossrefHarvester,
    # Add more harvesters here as they are implemented
}


def send_email(subject: str, body: str):
    """Send email notification."""
    smtp_host = os.getenv('SMTP_HOST', 'localhost')
    smtp_port = int(os.getenv('SMTP_PORT', '25'))
    from_addr = os.getenv('SMTP_FROM', 'library@institution.edu')
    to_addr = os.getenv('SMTP_TO', 'library@institution.edu')

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(msg)
        logger.info("email_sent", to=to_addr)
    except Exception as e:
        logger.error("email_failed", error=str(e))


def harvest_source(source_name: str, harvest_type: str) -> dict:
    """
    Harvest a single source.

    Args:
        source_name: Name of source to harvest
        harvest_type: Type of harvest

    Returns:
        Result dictionary with changedCount and summary
    """
    if source_name not in HARVESTERS:
        logger.error("unknown_source", source=source_name)
        return {
            'changedCount': 0,
            'summary': f"Error: Unknown source '{source_name}'"
        }

    logger.info("harvest_starting", source=source_name, type=harvest_type)
    start_time = time.time()

    try:
        # Instantiate harvester
        harvester_class = HARVESTERS[source_name]
        harvester = harvester_class()

        # Run harvest
        result = harvester.harvest(harvest_type)

        elapsed = time.time() - start_time
        logger.info(
            "harvest_completed",
            source=source_name,
            duration=elapsed,
            changed=result['changedCount']
        )

        result['summary'] = f"{result['summary']}\nHarvest time: {elapsed:.2f} seconds"

        return result

    except Exception as e:
        logger.error("harvest_failed", source=source_name, error=str(e))
        return {
            'changedCount': 0,
            'summary': f"Error harvesting {source_name}: {str(e)}"
        }


def main():
    """Main harvest orchestrator."""
    parser = argparse.ArgumentParser(description='IRTS Metadata Harvester')
    parser.add_argument(
        '--source',
        required=True,
        help='Comma-separated list of sources to harvest'
    )
    parser.add_argument(
        '--type',
        default='new',
        choices=['new', 'reprocess', 'reharvest', 'requery'],
        help='Type of harvest to perform (default: new)'
    )

    args = parser.parse_args()

    # Parse sources
    sources = [s.strip() for s in args.source.split(',')]
    harvest_type = args.type

    logger.info(
        "harvest_started",
        sources=sources,
        type=harvest_type,
        timestamp=datetime.now().isoformat()
    )

    # Harvest each source
    total_changed = 0
    new_in_process = 0
    harvest_summary_lines = [
        "=" * 60,
        "IRTS HARVEST REPORT",
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Harvest type: {harvest_type}",
        "=" * 60,
        ""
    ]

    for source in sources:
        print(f"\n{'='*60}")
        print(f"Harvesting: {source}")
        print(f"{'='*60}\n")

        result = harvest_source(source, harvest_type)

        total_changed += result['changedCount']
        harvest_summary_lines.append(result['summary'])
        harvest_summary_lines.append("")

        print(f"\n{result['summary']}\n")

    # Final summary
    harvest_summary_lines.extend([
        "=" * 60,
        "TOTALS",
        "=" * 60,
        f"New items needing review: {new_in_process}",
        f"Total changed records: {total_changed}",
        ""
    ])

    harvest_summary = "\n".join(harvest_summary_lines)

    # Print summary
    print(harvest_summary)

    # Send email if there were changes
    if total_changed > 0:
        send_email(
            "Results of Publications Harvest",
            harvest_summary
        )

    logger.info("harvest_complete", total_changed=total_changed)

    return 0


if __name__ == '__main__':
    sys.exit(main())
