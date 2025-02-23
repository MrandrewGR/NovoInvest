# servies/nsd_worker/app/table_extractor.py

import logging

logger = logging.getLogger(__name__)

class TableExtractor:
    """
    Stub for a future table_extractor that will consume
    a Kafka topic (e.g. 'nsd_processed_output') and attempt
    to extract data from the stored XML.
    """
    def process_message(self, message: dict):
        logger.info(f"[TableExtractor] Stub invoked with message: {message}")
        # Future logic goes here...
