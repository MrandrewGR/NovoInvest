import asyncio
import heapq
import json
import logging
import psycopg2
from collections import deque
from mirco_services_data_management.base_worker import BaseWorker
from mirco_services_data_management.db import (
    get_connection,
    ensure_partitioned_parent_table,
    insert_partitioned_record,
    ensure_database_exists
)
# NEW: import create_producer so we can manually create our producer
from mirco_services_data_management.kafka_io import create_producer, send_message

from .config import NsdWorkerConfig
from .utils import random_delay, fetch_content, parse_html

logger = logging.getLogger(__name__)


class NsdWorker(BaseWorker):
    """
    nsd_worker microservice:
    - Inherits the async BaseWorker from mirco_services_data_management.
    - Consumes messages from Kafka with name_uname=@nsdfeed_ru
      or falls back to DB if no Kafka messages are found.
    - Extracts URLs, fetches and parses the content, stores in partitioned table.
    - Publishes processed results to a Kafka topic.
    """

    def __init__(self, config: NsdWorkerConfig):
        super().__init__(config)
        self.kafka_queue = []
        self.db_queue = deque()
        self.last_kafka_message_time = None
        self.db_polling_enabled = True
        self.dbname_var = self.config.TG_UBOT_DB_NAME
        self.producer = None  # We will create/assign this in start()

        # Ensure the database exists before trying to connect
        try:
            ensure_database_exists("NSD_WORKER_DB_NAME")  # Ensure correct DB
        except Exception as e:
            logger.warning(f"Could not ensure database existence: {e}")

        # Ensure partitioned table is created and "processed" column exists
        try:
            logger.info(f"Ensuring partitioned parent table {self.config.NSD_PARENT_TABLE} ...")
            ensure_partitioned_parent_table(
                parent_table=self.config.NSD_PARENT_TABLE,
                unique_index_fields=["(data->>'message_id')", "month_part"]
            )
            self._ensure_processed_column()
        except Exception as e:
            logger.warning(f"Could not ensure partitioned parent table: {e}")

    def _ensure_processed_column(self):
        """ Ensures the 'processed' column exists in TG_UBOT_NSD_TABLE. """
        conn = None
        try:
            conn = get_connection("TG_UBOT_DB_NAME")
            with conn.cursor() as cur:
                table = self.config.TG_UBOT_NSD_TABLE
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}' AND column_name = 'processed';
                """)
                result = cur.fetchone()
                if not result:
                    logger.info(f"Adding 'processed' column to {table}...")
                    cur.execute(f"""
                        ALTER TABLE {table}
                        ADD COLUMN processed BOOLEAN DEFAULT NULL;
                    """)
                    conn.commit()
                    logger.info("'processed' column added successfully.")
        except Exception as e:
            logger.error(f"Error ensuring 'processed' column: {e}")
        finally:
            if conn:
                conn.close()

    async def start(self):
        """
        Creates and starts our Kafka producer (manually) and then begins
        handling both Kafka and DB fallback messages in parallel.
        """
        # 1) Create/start the producer here (instead of parent.start())
        self.producer = await create_producer(
            bootstrap_servers=self.config.KAFKA_BROKER,
            transactional=self.config.TRANSACTIONAL,
            transactional_id=self.config.TRANSACTIONAL_ID
        )
        await self.producer.start()
        logger.info("NsdWorker's Kafka producer started successfully.")

        # 2) Start processing "internal" queues
        asyncio.create_task(self.process_messages())

        # 3) Continuously poll DB for unprocessed data (fallback)
        while True:
            await self.check_db_for_unprocessed()
            await asyncio.sleep(self.config.POLL_INTERVAL_DB)

    def stop(self):
        """
        Signals the worker to stop. You can call this on KeyboardInterrupt.
        """
        # Just set parent's stop_event so we can break loops if we want
        self._stop_event.set()

    async def shutdown(self):
        """
        Clean up resources (close producer, etc.) after stop is signaled.
        """
        logger.info("NsdWorker shutting down...")
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer closed.")

    async def handle_message(self, message: dict):
        """
        Overriding the base class method for custom logic.
        Only process messages that have name_uname == '@nsdfeed_ru'.
        """
        self.last_kafka_message_time = asyncio.get_event_loop().time()

        name_uname = message.get("name_uname")
        if name_uname == "@nsdfeed_ru":
            # Call parent's handle_message for "processed_messages" logic
            await super().handle_message(message)
        else:
            logger.debug(f"Ignoring message with name_uname={name_uname}")

    async def check_db_for_unprocessed(self):
        """
        Looks for rows in the DB that haven't been processed yet (processed=NULL),
        pushes them into self.db_queue, and then we handle them in process_messages().
        """
        # If we haven't received any Kafka message, or if it's been a while
        if self.db_polling_enabled and self.last_kafka_message_time is None:
            logger.info("No Kafka messages received yet; checking DB for fallback.")
            await self._fetch_and_process_db_rows()
        else:
            now = asyncio.get_event_loop().time()
            if self.last_kafka_message_time is not None:
                time_since_last_msg = now - self.last_kafka_message_time
            else:
                time_since_last_msg = 999999

            if time_since_last_msg > (self.config.POLL_INTERVAL_DB * 2):
                logger.info("It's been a while since last Kafka message; checking DB fallback.")
                await self._fetch_and_process_db_rows()
            else:
                logger.debug("Kafka messages are recent enough; skipping DB fallback.")

    async def _fetch_and_process_db_rows(self):
        """
        Fetch up to 5 unprocessed rows from the DB (where name_uname=@nsdfeed_ru),
        push them onto self.db_queue for the main process_messages() loop.
        """
        conn = None
        try:
            conn = get_connection("TG_UBOT_DB_NAME")
            with conn.cursor() as cur:
                table = self.config.TG_UBOT_NSD_TABLE
                query = f"""
                    SELECT id, data
                    FROM {table}
                    WHERE (data->>'name_uname') = '@nsdfeed_ru'
                      AND (data->>'processed') IS NULL
                    ORDER BY id ASC
                    LIMIT 5;
                """
                cur.execute(query)
                rows = cur.fetchall()
                if rows:
                    logger.info(f"Fetched {len(rows)} rows from DB for processing.")
                    for row_id, data_json in rows:
                        # Ensure data_json is a dict (if stored as JSONB)
                        if isinstance(data_json, dict):
                            message_dict = data_json
                        else:
                            message_dict = json.loads(data_json)

                        message_dict["message_id"] = row_id
                        self.db_queue.append(message_dict)
                else:
                    logger.debug("No unprocessed messages found in DB.")
        except Exception as e:
            logger.error(f"Error fetching from DB: {e}")
        finally:
            if conn:
                conn.close()

    async def process_messages(self):
        """
        Main loop that processes messages from self.kafka_queue or self.db_queue.
        (In your snippet, kafka_queue is not actually populated, but you could adapt it.)
        """
        while True:
            # If you have code somewhere that puts messages into self.kafka_queue, you'd check it here:
            if self.kafka_queue:
                message = heapq.heappop(self.kafka_queue)
                await self.process_message(message)
            elif self.db_queue:
                message = self.db_queue.popleft()
                await self.process_message(message)
            else:
                await asyncio.sleep(0.5)

    async def process_message(self, message: dict):
        """
        Perform the actual "logic" on each message:
         1) For each link, fetch and parse.
         2) Insert into partitioned table.
         3) Send a final "processed" message to Kafka.
        """
        logger.info(f"[nsd_worker] Processing message: {message}")
        links = message.get("links", [])
        link_results = []

        for i, link_info in enumerate(links):
            url = link_info.get("url")
            if not url:
                continue

            # random delay between requests
            if i > 0:
                await random_delay(self.config.DELAY_MIN, self.config.DELAY_MAX)

            content = await fetch_content(url)
            # Parse HTML (returns a BeautifulSoup object)
            parsed_html = parse_html(content)

            # FIX #1: Convert BeautifulSoup to string
            link_results.append({
                "url": url,
                "parsed_html": str(parsed_html)  # prevents JSON serialization errors
            })

        store_data = {
            "message": message,
            "link_results": link_results
        }

        # Insert record into the partitioned parent table
        inserted = insert_partitioned_record(
            parent_table=self.config.NSD_PARENT_TABLE,
            data_dict=store_data,
            deduplicate=True
        )

        if inserted:
            logger.info(f"Inserted new record into {self.config.NSD_PARENT_TABLE} partition.")
        else:
            logger.info("Record already exists or was not inserted (duplicate).")

        # Now send a "processed" message out to Kafka
        out_message = {
            "original_message_id": message.get("message_id"),
            "processed_links_count": len(link_results),
            "timestamp": message.get("date"),
            "status": "processed"
        }

        # FIX #2: self.producer is no longer None because we started it in start()
        await send_message(self.producer, self.config.KAFKA_PRODUCE_TOPIC, out_message)
        logger.info(f"Published processed message to {self.config.KAFKA_PRODUCE_TOPIC}.")
