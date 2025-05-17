import sys
import csv
import logging
import argparse
from typing import List, Optional, Dict, Any
from datetime import datetime
from elasticsearch import Elasticsearch

# === Constants ===
DEFAULT_INDEX = "logs-multiples"
DEFAULT_BATCH_SIZE = 1000
CSV_CHUNK_SIZE = 10000
ELASTICSEARCH_URL = "http://localhost:9200"
CSV_OUTPUT_PATH = "extracted_logs"

# === Logging config ===
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# === Elasticsearch client ===
es = Elasticsearch(ELASTICSEARCH_URL)

def validate_index_exists(index: str) -> None:
    """Check if the given index exists in Elasticsearch."""
    if not es.indices.exists(index=index):
        logging.error(f"The index '{index}' does not exist.")
        sys.exit(1)

def open_point_in_time(index: str, keep_alive: str = "1m") -> str:
    """Open a Point In Time (PIT) context and return its ID."""
    pit_response = es.open_point_in_time(index=index, keep_alive=keep_alive)
    pit_id = pit_response.get("pit_id") or pit_response.get("id")
    if not pit_id:
        logging.error("Failed to retrieve the PIT ID.")
        logging.debug(f"Raw PIT response: {pit_response}")
        sys.exit(1)
    return pit_id

def build_query(
    pit_id: str,
    search_after: Optional[List[Any]],
    level: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int
) -> Dict[str, Any]:
    """Build the query for fetching logs."""
    filters = []

    if level:
        filters.append({"match": {"level": level}})

    if start_date or end_date:
        date_range = {}
        if start_date:
            date_range["gte"] = start_date
        if end_date:
            date_range["lte"] = end_date
        filters.append({"range": {"timestamp": date_range}})

    query_body = {
        "size": batch_size,
        "sort": [{"timestamp": "asc"}],
        "pit": {"id": pit_id, "keep_alive": "1m"},
        "query": {
            "bool": {
                "must": filters if filters else {"match_all": {}}
            }
        }
    }

    if search_after:
        query_body["search_after"] = search_after

    return query_body

def save_logs_to_csv(logs: List[Dict[str, Any]], chunk_index: int) -> None:
    """Save logs to a CSV file with chunking."""
    output_file = f"{CSV_OUTPUT_PATH}/logs_chunk_{chunk_index}.csv"
    headers = ["timestamp", "level", "msg"]

    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for log in logs:
            writer.writerow({
                "timestamp": log.get("timestamp"),
                "level": log.get("level"),
                "msg": log.get("msg")
            })

    logging.info(f"✅ Saved {len(logs)} logs to: {output_file}")

def fetch_logs(
    index: str,
    level: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int
) -> None:
    """Main function to fetch logs using PIT and save to CSV chunks."""
    validate_index_exists(index)
    pit_id = open_point_in_time(index)

    search_after = None
    total_logs = 0
    current_chunk = []
    chunk_counter = 1

    try:
        while True:
            query = build_query(pit_id, search_after, level, start_date, end_date, batch_size)
            response = es.search(body=query)
            hits = response.get("hits", {}).get("hits", [])

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                current_chunk.append(source)
                total_logs += 1

                if len(current_chunk) >= CSV_CHUNK_SIZE:
                    save_logs_to_csv(current_chunk, chunk_counter)
                    chunk_counter += 1
                    current_chunk.clear()

            search_after = hits[-1]["sort"]

        # Save any remaining logs in the last chunk
        if current_chunk:
            save_logs_to_csv(current_chunk, chunk_counter)

        logging.info(f"✅ Total logs extracted: {total_logs}")

    finally:
        es.close_point_in_time(body={"id": pit_id})

def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Extract logs from Elasticsearch using PIT and save as CSV.")
    parser.add_argument("--index", default=DEFAULT_INDEX, help="Elasticsearch index name")
    parser.add_argument("--level", help="Filter by log level (INFO, ERROR, etc.)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size per query")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    fetch_logs(
        index=args.index,
        level=args.level,
        start_date=args.start,
        end_date=args.end,
        batch_size=args.batch
    )
