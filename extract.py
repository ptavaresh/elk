import sys
import os
import csv
import yaml
import logging
import argparse
from typing import List, Optional, Dict, Any
from datetime import datetime
from elasticsearch import Elasticsearch

# === Constants ===
DEFAULT_INDEX = "logs-multiples"
DEFAULT_BATCH_SIZE = 1000
CSV_CHUNK_SIZE = 10000
DEFAULT_ENV = "dev"
CONFIG_PATH = "config.yml"

# === Logging config ===
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# === Config loader ===
def load_config(env_name: str, config_path: str = CONFIG_PATH) -> Dict[str, Any]:
    """Load configuration from YAML based on environment."""
    if not os.path.exists(config_path):
        logging.error(f"❌ Config file '{config_path}' not found.")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if env_name not in config:
        logging.error(f"❌ Environment '{env_name}' not defined in config file.")
        sys.exit(1)

    logging.info(f"✅ Loaded environment: {env_name}")
    return config[env_name]

# === Elasticsearch logic ===
def validate_index_exists(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        logging.error(f"The index '{index}' does not exist.")
        sys.exit(1)

def open_point_in_time(es: Elasticsearch, index: str, keep_alive: str = "1m") -> str:
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

def save_logs_to_csv(logs: List[Dict[str, Any]], chunk_index: int, output_path: str) -> None:
    if not logs:
        return

    os.makedirs(output_path, exist_ok=True)
    output_file = f"{output_path}/logs_chunk_{chunk_index}.csv"
    fieldnames = sorted({key for log in logs for key in log.keys()})

    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for log in logs:
            writer.writerow(log)

    logging.info(f"✅ Saved {len(logs)} logs to: {output_file}")

def fetch_logs(
    es: Elasticsearch,
    index: str,
    level: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int,
    output_path: str
) -> None:
    validate_index_exists(es, index)
    pit_id = open_point_in_time(es, index)

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
                    save_logs_to_csv(current_chunk, chunk_counter, output_path)
                    chunk_counter += 1
                    current_chunk.clear()

            search_after = hits[-1]["sort"]

        if current_chunk:
            save_logs_to_csv(current_chunk, chunk_counter, output_path)

        logging.info(f"✅ Total logs extracted: {total_logs}")

    finally:
        es.close_point_in_time(body={"id": pit_id})

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract logs from Elasticsearch using PIT and save as CSV.")
    parser.add_argument("--index", default=DEFAULT_INDEX, help="Elasticsearch index name")
    parser.add_argument("--level", help="Filter by log level (INFO, ERROR, etc.)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size per query")
    parser.add_argument("--env", default=DEFAULT_ENV, help="Environment to use (dev, test, prod)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    config = load_config(args.env)
    es = Elasticsearch(config["elasticsearch_url"])

    fetch_logs(
        es=es,
        index=args.index,
        level=args.level,
        start_date=args.start,
        end_date=args.end,
        batch_size=args.batch,
        output_path=config["csv_output_path"]
    )
