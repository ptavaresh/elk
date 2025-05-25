import sys
import os
import csv
import logging
import argparse
import yaml
from typing import List, Optional, Dict, Any
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, AuthenticationException, TransportError

# === Load YAML config ===
def load_config(env: str = "dev") -> Dict[str, Any]:
    try:
        with open("config.yml", "r") as f:
            config = yaml.safe_load(f)
        if env not in config:
            logging.error(f"Environment '{env}' not found in config.yml")
            sys.exit(1)
        return config[env]
    except FileNotFoundError:
        logging.error("config.yml file not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config.yml: {e}")
        sys.exit(1)

# === Logging config ===
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_output_directory(base_dir: str, index: str) -> str:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(base_dir, f"{index}/run_{run_id}")
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"❌ Error creating output directory '{output_dir}': {e}")
        sys.exit(1)
    return output_dir

def validate_index_exists(es: Elasticsearch, index: str) -> None:
    try:
        if not es.indices.exists(index=index):
            logging.error(f"The index '{index}' does not exist.")
            sys.exit(1)
    except TransportError as e:
        logging.error(f"Transport error checking index: {e}")
        sys.exit(1)

def open_point_in_time(es: Elasticsearch, index: str, keep_alive: str = "1m") -> str:
    try:
        pit_response = es.open_point_in_time(index=index, keep_alive=keep_alive)
        pit_id = pit_response.get("pit_id") or pit_response.get("id")
        if not pit_id:
            logging.error("Failed to retrieve the PIT ID.")
            sys.exit(1)
        return pit_id
    except TransportError as e:
        logging.error(f"Error opening PIT: {e}")
        sys.exit(1)

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

def save_logs_to_csv(logs: List[Dict[str, Any]], chunk_index: int, output_dir: str) -> None:
    output_file = os.path.join(output_dir, f"logs_chunk_{chunk_index}.csv")
    all_keys = set().union(*(log.keys() for log in logs))

    try:
        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(all_keys))
            writer.writeheader()
            for log in logs:
                writer.writerow(log)
        logging.info(f"✅ Saved {len(logs)} logs to: {output_file}")
    except OSError as e:
        logging.error(f"❌ Error writing to file '{output_file}': {e}")
    except Exception as e:
        logging.error(f"❌ Unexpected error while saving logs to CSV: {e}")

def fetch_logs(
    es: Elasticsearch,
    index: str,
    level: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int,
    csv_output_base: str,
    fields: Optional[List[str]] = None,
    max_logs_per_chunk: int = 1000  # Nuevo parámetro con valor por defecto
) -> None:
    validate_index_exists(es, index)
    pit_id = open_point_in_time(es, index)
    output_dir = get_output_directory(csv_output_base, index)

    search_after = None
    total_logs = 0
    current_chunk = []
    chunk_counter = 1

    try:
        while True:
            query = build_query(pit_id, search_after, level, start_date, end_date, batch_size)
            if fields:
                query["_source"] = fields
            response = es.search(body=query)
            hits = response.get("hits", {}).get("hits", [])

            if not hits:
                break

            for hit in hits:
                log = hit["_source"]
                if fields:
                    log = {k: log.get(k, None) for k in fields}
                current_chunk.append(log)
                total_logs += 1
                if len(current_chunk) >= max_logs_per_chunk:
                    save_logs_to_csv(current_chunk, chunk_counter, output_dir)
                    chunk_counter += 1
                    current_chunk.clear()

            search_after = hits[-1]["sort"]

        if current_chunk:
            save_logs_to_csv(current_chunk, chunk_counter, output_dir)

        logging.info(f"✅ Total logs extracted: {total_logs}")
    finally:
        close_point_in_time_with_retries(es, pit_id)

def close_point_in_time_with_retries(es: Elasticsearch, pit_id: str, retries: int = 3):
    for attempt in range(1, retries + 1):
        try:
            es.close_point_in_time(body={"id": pit_id})
            logging.info("✅ PIT closed successfully.")
            return
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt} to close PIT failed: {e}")
    logging.error("❌ Could not close PIT after several attempts. Manual cleanup may be required.")

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract logs from Elasticsearch using PIT and save as CSV.")
    parser.add_argument("--env", default="dev", help="Environment (dev, test, prod)")
    parser.add_argument("--index", help="Override index name from config")
    parser.add_argument("--level", help="Filter by log level (INFO, ERROR, etc.)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--batch", type=int, help="Override batch size")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    config = load_config(args.env)

    try:
        es = Elasticsearch(config["elasticsearch_url"])
    except (ConnectionError, AuthenticationException) as e:
        logging.error(f"❌ Error connecting to Elasticsearch: {e}")
        sys.exit(1)

    index = args.index or config["default_index"]
    batch_size = args.batch or config["batch_size"]
    csv_output_base = config["csv_output_base"]
    max_logs_per_chunk = config.get("max_logs_per_chunk", 1000)

    fetch_logs(
        es=es,
        index=index,
        level=args.level,
        start_date=args.start,
        end_date=args.end,
        batch_size=batch_size,
        csv_output_base=csv_output_base,
        fields=["timestamp", "level", "message"],
        max_logs_per_chunk=max_logs_per_chunk  # <-- ahora configurable
    )
