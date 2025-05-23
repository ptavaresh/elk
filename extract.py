import sys
import os
import csv
import logging
import argparse
import yaml
from typing import List, Optional, Dict, Any
from datetime import datetime
from elasticsearch import Elasticsearch

# === Load YAML config ===
def load_config(env: str = "dev") -> Dict[str, Any]:
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    if env not in config:
        logging.error(f"Environment '{env}' not found in config.yml")
        sys.exit(1)
    return config[env]

# === Logging config ===
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_output_directory(base_dir: str, index: str) -> str:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(base_dir, f"{index}/run_{run_id}")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def validate_index_exists(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        logging.error(f"The index '{index}' does not exist.")
        sys.exit(1)

def open_point_in_time(es: Elasticsearch, index: str, keep_alive: str = "1m") -> str:
    pit_response = es.open_point_in_time(index=index, keep_alive=keep_alive)
    pit_id = pit_response.get("pit_id") or pit_response.get("id")
    if not pit_id:
        logging.error("Failed to retrieve the PIT ID.")
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

def save_logs_to_csv(logs: List[Dict[str, Any]], chunk_index: int, output_dir: str) -> None:
    output_file = os.path.join(output_dir, f"logs_chunk_{chunk_index}.csv")
    all_keys = set().union(*(log.keys() for log in logs))
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(all_keys))
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
    csv_output_base: str
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
            response = es.search(body=query)
            hits = response.get("hits", {}).get("hits", [])

            if not hits:
                break

            for hit in hits:
                current_chunk.append(hit["_source"])
                total_logs += 1
                if len(current_chunk) >= 10000:
                    save_logs_to_csv(current_chunk, chunk_counter, output_dir)
                    chunk_counter += 1
                    current_chunk.clear()

            search_after = hits[-1]["sort"]

        if current_chunk:
            save_logs_to_csv(current_chunk, chunk_counter, output_dir)

        logging.info(f"✅ Total logs extracted: {total_logs}")
    finally:
        es.close_point_in_time(body={"id": pit_id})

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

    es = Elasticsearch(config["elasticsearch_url"])
    index = args.index or config["default_index"]
    batch_size = args.batch or config["batch_size"]
    csv_output_base = config["csv_output_base"]

    fetch_logs(
        es=es,
        index=index,
        level=args.level,
        start_date=args.start,
        end_date=args.end,
        batch_size=batch_size,
        csv_output_base=csv_output_base
    )
