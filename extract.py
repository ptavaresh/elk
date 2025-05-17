import os
import csv
import sys
import logging
from typing import List, Optional
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError
import argparse

# === Configuración y constantes ===
CHUNK_SIZE = 10000
OUTPUT_DIR = "logs_extraidos"
ELASTICSEARCH_URL = "http://localhost:9200"
PIT_KEEP_ALIVE = "1m"

# === Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === Cliente de Elasticsearch ===
es = Elasticsearch(ELASTICSEARCH_URL)

# === Funciones auxiliares ===
def construir_query(pit_id: str, nivel: Optional[str], fecha_inicio: Optional[str], fecha_fin: Optional[str], search_after: Optional[list], batch_size: int) -> dict:
    filtro = []

    if nivel:
        filtro.append({"match": {"level": nivel}})

    if fecha_inicio or fecha_fin:
        rango = {}
        if fecha_inicio:
            rango["gte"] = fecha_inicio
        if fecha_fin:
            rango["lte"] = fecha_fin
        filtro.append({"range": {"timestamp": rango}})

    query = {
        "size": batch_size,
        "sort": [{"timestamp": "asc"}],
        "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
        "query": {
            "bool": {
                "must": filtro if filtro else {"match_all": {}}
            }
        }
    }

    if search_after:
        query["search_after"] = search_after

    return query

def guardar_chunk_csv(logs: List[List[str]], chunk_idx: int, output_dir: str = OUTPUT_DIR) -> None:
    os.makedirs(output_dir, exist_ok=True)
    archivo = os.path.join(output_dir, f"logs_chunk_{chunk_idx}.csv")

    with open(archivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "level", "msg"])
        writer.writerows(logs)

    logger.info(f"Chunk {chunk_idx} guardado con {len(logs)} registros en '{archivo}'")

# === Función principal ===
def obtener_logs_pit(index: str, nivel: Optional[str] = None, fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None, batch_size: int = 1000) -> None:
    try:
        if not es.indices.exists(index=index):
            logger.error(f"El índice '{index}' no existe.")
            sys.exit(1)

        pit_response = es.open_point_in_time(index=index, keep_alive=PIT_KEEP_ALIVE)
        pit_id = pit_response.get("pit_id") or pit_response.get("id")

        if not pit_id:
            logger.error("No se pudo obtener el PIT. Respuesta: %s", pit_response)
            sys.exit(1)

        total_docs = 0
        search_after = None
        logs = []
        chunk_idx = 1

        while True:
            query = construir_query(pit_id, nivel, fecha_inicio, fecha_fin, search_after, batch_size)
            resp = es.search(body=query)
            hits = resp["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                logs.append([source.get("timestamp"), source.get("level"), source.get("msg")])
                total_docs += 1

                if len(logs) >= CHUNK_SIZE:
                    guardar_chunk_csv(logs, chunk_idx)
                    logs = []
                    chunk_idx += 1

            search_after = hits[-1]["sort"]

        if logs:
            guardar_chunk_csv(logs, chunk_idx)

        logger.info(f"\n=== Total logs extraídos: {total_docs} ===")

    except (ConnectionError, NotFoundError) as e:
        logger.error(f"Error de conexión con Elasticsearch: {e}")
        sys.exit(1)

    finally:
        try:
            es.close_point_in_time(body={"id": pit_id})
        except Exception as e:
            logger.warning(f"No se pudo cerrar el PIT: {e}")

# === CLI ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--indice", default="logs-multiples")
    parser.add_argument("--nivel", help="Filtrar por nivel (INFO, ERROR...)")
    parser.add_argument("--inicio", help="Fecha inicio (YYYY-MM-DD o ISO 8601)")
    parser.add_argument("--fin", help="Fecha fin (YYYY-MM-DD o ISO 8601)")
    parser.add_argument("--batch", type=int, default=1000)

    args = parser.parse_args()

    obtener_logs_pit(
        index=args.indice,
        nivel=args.nivel,
        fecha_inicio=args.inicio,
        fecha_fin=args.fin,
        batch_size=args.batch
    )
