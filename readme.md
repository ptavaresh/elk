# 🔍 Elasticsearch Log Extractor (PIT-based)

Este script permite extraer logs desde un índice de Elasticsearch utilizando el mecanismo **Point In Time (PIT)**, con soporte para filtros por nivel y rango de fechas, y guardado en archivos CSV por chunks de 10,000 registros.

## ✅ Características

- Conexión a Elasticsearch vía HTTP.
- Búsqueda eficiente usando PIT (ideal para grandes volúmenes).
- Filtros opcionales: nivel (`INFO`, `ERROR`, etc.) y fechas (`--start`, `--end`).
- Guardado en archivos CSV por bloques de 10,000 registros.
- Logging informativo y robusto.

---

## 🚀 Requisitos

- Python 3.8+
- Elasticsearch en ejecución
- Paquetes Python:

```bash
pip install elasticsearch
