import csv
import random
from faker import Faker
import argparse
import os
from math import ceil
from tqdm import tqdm
import time

fake = Faker()

# Columnas base
COLUMNAS = [
    "id", "nombre", "apellido", "email", "telefono", "direccion",
    "ciudad", "estado", "codigo_postal", "pais", "empresa", "puesto",
    "fecha_registro", "fecha_ultima_compra", "producto_favorito",
    "monto_total_compras", "frecuencia_compra", "metodo_pago",
    "activo", "notas", "fecha_nacimiento", "genero", "nivel_ingresos",
    "nivel_satisfaccion", "cliente_recurrente", "nps", "descuento",
    "tipo_cliente", "referido_por", "region"
]

def generar_fila():
    return {
        "id": fake.uuid4(),
        "nombre": fake.first_name(),
        "apellido": fake.last_name(),
        "email": fake.email(),
        "telefono": fake.phone_number(),
        "direccion": fake.street_address(),
        "ciudad": fake.city(),
        "estado": fake.state(),
        "codigo_postal": fake.postcode(),
        "pais": fake.country(),
        "empresa": fake.company(),
        "puesto": fake.job(),
        "fecha_registro": fake.date_between(start_date='-2y', end_date='today'),
        "fecha_ultima_compra": fake.date_between(start_date='-1y', end_date='today'),
        "producto_favorito": fake.word(),
        "monto_total_compras": round(random.uniform(100, 10000), 2),
        "frecuencia_compra": random.choice(["Alta", "Media", "Baja"]),
        "metodo_pago": random.choice(["Tarjeta", "Transferencia", "Efectivo", "PayPal"]),
        "activo": random.choice([True, False]),
        "notas": fake.sentence(),
        "fecha_nacimiento": fake.date_of_birth(minimum_age=18, maximum_age=70),
        "genero": random.choice(["M", "F", "Otro"]),
        "nivel_ingresos": random.choice(["Bajo", "Medio", "Alto"]),
        "nivel_satisfaccion": random.randint(1, 10),
        "cliente_recurrente": random.choice([True, False]),
        "nps": random.randint(-100, 100),
        "descuento": f"{random.randint(0, 50)}%",
        "tipo_cliente": random.choice(["Nuevo", "Leal", "VIP", "Potencial"]),
        "referido_por": fake.name(),
        "region": random.choice(["Norte", "Sur", "Este", "Oeste", "Internacional"])
    }

def generar_csv_chunks(nombre_base, total_filas, filas_por_chunk):
    num_chunks = ceil(total_filas / filas_por_chunk)
    contador_total = 0

    start_time = time.time()

    for chunk in range(num_chunks):
        nombre_archivo = f"{nombre_base}_parte_{chunk+1}.csv"
        filas_en_este_chunk = min(filas_por_chunk, total_filas - contador_total)

        with open(nombre_archivo, mode='w', newline='', encoding='utf-8') as archivo:
            writer = csv.DictWriter(archivo, fieldnames=COLUMNAS)
            writer.writeheader()

            for _ in tqdm(range(filas_en_este_chunk), desc=f"Generando {nombre_archivo}", unit="fila"):
                writer.writerow(generar_fila())
                contador_total += 1

        print(f"✅ {nombre_archivo} generado con {filas_en_este_chunk} filas.")

    duracion = time.time() - start_time
    print(f"\n🎉 ¡Listo! {total_filas} filas generadas en {duracion:.2f} segundos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de CSV falso con chunks y progreso")
    parser.add_argument("nombre_base", help="Nombre base de los archivos (ej: datos)")
    parser.add_argument("total_filas", type=int, help="Cantidad total de filas a generar")
    parser.add_argument("--chunk", type=int, default=10000, help="Filas por archivo (default: 10000)")
    args = parser.parse_args()

    generar_csv_chunks(args.nombre_base, args.total_filas, args.chunk)
