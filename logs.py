import random
from datetime import datetime, timedelta

# Config
output_file = "logs/mi_app.log"
num_logs = 10000
niveles = ["INFO", "WARN", "ERROR", "DEBUG"]
mensajes = [
    "Usuario inició sesión",
    "Fallo en la autenticación",
    "Conexión con base de datos exitosa",
    "Error al cargar datos del usuario",
    "Token JWT expirado",
    "Se actualizó la configuración",
    "Intento de acceso no autorizado",
    "Carga de módulo completada",
    "Archivo no encontrado",
    "Parámetros inválidos en la solicitud"
]

# Crear logs
inicio = datetime(2025, 4, 25, 0, 0, 0)

with open(output_file, "w") as f:
    for i in range(num_logs):
        timestamp = inicio + timedelta(seconds=i * random.randint(1, 3))
        nivel = random.choice(niveles)
        mensaje = random.choice(mensajes)
        log_line = f"{timestamp.isoformat()}Z {nivel} {mensaje}\n"
        f.write(log_line)

print(f"Generado archivo con {num_logs} líneas en {output_file}")
