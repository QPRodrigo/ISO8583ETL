from pathlib import Path
import random
import time
from datetime import datetime

LAT_MIN = -12.10
LAT_MAX = -11.95
LON_MIN = -77.15
LON_MAX = -76.95

# Datos simulados
response_codes = ["00", "05", "12", "91", "96", "80", "48"]  # ISO8583 response codes
institutions = ["BCP", "BBVA", "Interbank", "Scotiabank"]
brands = ["Visa", "MasterCard", "Amex", "Diners"]
service_types = ["Tarjeta de Credito", "Tarjeta de Debito", "Tarjeta Prepago"]
currencies = ["USD", "EUR", "PEN"]
transaction_types = ["Compra", "Retiro de Efectivo", "Consulta de Saldo"]

# Función para generar una trama tipo ISO8583
def generar_trama_iso8583():
    return (
        f"{datetime.utcnow().isoformat()},"
        f"txn_{random.randint(100000000, 999999999)},"
        f"0200,"  # Message Type ID para solicitud financiera
        f"{random.choice(response_codes)},"
        f"{random.choice(service_types)},"
        f"{random.choice(institutions)},"
        f"{random.choice(brands)},"
        f"{random.choice(transaction_types)},"
        f"user_{random.randint(1000, 9999)},"
        f"{random.choice(currencies)},"
        f"{round(random.uniform(5.0, 1000.0), 2)},"
        f"{random.randint(50, 500)},"
        f"{round(random.uniform(LAT_MIN, LAT_MAX), 6)},"
        f"{round(random.uniform(LON_MIN, LON_MAX), 6)}"
    )

# Ruta de log
log_path = Path(__file__).resolve().parent / "data" / "transactions.csv"
log_path.parent.mkdir(parents=True, exist_ok=True)

print(f"📂 Guardando logs ISO8583 en: {log_path}")

header = "timestamp,transaction_id,mtid,response_code,service_types,institution_id,brand,transaction_type,user_id,currency,amount,response_time_ms,lat,lon"
# Escritura continua
try:
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(header+ "\n")
        for _ in range(1000):
            linea = generar_trama_iso8583()
            f.write(linea + "\n")
            f.flush()
            print("📝 Trama generada:", linea)
            #time.sleep(4)  # Pausa entre tramas
except Exception as e:
    print("❌ Error al escribir en el archivo:", e)
    