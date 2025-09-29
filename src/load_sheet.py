import os
import psycopg
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv, find_dotenv

# Cargar variables del .env
load_dotenv(find_dotenv())

def main():
    # Conectar a Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_path:
        raise ValueError("⚠️ No se encontró GOOGLE_SHEETS_CREDENTIALS en el .env")
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    # Abrir el Google Sheet
    sheet_id = os.getenv("GSHEET_URL").split("/d/")[1].split("/")[0]
    worksheet = os.getenv("GSHEET_WORKSHEET")
    sheet = client.open_by_key(sheet_id).worksheet(worksheet)
    data = sheet.get_all_records()

    # Conectar a PostgreSQL
    conn = psycopg.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432)
    )
    cur = conn.cursor()

    # Insertar cada fila en la tabla raw.servicios
    for row in data:
        # Limpiar MONTO (quitar $, comas y espacios, convertir a float)
        monto_str = str(row["MONTO"])
        monto_clean = None
        if monto_str.strip() != "":
            monto_clean = float(monto_str.replace("$", "").replace(",", "").strip())

        cur.execute(
            """
                INSERT INTO raw.servicios (
                    fecha, folio, cliente, vehiculo, mecanico,
                    movimiento, concepto, descripcion, monto
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["FECHA"],
                row["FOLIO"],
                row["CLIENTE"],
                row["VEHICULO"],
                row["MECANICO"],
                row["MOVIMIENTO"],
                row["CONCEPTO"],
                row["DESCRIPCION"],
                row["MONTO"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Datos cargados correctamente en raw.servicios")

if __name__ == "__main__":
    main()
