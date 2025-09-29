import os
import psycopg
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
load_dotenv()
print("Ruta credenciales:", os.getenv("GOOGLE_SHEETS_CREDENTIALS"))


def main():
    # Conectar a Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("GOOGLE_SHEETS_CREDENTIALS"), scope)
    client = gspread.authorize(creds)

    # Abrir el Google Sheet
    sheet = client.open_by_key(os.getenv("GOOGLE_SHEET_ID")).sheet1
    data = sheet.get_all_records()

    # Conectar a PostgreSQL
    conn = psycopg.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=os.getenv("POSTGRES_PORT", 5432)
    )
    cur = conn.cursor()

    # Insertar cada fila en la tabla raw.servicios
    for row in data:
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

if __name__ == "__main__":
    main()
