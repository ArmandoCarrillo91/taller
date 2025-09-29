import os
import psycopg
import gspread
from dotenv import load_dotenv, find_dotenv
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv(find_dotenv())

DB = dict(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST", "localhost"),  # si corres Python fuera de Docker
    port=os.getenv("POSTGRES_PORT", 5432),
)

CREDS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
DEST_URL = os.getenv("GSHEET_URL_DEST")
DEST_TAB = os.getenv("GSHEET_WORKSHEET_DEST", "core_servicios")

def gsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, scopes)
    return gspread.authorize(creds)

def open_dest(gc):
    sheet_id = DEST_URL.split("/d/")[1].split("/")[0]
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(DEST_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=DEST_TAB, rows=1000, cols=20)
    return ws

def fetch_core():
    q = """
      SELECT 
        folio,
        cliente,
        vehiculo,
        mecanico,
        to_char(fecha_inicio, 'YYYY-MM-DD') AS fecha_inicio,
        to_char(fecha_fin,    'YYYY-MM-DD') AS fecha_fin,
        COALESCE(venta_refacciones,0)::text      AS venta_refacciones,
        COALESCE(costo_refacciones,0)::text      AS costo_refacciones,
        COALESCE(utilidad_refacciones,0)::text   AS utilidad_refacciones,
        COALESCE(venta_mano_obra,0)::text        AS venta_mano_obra,
        COALESCE(pago_mecanico,0)::text          AS pago_mecanico,
        COALESCE(utilidad_mano_obra,0)::text     AS utilidad_mano_obra,
        COALESCE(total_venta,0)::text            AS total_venta,
        COALESCE(total_costos,0)::text           AS total_costos,
        COALESCE(utilidad_total,0)::text         AS utilidad_total,
        (COALESCE("%utilidad",0))::text          AS "%utilidad",
        COALESCE(piezas_ref_sin_compra,0)::text  AS piezas_ref_sin_compra,
        COALESCE(piezas_ref_sin_venta,0)::text   AS piezas_ref_sin_venta
      FROM mart.servicios_summary
      ;
    """
    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()
    # a texto para Sheets
    values = [cols] + [[("" if v is None else str(v)) for v in r] for r in rows]
    return values

def main():
    if not CREDS_PATH or not DEST_URL:
        raise SystemExit("Faltan GOOGLE_SHEETS_CREDENTIALS o GSHEET_URL_DEST en .env")
    data = fetch_core()
    gc = gsheet()
    ws = open_dest(gc)
    ws.clear()
    ws.update("A1", data, value_input_option="USER_ENTERED")
    print(f"Exportadas {len(data)-1} filas a '{DEST_TAB}'")

if __name__ == "__main__":
    main()
