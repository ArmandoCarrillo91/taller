import psycopg

conn = psycopg.connect(
    dbname="taller",
    user="admin",
    password="codigo",
    host="localhost",
    port=5432
)

print("✅ Conexión exitosa:", conn.info.dbname)
conn.close()
