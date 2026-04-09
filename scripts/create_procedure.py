import mysql.connector
import re

# Get procedure body from source
src = mysql.connector.connect(
    host='srv1978.hstgr.io', port=3306,
    user='u658050396_QEB', password='QEBdevelop.1',
    database='u658050396_QEB', charset='utf8mb4'
)
cursor = src.cursor()
cursor.execute('SHOW CREATE PROCEDURE actualizar_reservas')
row = cursor.fetchone()
body = row[2]
cursor.close()
src.close()

# Remove DEFINER clause
clean_body = re.sub(r'DEFINER=`[^`]+`@`[^`]+`\s*', '', body)
print('Cleaned procedure body (first 300 chars):')
print(clean_body[:300])

# Connect to dest
dst = mysql.connector.connect(
    host='82.197.82.225', port=3306,
    user='u658050396_QEB_PRUEBAS', password='/uQ3FCrLG5:6',
    database='u658050396_QEB_PRUEBAS', charset='utf8mb4'
)
dcursor = dst.cursor()
dcursor.execute('DROP PROCEDURE IF EXISTS actualizar_reservas')
dcursor.execute(clean_body)
dst.commit()
dcursor.close()
dst.close()
print('\nStored procedure created successfully!')
