import oracledb
import sys
# cx_Oracle 호환 패치
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
import cx_Oracle
from config import SOURCE_DB_CONFIG

def add_data():
    try:
        conn = cx_Oracle.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO TB_USER (USER_ID, USER_NAME, AGE) VALUES ('user3', 'Charlie', 22)")
        conn.commit()
        print("Successfully added 'user3' to Source DB.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    add_data()
