import logging
import oracledb
from config import TARGET_DB_CONFIG

# Setup logging
logging.basicConfig(level=logging.INFO)

def check_schema():
    try:
        conn = oracledb.connect(
            user=TARGET_DB_CONFIG['user'],
            password=TARGET_DB_CONFIG['password'],
            dsn=TARGET_DB_CONFIG['dsn']
        )
        cursor = conn.cursor()
        
        table_name = "SM_FILE" 
        owner = TARGET_DB_CONFIG['user'].upper()
        
        query = """
        SELECT column_name, data_type, data_scale
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :table_name
        ORDER BY column_id
        """
        
        cursor.execute(query, {'owner': owner, 'table_name': table_name})
        rows = cursor.fetchall()
        
        print(f"--- Schema for {table_name} ---")
        for row in rows:
            print(f"Column: {row[0]}, Type: {row[1]}, Scale: {row[2]}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
