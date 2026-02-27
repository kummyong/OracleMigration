try:
    import oracledb as cx_Oracle
except ImportError:
    import cx_Oracle
import sys
import os
import random
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 프로젝트 루트 경로 추가하여 config 등 임포트 가능하게 함
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import SOURCE_DB_CONFIG

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_COUNT = 50
ROWS_PER_TABLE = 100000
BATCH_SIZE = 10000  # 1만 건 단위 커밋

def create_table_and_data(table_num):
    table_name = f"TB_SENSOR_{table_num:03d}"
    conn = None
    try:
        conn = cx_Oracle.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 테이블 생성
        cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                SENSOR_ID VARCHAR2(20) PRIMARY KEY,
                MEASURE_VAL NUMBER(10, 2),
                STATUS VARCHAR2(10),
                EVENT_TIME TIMESTAMP DEFAULT SYSTIMESTAMP
            )
        """)
        
        # 2. 데이터 생성 및 주입
        start_time = datetime.now() - timedelta(seconds=ROWS_PER_TABLE)
        
        for batch_start in range(0, ROWS_PER_TABLE, BATCH_SIZE):
            data = []
            for i in range(batch_start, batch_start + BATCH_SIZE):
                event_time = start_time + timedelta(seconds=i)
                sensor_id = f"S_{table_num:03d}_{i:06d}"
                val = round(random.uniform(10.0, 90.0), 2)
                status = random.choice(['OK', 'OK', 'WARN', 'FAIL'])
                data.append((sensor_id, val, status, event_time))
            
            cursor.executemany(f"INSERT INTO {table_name} VALUES (:1, :2, :3, :4)", data)
            conn.commit()
            
        logging.info(f"Successfully created {table_name} with {ROWS_PER_TABLE} rows.")
        
    except Exception as e:
        logging.error(f"Error creating {table_name}: {e}")
    finally:
        if conn: conn.close()

def main():
    start_all = time.time()
    logging.info(f"Starting bulk creation of {TABLE_COUNT} tables...")
    
    # 병렬 실행 (동시 5개 테이블씩 생성)
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(create_table_and_data, range(1, TABLE_COUNT + 1)))
        
    duration = time.time() - start_all
    logging.info(f"Bulk creation complete! Total duration: {duration:.2f} seconds.")

if __name__ == "__main__":
    main()
