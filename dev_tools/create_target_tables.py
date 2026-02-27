try:
    import oracledb as cx_Oracle
except ImportError:
    import cx_Oracle
import sys
import os
import logging

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import TARGET_DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_target_tables():
    conn = None
    try:
        conn = cx_Oracle.connect(**TARGET_DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 50개 센서 테이블
        for i in range(1, 51):
            table_name = f"TB_SENSOR_{i:03d}"
            cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
            cursor.execute(f"CREATE TABLE {table_name} (SENSOR_ID VARCHAR2(20) PRIMARY KEY, MEASURE_VAL NUMBER(10, 2), STATUS VARCHAR2(10), EVENT_TIME TIMESTAMP)")
            
        # 2. 기타 테이블들
        other_tables = ["TB_SENSOR_DATA", "TB_USER", "TB_LOG"]
        for table in other_tables:
            cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
            
        cursor.execute("CREATE TABLE TB_SENSOR_DATA (SENSOR_ID VARCHAR2(20) PRIMARY KEY, MEASURE_VAL NUMBER(10, 2), STATUS VARCHAR2(10), EVENT_TIME TIMESTAMP)")
        cursor.execute("CREATE TABLE TB_USER (USER_ID VARCHAR2(20) PRIMARY KEY, USER_NAME VARCHAR2(50), AGE NUMBER, REG_DT DATE)")
        cursor.execute("CREATE TABLE TB_LOG (LOG_ID VARCHAR2(20), MSG VARCHAR2(100), LOG_DT DATE)")
        
        conn.commit()
        logging.info("All target tables recreated successfully.")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    create_target_tables()