import cx_Oracle
import sys
import os
import time
from datetime import datetime, timedelta

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import SOURCE_DB_CONFIG

def create_out_of_order_data():
    conn = None
    try:
        conn = cx_Oracle.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 테이블 생성
        print("Creating TB_WORK_PROCESS table...")
        cursor.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE TB_WORK_PROCESS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        cursor.execute("""
            CREATE TABLE TB_WORK_PROCESS (
                WORK_ID VARCHAR2(20) PRIMARY KEY,
                START_TIME TIMESTAMP,
                END_TIME TIMESTAMP,
                REG_DT TIMESTAMP DEFAULT SYSTIMESTAMP,
                UPD_DT TIMESTAMP DEFAULT SYSTIMESTAMP,
                STATUS VARCHAR2(20)
            )
        """)
        
        # 2. 과거 데이터 생성 (이미 완료된 작업)
        print("Inserting old records...")
        old_time = datetime.now() - timedelta(days=5)
        # :1~:4까지 총 4개의 바인딩 필요
        cursor.execute("""
            INSERT INTO TB_WORK_PROCESS (WORK_ID, START_TIME, END_TIME, REG_DT, UPD_DT, STATUS)
            VALUES ('JOB_OLD_01', :1, :2, :3, :4, 'COMPLETED')
        """, [old_time, old_time + timedelta(hours=1), old_time, old_time])

        # 3. '뒤늦게 도착한' 데이터 (Out-of-order)
        print("Inserting out-of-order record...")
        now = datetime.now()
        ten_mins_ago = now - timedelta(minutes=10)
        cursor.execute("""
            INSERT INTO TB_WORK_PROCESS (WORK_ID, START_TIME, REG_DT, UPD_DT, STATUS)
            VALUES ('JOB_LATE_01', :1, :2, :3, 'RUNNING')
        """, [ten_mins_ago, now, now])
        
        conn.commit()
        print("Initial data created successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    create_out_of_order_data()