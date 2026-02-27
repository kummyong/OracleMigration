try:
    import oracledb as cx_Oracle
except ImportError:
    import cx_Oracle
import sys
import random
from datetime import datetime, timedelta
from config import SOURCE_DB_CONFIG

def create_large_sensor_data():
    conn = None
    try:
        conn = cx_Oracle.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 테이블 삭제 (기존 테이블 존재 시)
        try:
            cursor.execute("DROP TABLE TB_SENSOR_DATA PURGE")
        except:
            pass
            
        # 2. 테이블 생성
        print("Creating TB_SENSOR_DATA table...")
        cursor.execute("""
            CREATE TABLE TB_SENSOR_DATA (
                SENSOR_ID VARCHAR2(20) PRIMARY KEY,
                MEASURE_VAL NUMBER(10, 2),
                STATUS VARCHAR2(10),
                EVENT_TIME TIMESTAMP DEFAULT SYSTIMESTAMP
            )
        """)
        
        # 3. 데이터 생성 (10,000건)
        print("Generating 10,000 rows of sensor data...")
        # 10,000초 전부터 1초 단위로 데이터 생성
        start_time = datetime.now() - timedelta(seconds=10000)
        
        data = []
        for i in range(10000):
            event_time = start_time + timedelta(seconds=i)
            sensor_id = f"SENS_{i+1:05d}"
            val = round(random.uniform(20.0, 80.0), 2)
            status = random.choice(['NORMAL', 'NORMAL', 'NORMAL', 'WARN', 'ERROR'])
            data.append((sensor_id, val, status, event_time))
            
        # 4. 대량 Insert (executemany)
        cursor.executemany("""
            INSERT INTO TB_SENSOR_DATA (SENSOR_ID, MEASURE_VAL, STATUS, EVENT_TIME)
            VALUES (:1, :2, :3, :4)
        """, data)
        
        conn.commit()
        print(f"Successfully created 10,000 rows. Start: {start_time}, End: {event_time}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    create_large_sensor_data()