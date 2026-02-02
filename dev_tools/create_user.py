import cx_Oracle
import sys
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG

def create_db_tester(config, label):
    print(f"Creating DB_TESTER on {label}...")
    try:
        # SYSTEM 계정으로 접속
        conn = cx_Oracle.connect(**config)
        cursor = conn.cursor()
        
        # 유저 생성 및 권한 부여
        sql_commands = [
            "DROP USER DB_TESTER CASCADE", # 기존 유저 삭제
            "CREATE USER DB_TESTER IDENTIFIED BY test_password",
            "GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO DB_TESTER",
            "GRANT CREATE VIEW, CREATE TABLE, CREATE SEQUENCE TO DB_TESTER"
        ]
        
        for sql in sql_commands:
            try:
                cursor.execute(sql)
                print(f"  Executed: {sql}")
            except Exception as e:
                if "ORA-01918" in str(e): pass # User does not exist (for drop)
                else: print(f"  Error on '{sql}': {e}")
        
        conn.commit()
        conn.close()
        print(f"DB_TESTER created successfully on {label}.\n")
    except Exception as e:
        print(f"Failed to connect to {label}: {e}")

if __name__ == "__main__":
    create_db_tester(SOURCE_DB_CONFIG, "Source DB")
    create_db_tester(TARGET_DB_CONFIG, "Target DB")
