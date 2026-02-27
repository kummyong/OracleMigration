try:
    import oracledb as cx_Oracle
except ImportError:
    import cx_Oracle
import sys
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG

def create_db_tester(config, label):
    print(f"Creating DB_TESTER on {label} (using SYSTEM)...")
    try:
        # SYSTEM 계정으로 접속
        conn = cx_Oracle.connect(user="SYSTEM", password="oracle", dsn=config['dsn'])
        cursor = conn.cursor()
        
        # 유저 생성 및 권한 부여
        sql_commands = [
            "DROP USER DB_TESTER CASCADE", 
            "CREATE USER DB_TESTER IDENTIFIED BY test_password",
            "GRANT CONNECT, RESOURCE, DBA TO DB_TESTER",
            "ALTER USER DB_TESTER QUOTA UNLIMITED ON USERS"
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
