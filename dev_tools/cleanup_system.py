import oracledb
import sys
# cx_Oracle 호환 패치
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
import cx_Oracle

# SYSTEM 계정 정보 (직접 입력)
SYSTEM_CONFIG = {
    'user': 'SYSTEM',
    'password': 'oracle',
    'dsn': '127.0.0.1:1521/XE'
}
SYSTEM_CONFIG_TARGET = {
    'user': 'SYSTEM',
    'password': 'oracle',
    'dsn': '127.0.0.1:1522/XE'
}

def cleanup_system_schema(config, label):
    print(f"Cleaning up SYSTEM schema on {label}...")
    try:
        conn = cx_Oracle.connect(**config)
        cursor = conn.cursor()
        
        # 삭제할 테이블 목록 생성
        tables_to_drop = [f"TB_SENSOR_{i:03d}" for i in range(1, 51)]
        tables_to_drop.extend(["TB_SENSOR_DATA", "TB_USER", "TB_LOG"])
        
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE {table} PURGE")
                print(f"  Dropped: {table}")
            except Exception as e:
                # 테이블이 없으면 무시
                if "ORA-00942" in str(e): pass
                else: print(f"  Error dropping {table}: {e}")
        
        conn.commit()
        conn.close()
        print(f"Cleanup finished on {label}.\n")
    except Exception as e:
        print(f"Failed to connect to {label}: {e}")

if __name__ == "__main__":
    cleanup_system_schema(SYSTEM_CONFIG, "Source DB")
    cleanup_system_schema(SYSTEM_CONFIG_TARGET, "Target DB")
