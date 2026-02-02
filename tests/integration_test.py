import unittest
import sys
import oracledb
sys.modules["cx_Oracle"] = oracledb
import cx_Oracle
import os
import json
import time
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 실제 모듈 임포트
from migration_utils import migrate

# 테스트용 설정 (docker-compose와 일치해야 함)
TEST_SOURCE_DSN = "127.0.0.1:1521/XE"
TEST_TARGET_DSN = "127.0.0.1:1522/XE"
TEST_USER = "DB_TESTER"
TEST_PW = "test_password"


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Docker DB가 뜰 때까지 대기"""
        cls.source_conn = None
        cls.target_conn = None

        print("Waiting for Oracle DBs to be ready...")
        max_retries = 40
        for i in range(max_retries):
            try:
                cls.source_conn = cx_Oracle.connect(
                    user=TEST_USER, password=TEST_PW, dsn=TEST_SOURCE_DSN
                )
                cls.target_conn = cx_Oracle.connect(
                    user=TEST_USER, password=TEST_PW, dsn=TEST_TARGET_DSN
                )
                print("\nOracle DB Connected!")
                break
            except cx_Oracle.Error as e:
                print(f".", end="", flush=True)
                time.sleep(5)

        if not cls.source_conn or not cls.target_conn:
            print("\n")
            raise Exception(
                "Failed to connect to Oracle DBs after long wait. Check container logs."
            )

    @classmethod
    def tearDownClass(cls):
        if cls.source_conn:
            cls.source_conn.close()
        if cls.target_conn:
            cls.target_conn.close()

    def test_1_auto_config_generation(self):
        """init_config.py 및 database.py의 지능형 PK 탐지 검증"""
        cursor = self.source_conn.cursor()
        from init_config import get_all_tables
        from database import get_upsert_keys

        tables = get_all_tables(cursor, TEST_USER)
        print(f"\nDetected tables for {TEST_USER}: {tables}")

        self.assertIn("TB_USER", tables)
        self.assertIn("TB_LOG", tables)

        # 지능형 PK 탐지 테스트 (TB_LOG는 PK가 없으나 LOG_ID를 가상 PK로 찾는지 확인)
        log_pk = get_upsert_keys(self.source_conn, "TB_LOG")
        print(f"Detected PK for TB_LOG: {log_pk}")
        self.assertIn("LOG_ID", log_pk)

    def test_2_adaptive_chunking(self):
        """데이터 동기화 및 자율 주행 청킹(Adaptive Chunking) 검증"""
        from migration_utils import migrate

        # 대량 테이블 중 하나 선택
        table_name = "TB_SENSOR_001"
        last_sync = datetime(2000, 1, 1)
        sync_start_time = datetime.now()

        print(f"\nMigrating {table_name} with Adaptive Chunking...")
        # 초기 fetchsize 5000으로 시작
        result = migrate(
            table_name,
            ["SENSOR_ID"],
            "EVENT_TIME",
            5000,
            last_sync,
            sync_start_time,
            self.source_conn,
            self.target_conn,
        )

        print(f"Migration Result: {result}")
        self.assertEqual(result["errors"], 0)
        self.assertGreaterEqual(result["processed"], 100000)


if __name__ == "__main__":
    unittest.main()
