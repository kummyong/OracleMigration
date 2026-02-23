
import unittest
from datetime import datetime
import migration_utils as mu
import sys
from unittest.mock import MagicMock

class SqlGenerationTest(unittest.TestCase):
    def setUp(self):
        # cx_Oracle 모킹
        self.mock_cx = MagicMock()
        mu.cx_Oracle = self.mock_cx
        
    def test_merge_sql_with_where_clause(self):
        """MERGE 문에 Redo Log 최적화를 위한 WHERE 절이 포함되는지 확인"""
        
        # 가상의 연결 객체
        conn = MagicMock()
        conn.username = "TEST_USER"
        cursor = conn.cursor.return_value.__enter__.return_value
        
        table_name = "SENSOR_DATA"
        p_keys = ["ID"]
        columns = ["ID", "VAL", "STATUS"]
        rows = [(1, 10.5, "OK")]
        
        # 실행
        mu._load_data_merge(conn, table_name, p_keys, columns, rows)
        
        # 호출된 SQL 추출
        called_sql = cursor.executemany.call_args[0][0]
        
        # 1. WHERE 절 포함 여부 확인
        self.assertIn("WHEN MATCHED THEN UPDATE SET", called_sql)
        self.assertIn("WHERE", called_sql)
        
        # 2. 값 비교 조건 확인 (V_1, V_2 는 바인딩 변수 맵핑)
        self.assertIn('T."VAL" != S.V_1', called_sql)
        self.assertIn('T."STATUS" != S.V_2', called_sql)
        
        # 3. NULL 처리 조건 확인 (NULL인 경우에도 업데이트가 되어야 함)
        self.assertIn('T."VAL" IS NULL AND S.V_1 IS NOT NULL', called_sql)
        
        # 4. PK가 UPDATE 절에서 제외되었는지 확인
        # ID는 PK이므로 UPDATE SET "ID"=... 가 없어야 함
        update_part = called_sql.split("SET")[1].split("WHERE")[0]
        self.assertNotIn('"ID"', update_part)

    def test_merge_sql_all_columns_null_safe(self):
        """모든 업데이트 컬럼에 대해 NULL 안전 비교가 생성되는지 확인"""
        conn = MagicMock()
        conn.username = "TEST_USER"
        cursor = conn.cursor.return_value.__enter__.return_value
        
        columns = ["ID", "COL1"]
        mu._load_data_merge(conn, "TB", ["ID"], columns, [(1, "A")])
        
        sql = cursor.executemany.call_args[0][0]
        
        # 특정 패턴 확인: (T.COL IS NULL AND S.VAL IS NOT NULL) OR (T.COL IS NOT NULL AND S.VAL IS NULL) OR (T.COL != S.VAL)
        expected_pattern = '(T."COL1" IS NULL AND S.V_1 IS NOT NULL) OR (T."COL1" IS NOT NULL AND S.V_1 IS NULL) OR (T."COL1" != S.V_1)'
        self.assertIn(expected_pattern, sql)

if __name__ == "__main__":
    unittest.main()
