import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# 프로젝트 루트 경로를 sys.path에 추가하여 모듈 임포트 가능하게 함
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# cx_Oracle 모킹 (import 전에 해야 함)
sys.modules['cx_Oracle'] = MagicMock()

from migration_utils import _load_data_merge

class TestMigrationUtils(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        self.mock_conn.username = "TEST_USER" # Oracle 연결처럼 위장

    def test_merge_sql_generation_success(self):
        """정상적인 PK가 있을 때 MERGE SQL이 올바르게 생성되는지 테스트"""
        table_name = "TEST_TABLE"
        p_keys = ["ID"]
        columns = ["ID", "NAME", "VAL"]
        rows = [[1, "Test", 100]]

        # 실행
        _load_data_merge(self.mock_conn, table_name, p_keys, columns, rows)

        # 검증
        # executemany가 호출되었는지 확인
        self.mock_cursor.executemany.assert_called()
        
        # 생성된 SQL 가져오기
        call_args = self.mock_cursor.executemany.call_args
        generated_sql = call_args[0][0]
        
        print(f"\nGenerated SQL: {generated_sql}")

        # 필수 구문 검증
        self.assertIn("MERGE INTO TEST_TABLE T", generated_sql)
        self.assertIn("USING (SELECT :1 V_0, :2 V_1, :3 V_2 FROM DUAL) S", generated_sql)
        self.assertIn('ON (T."ID" = S.V_0)', generated_sql)
        
        # UPDATE 절 검증
        self.assertIn('UPDATE SET T."NAME"=S.V_1, T."VAL"=S.V_2', generated_sql)
        
        # INSERT 절 검증
        self.assertIn('INSERT ("ID", "NAME", "VAL") VALUES (S.V_0, S.V_1, S.V_2)', generated_sql)

    def test_merge_sql_case_insensitive_pk(self):
        """설정된 PK(소문자)와 DB 컬럼(대문자) 매칭 테스트 - 실제 DB 컬럼명을 사용해야 함"""
        table_name = "TEST_TABLE"
        p_keys = ["id"] # 소문자 입력
        columns = ["ID", "NAME"] # 대문자 컬럼
        rows = [[1, "Test"]]

        _load_data_merge(self.mock_conn, table_name, p_keys, columns, rows)
        
        generated_sql = self.mock_cursor.executemany.call_args[0][0]
        
        # 대소문자 무시하고 매칭하되, SQL 생성 시에는 DB 컬럼명인 "ID"를 사용했는지 확인
        self.assertIn('ON (T."ID" = S.V_0)', generated_sql)
        self.assertNotIn('T."id"', generated_sql) # 소문자 id가 SQL에 직접 들어가면 안 됨
        
    def test_quote_escaping(self):
        """ORA-01756, ORA-01747 방지를 위한 따옴표 처리 검증"""
        table_name = "TEST_TABLE"
        p_keys = ["ID"]
        columns = ["ID", "DESC"]
        rows = [[1, "Description"]]

        _load_data_merge(self.mock_conn, table_name, p_keys, columns, rows)
        
        generated_sql = self.mock_cursor.executemany.call_args[0][0]
        
        # 잘못된 이스케이프(")가 없는지 확인
        self.assertNotIn(r'T.\"', generated_sql) 
        self.assertNotIn(r'T.\"DESC\"', generated_sql)
        
        # 올바른 형태 확인
        self.assertIn('T."DESC"=S.V_1', generated_sql)

    def test_merge_integrity_error_scenario(self):
        """무결성 에러 발생 시나리오: MERGE SQL의 ON 절이 단순 비교인지 확인"""
        table_name = "TEST_TABLE"
        p_keys = ["ID", "CODE"]
        columns = ["ID", "CODE", "VAL"]
        # Source에서는 공백 없는 "A", Target DB에는 공백 있는 "A "가 있다고 가정
        rows = [[1, "A", 100]] 

        _load_data_merge(self.mock_conn, table_name, p_keys, columns, rows)
        
        generated_sql = self.mock_cursor.executemany.call_args[0][0]
        
        # 현재 로직: 단순 비교 (=)
        # 만약 DB의 'CODE' 컬럼이 CHAR 타입이라 공백이 들어가 있다면?
        # T."CODE" = S.V_1 비교는 'A ' = 'A' -> False가 됨.
        # 결과: INSERT 시도 -> PK 중복 (ORA-00001) 발생!
        # 해결책: TRIM()을 쓰거나 해야 하지만, 인덱스 때문에 함부로 쓸 수 없음.
        
        print(f"\n[Integrity Check SQL] {generated_sql}")
        self.assertIn('ON (T."ID" = S.V_0 AND T."CODE" = S.V_1)', generated_sql)

if __name__ == '__main__':
    unittest.main()