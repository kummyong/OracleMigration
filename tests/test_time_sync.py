import unittest
from unittest.mock import patch, mock_open
import json
from datetime import datetime, timedelta
import sys
import os

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# cx_Oracle 모킹
sys.modules['cx_Oracle'] = unittest.mock.MagicMock()

from migration_utils import get_last_sync_time

class TestTimeSync(unittest.TestCase):
    
    def test_get_last_sync_time_exact_match(self):
        """테이블 이름이 정확히 일치할 때 시간을 잘 가져오는지"""
        mock_data = json.dumps({"TEST_TABLE": "2025-01-01T12:00:00"})
        with patch("builtins.open", mock_open(read_data=mock_data)):
            with patch("os.path.exists", return_value=True):
                result = get_last_sync_time("TEST_TABLE")
                expected = datetime(2025, 1, 1, 12, 0, 0)
                self.assertEqual(result, expected)

    def test_get_last_sync_time_case_insensitive(self):
        """파일에는 대문자로 저장되어 있고, 입력은 소문자로 들어올 때 (대소문자 무시 조회 검증)"""
        mock_data = json.dumps({"TEST_TABLE": "2025-01-01T12:00:00"})
        with patch("builtins.open", mock_open(read_data=mock_data)):
            with patch("os.path.exists", return_value=True):
                # 소문자 "test_table"로 조회 시도
                result = get_last_sync_time("test_table")
                
                # 기대값: 대문자 키의 값을 찾아와야 함
                expected = datetime(2025, 1, 1, 12, 0, 0)
                
                # 현재 코드라면 여기서 실패할 것임 (기본값인 3일 전 시간이 반환됨)
                self.assertEqual(result, expected)

    def test_get_last_sync_time_no_record(self):
        """기록이 없을 때 3일 전 시간을 반환하는지"""
        mock_data = json.dumps({})
        with patch("builtins.open", mock_open(read_data=mock_data)):
            with patch("os.path.exists", return_value=True):
                result = get_last_sync_time("NEW_TABLE")
                
                # 3일 전 시간 계산 (초 단위 오차 허용을 위해 범위 비교)
                expected_base = datetime.now() - timedelta(days=3)
                diff = abs((result - expected_base).total_seconds())
                
                self.assertLess(diff, 5) # 5초 이내 오차면 성공

if __name__ == '__main__':
    unittest.main()