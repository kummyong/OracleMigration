import unittest
import os
import shutil
import sys

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dashboard_generator import generate_html_dashboard

class TestDashboardGenerator(unittest.TestCase):
    def setUp(self):
        # 테스트용 출력 파일 경로
        self.output_path = "test_dashboard.html"
        self.temp_path = self.output_path + ".tmp"
        
        # 테스트용 데이터
        self.status_data = {
            "last_updated": "2025-01-01 12:00:00",
            "updated_tables_count": 1,
            "cycle_duration": 1.5,
            "tables": {
                "TEST_TABLE": {
                    "status": "success",
                    "processed": 100,
                    "errors": 0,
                    "duration": 0.5,
                    "last_sync_time": "2025-01-01 11:50:00",
                    "message": "성공"
                }
            }
        }
        self.interval = 10

    def tearDown(self):
        # 테스트 후 파일 정리
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

    def test_generate_dashboard_file(self):
        """대시보드 파일이 정상적으로 생성되는지 테스트"""
        generate_html_dashboard(self.status_data, self.interval, self.output_path)
        
        # 1. 파일 존재 여부 확인
        self.assertTrue(os.path.exists(self.output_path), "Dashboard file should exist")
        
        # 2. 임시 파일 삭제 여부 확인 (Atomic Write 검증)
        self.assertFalse(os.path.exists(self.temp_path), "Temp file should be removed")
        
        # 3. 내용 검증
        with open(self.output_path, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertIn("TEST_TABLE", content)
            self.assertIn("status-success", content)
            self.assertIn("2025-01-01 11:50:00", content)

if __name__ == '__main__':
    unittest.main()