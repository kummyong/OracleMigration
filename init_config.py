# init_config.py
import json
import os
import logging
import time
import sys
import cx_Oracle

from config import SOURCE_DB_CONFIG, TABLES_CONFIG_FILE
from database import get_db_connection, get_upsert_keys

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_existing_config():
    """기존 설정 파일을 로드하여 테이블명을 키로 하는 딕셔너리로 반환"""
    if os.path.exists(TABLES_CONFIG_FILE):
        try:
            with open(TABLES_CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {item['table_name']: item for item in data}
        except Exception as e:
            logging.warning(f"기존 설정 파일 로드 중 오류 발생 (무시하고 새로 생성): {e}")
    return {}

def get_all_tables(cursor, owner):
    """DB에서 모든 테이블 목록 조회 (BIN$ 제외)"""
    query = """
        SELECT table_name 
        FROM all_tables 
        WHERE owner = :owner 
          AND table_name NOT LIKE 'BIN$%' 
          AND nested = 'NO'
        ORDER BY table_name
    """
    cursor.execute(query, {'owner': owner})
    return [row[0] for row in cursor.fetchall()]

def find_best_date_column(cursor, owner, table_name):
    """동기화 기준 컬럼(Date Column) 자동 탐지 (Heuristic)"""
    query = """
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = :owner 
          AND table_name = :table_name 
          AND (data_type LIKE 'DATE%' OR data_type LIKE 'TIMESTAMP%')
        ORDER BY column_id
    """
    cursor.execute(query, {'owner': owner, 'table_name': table_name})
    date_cols = [row[0] for row in cursor.fetchall()]
    
    if not date_cols:
        return "CHECK_ME" # 날짜 컬럼 없음

    # 우선순위: 수정일(MOD, UPD) > 등록일(REG, INS) > 기타
    for col in date_cols:
        if any(keyword in col.upper() for keyword in ['MOD', 'UPD', 'LAST', 'CHANGE']):
            return col
    for col in date_cols:
        if any(keyword in col.upper() for keyword in ['REG', 'INS', 'CRE', 'DATE', 'TIME']):
            return col
            
    return date_cols[0] # 없으면 첫 번째 날짜 컬럼

def main():
    conn = None
    try:
        # 1. 기존 설정 로드 (Smart Merge 준비)
        existing_configs = load_existing_config()
        logging.info(f"기존 설정 로드 완료: {len(existing_configs)}개 테이블")

        # 2. DB 연결 및 테이블 스캔
        logging.info("Source DB 연결 중...")
        conn = get_db_connection(SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        owner = SOURCE_DB_CONFIG['user'].upper()

        logging.info("전체 테이블 목록 조회 중...")
        all_tables = get_all_tables(cursor, owner)
        
        final_list = []
        new_count = 0
        preserved_count = 0

        for table_name in all_tables:
            # A. 기존 설정에 있으면 그대로 유지 (Preserve)
            if table_name in existing_configs:
                final_list.append(existing_configs[table_name])
                preserved_count += 1
                continue

            # B. 신규 테이블 분석 및 추가
            logging.info(f"[{table_name}] 신규 분석 중...")
            
            # PK 탐지 (데이터 프로파일링 로직 탑재된 database.py 함수 활용)
            pk_cols = get_upsert_keys(conn, table_name)
            
            # Date Column 탐지
            date_col = find_best_date_column(cursor, owner, table_name)

            # 신규 설정 객체 생성
            new_config = {
                "table_name": table_name,
                "enabled": True,
                "auto_schema_sync": False,
                "primary_keys": pk_cols,
                "date_column": date_col,
                "description": "자동 생성됨"
            }
            final_list.append(new_config)
            new_count += 1

        # 3. 결과 저장
        final_list.sort(key=lambda x: x['table_name'])

        with open(TABLES_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_list, f, indent=4, ensure_ascii=False)

        logging.info("=" * 50)
        logging.info(f"설정 파일 갱신 완료: {TABLES_CONFIG_FILE}")
        logging.info(f" - 총 테이블: {len(final_list)}")
        logging.info(f" - 기존 유지: {preserved_count}")
        logging.info(f" - 신규 추가: {new_count}")
        logging.info("=" * 50)

    except Exception as e:
        logging.error(f"설정 생성 중 오류 발생: {e}", exc_info=True)
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()