import cx_Oracle
import logging
import os
from datetime import datetime
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, LAST_SYNC_TIME_FILE

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(config):
    """지정된 설정으로 데이터베이스 연결을 생성합니다."""
    try:
        conn = cx_Oracle.connect(
            user=config['user'],
            password=config['password'],
            dsn=config['dsn']
        )
        logging.info(f"성공적으로 DB에 연결되었습니다. (DSN: {config['dsn']})")
        return conn
    except cx_Oracle.Error as e:
        logging.error(f"DB 연결 중 오류 발생 (DSN: {config['dsn']}): {e}")
        raise

def get_primary_keys(connection, table_name):
    """
    소스 DB의 데이터 딕셔너리를 조회하여 테이블의 Primary Key 컬럼 목록을 반환합니다.
    """
    query = """
    SELECT
        cols.column_name
    FROM
        all_constraints cons
    JOIN
        all_cons_columns cols ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name
    WHERE
        cons.constraint_type = 'P'
        AND cons.owner = :owner
        AND cons.table_name = :table_name
    ORDER BY
        cols.position
    """
    # 일반적으로 접속 유저 이름이 스키마(owner) 이름과 동일합니다.
    owner = connection.username.upper()
    table_name_upper = table_name.upper()

    logging.info(f"[{table_name}] 테이블의 Primary Key를 소스 DB에서 조회합니다...")
    with connection.cursor() as cursor:
        cursor.execute(query, {'owner': owner, 'table_name': table_name_upper})
        keys = [row[0] for row in cursor.fetchall()]
        if keys:
            logging.info(f"[{table_name}] 자동 조회된 PK: {keys}")
        else:
            logging.warning(f"[{table_name}] 테이블에서 Primary Key를 찾을 수 없습니다. (MERGE 불가)")
        return keys

