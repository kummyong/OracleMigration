import cx_Oracle
import logging
import os
from datetime import datetime
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, LAST_SYNC_TIME_FILE

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

def get_upsert_keys(connection, table_name):
    """
    소스 DB의 데이터 딕셔너리를 조회하여 테이블의 Upsert에 사용할 키 컬럼 목록을 반환합니다.
    1순위: Primary Key
    2순위: 첫 번째 Unique Key
    """
    owner = connection.username.upper()
    table_name_upper = table_name.upper()

    # 1순위: Primary Key 조회
    pk_query = """
    SELECT cols.column_name
    FROM all_constraints cons
    JOIN all_cons_columns cols ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name
    WHERE cons.constraint_type = 'P'
      AND cons.owner = :owner
      AND cons.table_name = :table_name
    ORDER BY cols.position
    """
    logging.info(f"[{table_name}] 테이블의 Primary Key를 소스 DB에서 조회합니다...")
    with connection.cursor() as cursor:
        cursor.execute(pk_query, {'owner': owner, 'table_name': table_name_upper})
        keys = [row[0] for row in cursor.fetchall()]
        if keys:
            logging.info(f"[{table_name}] 자동 조회된 PK: {keys}")
            return keys

    logging.warning(f"[{table_name}] Primary Key가 없어 Unique Key를 조회합니다...")

    # 2순위: Unique Key 조회
    uk_query = """
    SELECT cols.column_name
    FROM all_constraints cons
    JOIN all_cons_columns cols ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name
    WHERE cons.constraint_type = 'U'
      AND cons.owner = :owner
      AND cons.table_name = :table_name
      AND cons.constraint_name = (
          -- 여러 Unique 제약조건 중 첫 번째 것만 선택
          SELECT MIN(sub_cons.constraint_name)
          FROM all_constraints sub_cons
          WHERE sub_cons.constraint_type = 'U'
            AND sub_cons.owner = :owner
            AND sub_cons.table_name = :table_name
      )
    ORDER BY cols.position
    """
    with connection.cursor() as cursor:
        cursor.execute(uk_query, {'owner': owner, 'table_name': table_name_upper})
        keys = [row[0] for row in cursor.fetchall()]
        if keys:
            logging.info(f"[{table_name}] 자동 조회된 Unique Key: {keys}")
        else:
            logging.warning(f"[{table_name}] 테이블에서 Primary Key 또는 Unique Key를 찾을 수 없습니다. (MERGE 불가)")
        return keys

