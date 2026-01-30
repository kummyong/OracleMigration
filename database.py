try:
    import oracledb
    import sys
    # cx_Oracle 호환 모드 활성화 (cx_Oracle이 설치되지 않아도 동작하게 함)
    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
    import cx_Oracle # 이제 oracledb가 cx_Oracle인 척 함
except ImportError:
    cx_Oracle = None
import logging
import os
from datetime import datetime
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, LAST_SYNC_TIME_FILE

def get_db_connection(config):
    """지정된 설정으로 데이터베이스 연결을 생성합니다."""
    # Oracle 처리
    if cx_Oracle is None:
        raise ImportError("cx_Oracle(oracledb) 패키지가 설치되어 있지 않아 Oracle DB에 연결할 수 없습니다.")
        
    try:
        conn = cx_Oracle.connect(
            user=config['user'],
            password=config['password'],
            dsn=config['dsn']
        )
        logging.info(f"성공적으로 Oracle DB에 연결되었습니다. (DSN: {config['dsn']})")
        return conn
    except cx_Oracle.Error as e:
        logging.error(f"Oracle DB 연결 중 오류 발생 (DSN: {config['dsn']}): {e}")
        raise

def get_session_pool(config, min_conn=1, max_conn=10):
    """지정된 설정으로 Oracle 세션 풀을 생성합니다."""
    if cx_Oracle is None:
        raise ImportError("cx_Oracle(oracledb) 패키지가 없어 세션 풀을 생성할 수 없습니다.")

    try:
        pool = cx_Oracle.SessionPool(
            user=config['user'],
            password=config['password'],
            dsn=config['dsn'],
            min=min_conn,
            max=max_conn,
            increment=1,
            encoding="UTF-8",
            threaded=True,
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT,
            ping_interval=60
        )
        logging.info(f"Oracle 세션 풀이 생성되었습니다. (DSN: {config['dsn']}, Max: {max_conn})")
        return pool
    except cx_Oracle.Error as e:
        logging.error(f"세션 풀 생성 중 오류 발생: {e}")
        raise

def get_upsert_keys(connection, table_name):
    """
    소스 DB의 데이터 딕셔너리를 조회하여 테이블의 Upsert에 사용할 키 컬럼 목록을 반환합니다.
    1순위: Primary Key
    2순위: 첫 번째 Unique Key
    """
    if hasattr(connection, 'username'):
        owner = connection.username.upper()
    else:
        return []

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
    with connection.cursor() as cursor:
        cursor.execute(pk_query, {'owner': owner, 'table_name': table_name_upper})
        keys = [row[0] for row in cursor.fetchall()]
        if keys:
            return keys

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

def get_table_columns(connection, table_name):
    """
    지정된 테이블의 컬럼 이름과 타입을 조회합니다.
    Oracle: all_tab_cols 사용
    반환값: [(col_name, data_type, data_length), ...]
    """
    table_name_upper = table_name.upper()

    # Oracle DB인지 확인
    if hasattr(connection, 'username'):
        owner = connection.username.upper()
        
        query = """
        SELECT column_name, data_type, data_length
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :table_name
        ORDER BY column_id
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, {'owner': owner, 'table_name': table_name_upper})
                return cursor.fetchall()
        except Exception as e:
            logging.warning(f"[{table_name}] 컬럼 조회 실패: {e}")
            return []
    
    return []
