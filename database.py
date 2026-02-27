import logging
import os
from datetime import datetime
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, LAST_SYNC_TIME_FILE

try:
    import oracledb
    # cx_Oracle과의 호환성을 위해 별칭 설정
    cx_Oracle = oracledb
    # Thin 모드로 동작 (C Client 설치 불필요)
except ImportError:
    try:
        import cx_Oracle
    except ImportError:
        cx_Oracle = None
        logging.error("oracledb 또는 cx_Oracle 모듈이 설치되지 않았습니다.")

def get_db_connection(config):
    """지정된 설정으로 데이터베이스 연결을 생성합니다."""
    if cx_Oracle is None:
        raise ImportError("Oracle 접속 라이브러리(oracledb/cx_Oracle)가 설치되어 있지 않습니다.")
        
    try:
        # oracledb 2.x+ 에서는 connect 파라미터가 약간 다를 수 있으나 기본은 호환됨
        conn = cx_Oracle.connect(
            user=config['user'],
            password=config['password'],
            dsn=config['dsn']
        )
        logging.info(f"성공적으로 Oracle DB에 연결되었습니다. (DSN: {config['dsn']})")
        return conn
    except Exception as e:
        logging.error(f"Oracle DB 연결 중 오류 발생 (DSN: {config['dsn']}): {e}")
        raise

def get_session_pool(config, min_conn=1, max_conn=10):
    """지정된 설정으로 Oracle 세션 풀을 생성합니다."""
    if cx_Oracle is None:
        raise ImportError("Oracle 접속 라이브러리가 없어 세션 풀을 생성할 수 없습니다.")

    try:
        # oracledb와 cx_Oracle의 SessionPool 파라미터 호환성 처리
        if hasattr(cx_Oracle, 'create_pool'): # oracledb 방식
            pool = cx_Oracle.create_pool(
                user=config['user'],
                password=config['password'],
                dsn=config['dsn'],
                min=min_conn,
                max=max_conn,
                increment=1,
                getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT,
                ping_interval=60
            )
        else: # cx_Oracle 방식
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
    except Exception as e:
        logging.error(f"세션 풀 생성 중 오류 발생: {e}")
        raise

def _get_db_user(connection):
    """안전하게 DB 사용자명을 가져옵니다. (connection.username이 None인 경우 대비)"""
    try:
        if hasattr(connection, 'username') and connection.username:
            return connection.username.upper()
        
        with connection.cursor() as cursor:
            cursor.execute("SELECT USER FROM DUAL")
            row = cursor.fetchone()
            if row:
                return row[0].upper()
    except Exception as e:
        logging.warning(f"DB 사용자명 조회 실패: {e}")
    return None

def get_upsert_keys(connection, table_name):
    """지능형 PK 탐지 로직"""
    owner = _get_db_user(connection)
    if not owner: return []

    table_name_upper = table_name.upper()

    query = """
    SELECT cols.column_name, cons.constraint_type
    FROM all_constraints cons
    JOIN all_cons_columns cols ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name
    WHERE cons.owner = :owner AND cons.table_name = :table_name
      AND cons.constraint_type IN ('P', 'U') AND cons.status = 'ENABLED'
    ORDER BY cons.constraint_type, cols.position
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, {'owner': owner, 'table_name': table_name_upper})
            rows = cursor.fetchall()
            if rows:
                pk_rows = [r[0] for r in rows if r[1] == 'P']
                if pk_rows: return pk_rows
                return [rows[0][0]]

        candidate_keywords = ['ID', 'NO', 'CD', 'CODE', 'SEQ', 'NUM', 'KEY']
        candidate_query = """
        SELECT column_name FROM all_tab_columns 
        WHERE owner = :owner AND table_name = :table_name AND nullable = 'N'
        """
        with connection.cursor() as cursor:
            cursor.execute(candidate_query, {'owner': owner, 'table_name': table_name_upper})
            candidates = [r[0] for r in cursor.fetchall() if any(kw in r[0].upper() for kw in candidate_keywords)]
            
            for col in candidates:
                prof_query = f'SELECT 1 FROM (SELECT "{col}" FROM "{table_name}" GROUP BY "{col}" HAVING COUNT(*) > 1) WHERE ROWNUM = 1'
                try:
                    cursor.execute(prof_query)
                    if cursor.fetchone() is None:
                        return [col]
                except: continue
        return []
    except Exception as e:
        logging.error(f"[{table_name}] PK 탐색 중 오류: {e}")
        return []

def get_table_columns(connection, table_name):
    """컬럼 정보 조회"""
    table_name_upper = table_name.upper()
    owner = _get_db_user(connection)
    if owner:
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
