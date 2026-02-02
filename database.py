try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
    print("Error: cx_Oracle 모듈이 설치되지 않았습니다. 'pip install cx_Oracle'을 실행하세요.")
import logging
import os
from datetime import datetime
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, LAST_SYNC_TIME_FILE

def get_db_connection(config):
    """지정된 설정으로 데이터베이스 연결을 생성합니다."""
    # Oracle 처리
    if cx_Oracle is None:
        raise ImportError("cx_Oracle 패키지가 설치되어 있지 않아 Oracle DB에 연결할 수 없습니다.")
        
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
        raise ImportError("cx_Oracle 패키지가 없어 세션 풀을 생성할 수 없습니다.")

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
    지능형 PK 탐지 로직:
    1순위: PK 제약조건, 2순위: UK 제약조건, 3순위: 데이터 프로파일링을 통한 가상 PK
    """
    if not hasattr(connection, 'username'): return []
    owner = connection.username.upper()
    table_name_upper = table_name.upper()

    # 1, 2순위: PK 및 UK 제약조건 조회
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
                # P(PK)가 있으면 P만, 없으면 U(UK) 반환
                pk_rows = [r[0] for r in rows if r[1] == 'P']
                if pk_rows: return pk_rows
                return [rows[0][0]] # 첫 번째 UK 컬럼 반환

        # 3순위: Heuristic & Data Profiling (제약조건이 없는 경우)
        logging.info(f"[{table_name}] 제약조건 없음. 가상 PK 탐지를 시작합니다...")
        candidate_keywords = ['ID', 'NO', 'CD', 'CODE', 'SEQ', 'NUM', 'KEY']
        
        # Not Null 컬럼 중 후보 키워드 포함된 컬럼 추출
        candidate_query = """
        SELECT column_name FROM all_tab_columns 
        WHERE owner = :owner AND table_name = :table_name AND nullable = 'N'
        """
        with connection.cursor() as cursor:
            cursor.execute(candidate_query, {'owner': owner, 'table_name': table_name_upper})
            candidates = [r[0] for r in cursor.fetchall() if any(kw in r[0].upper() for kw in candidate_keywords)]
            
            for col in candidates:
                # 실제 데이터 중복 검증 쿼리 (최대 1건이라도 중복 발견 시 중단)
                prof_query = f'SELECT 1 FROM (SELECT "{col}" FROM "{table_name}" GROUP BY "{col}" HAVING COUNT(*) > 1) WHERE ROWNUM = 1'
                try:
                    # 프로파일링은 오래 걸릴 수 있으므로 짧은 타임아웃 권장되나 여기서는 기본 수행
                    cursor.execute(prof_query)
                    if cursor.fetchone() is None:
                        logging.info(f"[{table_name}] 데이터 검증된 가상 PK 채택: {col}")
                        return [col]
                except: continue
                
        logging.warning(f"[{table_name}] 유효한 키를 찾지 못했습니다. (INSERT 모드로 동작)")
        return []
    except Exception as e:
        logging.error(f"[{table_name}] PK 탐색 중 오류: {e}")
        return []

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
