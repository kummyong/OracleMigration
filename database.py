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


def get_last_sync_time(table_name):
    """테이블별 마지막 동기화 시간을 파일에서 읽어옵니다."""
    try:
        if os.path.exists(LAST_SYNC_TIME_FILE):
            with open(LAST_SYNC_TIME_FILE, 'r') as f:
                for line in f:
                    name, time_str = line.strip().split('=')
                    if name == table_name:
                        return datetime.fromisoformat(time_str)
        # 파일이나 테이블 항목이 없으면 아주 오래된 시간을 반환하여 전체 동기화를 유도
        return datetime(1970, 1, 1)
    except Exception as e:
        logging.warning(f"마지막 동기화 시간 로딩 중 오류: {e}. 전체 동기화를 위해 기본 시간을 반환합니다.")
        return datetime(1970, 1, 1)

def update_last_sync_time(table_name, sync_time):
    """테이블별 동기화 시간을 파일에 기록합니다."""
    times = {}
    if os.path.exists(LAST_SYNC_TIME_FILE):
        with open(LAST_SYNC_TIME_FILE, 'r') as f:
            for line in f:
                name, time_str = line.strip().split('=')
                times[name] = time_str
    
    times[table_name] = sync_time.isoformat()

    with open(LAST_SYNC_TIME_FILE, 'w') as f:
        for name, time_str in times.items():
            f.write(f"{name}={time_str}\n")


def extract_data(connection, table_name, timestamp_column, last_sync_time):
    """
    소스 DB에서 특정 시간 이후에 변경된 데이터를 추출합니다.
    """
    query = f"SELECT * FROM {table_name} WHERE {timestamp_column} > :1"
    
    logging.info(f"데이터 추출 실행: {query} (last_sync_time: {last_sync_time})")
    
    with connection.cursor() as cursor:
        cursor.execute(query, [last_sync_time])
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        logging.info(f"{len(rows)}개의 새로운/업데이트된 행을 찾았습니다.")
        return columns, rows

def load_data(connection, table_name, p_keys, columns, rows):
    """
    타겟 DB에 MERGE 문을 사용하여 데이터를 적재(Upsert)합니다.
    """
    if not rows:
        logging.info("적재할 데이터가 없습니다.")
        return

    # 1. 컬럼 목록 생성
    cols_str = ", ".join(columns)
    
    # 2. 바인드 변수 목록 생성 (:1, :2, ...)
    bind_vars = ", ".join([f":{i+1}" for i in range(len(columns))])
    
    # 3. MERGE ON 조건 생성 (PK 기반)
    # p_keys가 리스트라고 가정
    on_condition = " AND ".join([f"T.{pk} = S.{pk}" for pk in p_keys])

    # 4. UPDATE SET 절 생성 (PK 제외한 나머지 컬럼)
    update_cols = [col for col in columns if col not in p_keys]
    update_set = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
    if not update_set: # 모든 컬럼이 PK인 경우
        logging.warning(f"{table_name} 테이블의 모든 컬럼이 PK로 설정되어있어 UPDATE가 불가능합니다.")
        # UPDATE가 없는 INSERT 전용 MERGE 문으로 변경
        update_set = f"T.{p_keys[0]} = S.{p_keys[0]}" # 더미 업데이트

    # 5. INSERT 절 생성
    insert_cols = cols_str
    insert_vals = ", ".join([f"S.{col}" for col in columns])

    # 6. 전체 MERGE 문 조합
    merge_sql = f"""
    MERGE INTO {table_name} T
    USING (
        SELECT {', '.join([f':{i+1} AS {col}' for i, col in enumerate(columns, 1)])} FROM DUAL
    ) S ON ({on_condition})
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    """
    
    logging.info(f"{table_name} 테이블에 데이터 {len(rows)}건 MERGE 시작...")
    with connection.cursor() as cursor:
        # executemany를 사용하여 여러 행을 효율적으로 처리
        cursor.executemany(merge_sql, rows)
        connection.commit()
    logging.info(f"{len(rows)}건의 데이터가 성공적으로 MERGE 되었습니다.")


def sync_table(table_name, p_keys, timestamp_column):
    """
    지정된 테이블의 데이터 동기화를 수행합니다.
    """
    logging.info(f"테이블 동기화를 시작합니다: {table_name}")
    source_conn = None
    target_conn = None
    
    try:
        # 0. 현재 동기화 시작 시간 기록
        current_sync_time = datetime.now()

        # 1. 마지막 동기화 시간 가져오기
        last_sync_time = get_last_sync_time(table_name)
        
        # 2. DB 연결
        source_conn = get_db_connection(SOURCE_DB_CONFIG)
        target_conn = get_db_connection(TARGET_DB_CONFIG)
        
        # 3. 데이터 추출
        columns, rows = extract_data(source_conn, table_name, timestamp_column, last_sync_time)
        
        # 4. 데이터 적재
        if rows:
            load_data(target_conn, table_name, p_keys, columns, rows)
        
        # 5. 성공 시, 마지막 동기화 시간 업데이트
        update_last_sync_time(table_name, current_sync_time)
        
        logging.info(f"테이블 동기화가 성공적으로 완료되었습니다: {table_name}")

    except Exception as e:
        logging.error(f"{table_name} 테이블 동기화 중 심각한 오류 발생: {e}", exc_info=True)
        # 오류 발생 시 동기화 시간을 업데이트하지 않음
    
    finally:
        if source_conn:
            source_conn.close()
            logging.info("소스 DB 연결을 닫았습니다.")
        if target_conn:
            target_conn.close()
            logging.info("타겟 DB 연결을 닫았습니다.")

