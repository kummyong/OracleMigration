# migration_utils.py
import cx_Oracle
import logging
import os
import json
from datetime import datetime
from config import LAST_SYNC_TIME_FILE # 설정 파일에서 동기화 시간 파일 경로 가져오기

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_sync_time(table_name):
    """테이블별 마지막 동기화 시간을 JSON 파일에서 읽어옵니다."""
    try:
        if os.path.exists(LAST_SYNC_TIME_FILE):
            with open(LAST_SYNC_TIME_FILE, 'r') as f:
                times = json.load(f)
                if table_name in times:
                    return datetime.fromisoformat(times[table_name])
        # 파일이나 테이블 항목이 없으면 아주 오래된 시간을 반환하여 전체 동기화를 유도
        return datetime(1970, 1, 1)
    except (IOError, json.JSONDecodeError) as e:
        logging.warning(f"마지막 동기화 시간 로딩 중 오류: {e}. 전체 동기화를 위해 기본 시간을 반환합니다.")
        return datetime(1970, 1, 1)

def update_last_sync_time(table_name, sync_time, lock):
    """[Thread-safe] 테이블별 동기화 시간을 JSON 파일에 기록합니다."""
    with lock:
        times = {}
        try:
            if os.path.exists(LAST_SYNC_TIME_FILE):
                with open(LAST_SYNC_TIME_FILE, 'r') as f:
                    times = json.load(f)
        except (IOError, json.JSONDecodeError):
            logging.warning(f"{LAST_SYNC_TIME_FILE} 파일을 읽는 중 오류 발생. 새로운 파일을 생성합니다.")

        times[table_name] = sync_time.isoformat()

        try:
            with open(LAST_SYNC_TIME_FILE, 'w') as f:
                json.dump(times, f, indent=4)
        except IOError as e:
            logging.error(f"동기화 시간 파일({LAST_SYNC_TIME_FILE}) 저장 실패: {e}")


def _load_data_merge(connection, table_name, p_keys, columns, rows):
    """
    타겟 DB에 데이터를 적재하고, 오류 건수를 반환합니다.
    p_keys가 제공되면 MERGE (Upsert)를 사용하고, 그렇지 않으면 단순 INSERT를 사용합니다.
    """
    if not rows:
        logging.debug(f"[{table_name}] 적재할 새로운 데이터가 없습니다.")
        return 0

    num_errors = 0
    cols_str = ", ".join(columns)
    bind_vars = ", ".join([f':{i+1}' for i, _ in enumerate(columns)])

    with connection.cursor() as cursor:
        try:
            if not p_keys:
                # No primary keys, use simple INSERT
                logging.info(f"[{table_name}] Primary Key가 없어 단순 INSERT로 적재합니다. (중복 데이터가 발생할 수 있음)")
                insert_sql = f"""
                INSERT INTO {table_name} ({cols_str})
                VALUES ({bind_vars})
                """
                cursor.executemany(insert_sql, rows, batcherrors=True)
                errors = cursor.getbatcherrors()
                num_errors = len(errors)
                if num_errors > 0:
                    logging.warning(f"[{table_name}] {num_errors}건의 데이터에서 INSERT 오류 발생.")
                
                connection.commit()
                successful_rows = len(rows) - num_errors
                if successful_rows > 0:
                    logging.info(f"[{table_name}] {successful_rows}건 데이터 INSERT 완료.")
            else:
                # Primary keys are available, use MERGE (Upsert)
                logging.info(f"[{table_name}] Primary Key가 있어 MERGE (Upsert)로 적재합니다.")
                on_condition = " AND ".join([f"T.{pk} = S.{pk}" for pk in p_keys])
                update_cols = [col for col in columns if col not in p_keys]

                if update_cols:
                    update_set = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
                    update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
                else:
                    logging.info(f"[{table_name}] PK 외 업데이트할 컬럼이 없어 WHEN MATCHED 시 UPDATE가 아닌 단순 PK 일치 확인만 수행됩니다.")
                    update_clause = f"WHEN MATCHED THEN UPDATE SET T.{p_keys[0]} = S.{p_keys[0]}" # Still needs a valid column for update part

                insert_cols = cols_str
                insert_vals = ", ".join([f"S.{col}" for col in columns])

                merge_sql = f"""
                MERGE INTO {table_name} T
                USING (
                    SELECT {', '.join([f':{i+1} AS {col}' for i, col in enumerate(columns, 1)])} FROM DUAL
                ) S ON ({on_condition})
                {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})
                """
                cursor.executemany(merge_sql, rows, batcherrors=True)
                errors = cursor.getbatcherrors()
                num_errors = len(errors)
                
                if num_errors > 0:
                    logging.warning(f"[{table_name}] {num_errors}건의 데이터에서 MERGE 오류 발생.")
                
                connection.commit()
                successful_rows = len(rows) - num_errors
                if successful_rows > 0:
                    logging.info(f"[{table_name}] {successful_rows}건 데이터 MERGE 완료.")

        except cx_Oracle.Error as e:
            logging.error(f"[{table_name}] DB 작업 중 심각한 오류 발생: {e}")
            connection.rollback()
            raise
        except Exception as e:
            logging.error(f"[{table_name}] 알 수 없는 오류 발생: {e}")
            connection.rollback()
            raise
    
    return num_errors


def migrate(table_name, p_keys, date_column_name, fetchsize, start_date, end_date, source_conn, target_conn):
    """
    데이터를 마이그레이션하고 처리 결과(처리 건수, 오류 건수, 최종 동기화 시간)를 담은 딕셔너리를 반환합니다.
    """
    logging.debug(f"[{table_name}] 기간: {start_date} ~ {end_date}")

    total_rows_processed = 0
    total_errors = 0
    max_ts = start_date  # 현재까지 발견된 가장 최신 타임스탬프...
    
    try:
        with source_conn.cursor() as source_cursor:
            query = f"SELECT * FROM {table_name} WHERE {date_column_name} >= :start_date AND {date_column_name} < :end_date"
            
            source_cursor.execute(query, {'start_date': start_date, 'end_date': end_date})
            
            columns = [desc[0] for desc in source_cursor.description]
            date_column_index = columns.index(date_column_name)
            
            while True:
                rows = source_cursor.fetchmany(fetchsize)
                if not rows:
                    break
                
                # 현재 청크의 최신 타임스탬프 찾기
                current_chunk_max_ts = max(row[date_column_index] for row in rows)
                if current_chunk_max_ts > max_ts:
                    max_ts = current_chunk_max_ts

                chunk_size = len(rows)
                logging.info(f"[{table_name}] {chunk_size}건 데이터 추출. 타겟에 적재합니다.")
                
                errors_in_chunk = _load_data_merge(target_conn, table_name, p_keys, columns, rows)
                total_errors += errors_in_chunk
                total_rows_processed += chunk_size

            if total_rows_processed == 0:
                logging.info(f"[{table_name}] 기간 내 변경된 데이터가 없습니다.")
            else:
                logging.info(f"[{table_name}] 총 {total_rows_processed}건 처리 완료 (성공: {total_rows_processed - total_errors}, 실패: {total_errors}).")
            
            return {"processed": total_rows_processed, "errors": total_errors, "max_ts": max_ts}

    except cx_Oracle.Error as e:
        logging.error(f"[{table_name}] 마이그레이션 중 DB 오류 발생: {e}")
        target_conn.rollback()
        raise
    except Exception as e:
        logging.error(f"[{table_name}] 알 수 없는 오류 발생: {e}")
        target_conn.rollback()
        raise

