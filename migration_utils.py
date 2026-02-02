# migration_utils.py
import oracledb
import sys
# cx_Oracle 호환 모드 활성화
sys.modules["cx_Oracle"] = oracledb
import cx_Oracle
import logging
import os
import json
import time
from datetime import datetime, timedelta
from config import LAST_SYNC_TIME_FILE
from database import get_table_columns

# 시간 쪼개기 단위 설정 (기본 1시간)
TIME_SLICE_MINUTES = 60 

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

def get_last_sync_time(table_name, lock=None):
    try:
        times = {}
        def _read_file():
            if os.path.exists(LAST_SYNC_TIME_FILE):
                with open(LAST_SYNC_TIME_FILE, 'r') as f:
                    data = json.load(f)
                    return {k.upper(): v for k, v in data.items()}
            return {}
        if lock:
            with lock: times = _read_file()
        else: times = _read_file()
        
        default_start = (datetime.now() - timedelta(days=3)).isoformat()
        time_str = times.get(table_name.upper())
        
        if time_str:
            return datetime.fromisoformat(time_str)
        return datetime.fromisoformat(default_start)
    except Exception as e:
        return datetime.now() - timedelta(days=3)

def update_last_sync_time(table_name, sync_time, lock):
    with lock:
        try:
            times = {}
            if os.path.exists(LAST_SYNC_TIME_FILE):
                with open(LAST_SYNC_TIME_FILE, 'r') as f:
                    try:
                        times = json.load(f)
                    except: times = {}
            times[table_name] = sync_time.isoformat()
            with open(LAST_SYNC_TIME_FILE, 'w') as f:
                json.dump(times, f, indent=4)
        except Exception as e: logging.error(f"Time save failed: {e}")

def _load_data_merge(connection, table_name, p_keys, columns, rows):
    if not rows: return 0, 0
    num_errors = 0
    
    if not (cx_Oracle and hasattr(connection, 'username')):
        raise ValueError("Oracle Connection is required for migration.")

    if p_keys:
        try:
            with connection.cursor() as cursor:
                # SQL Execution Timeout (60s)
                if cx_Oracle: 
                    try:
                        if hasattr(cursor, 'connection') and hasattr(cursor.connection, 'callTimeout'):
                            cursor.connection.callTimeout = 60000
                    except: pass
                
                col_map = {col: f"V_{i}" for i, col in enumerate(columns)}
                col_map_upper = {col.upper(): f"V_{i}" for i, col in enumerate(columns)}
                using_vals = ", ".join([f":{i+1} {col_map[col]}" for i, col in enumerate(columns)])
                
                on_cond_list = []
                matched_pk_cols_upper = []
                for pk in p_keys:
                    pk_upper = pk.upper()
                    if pk_upper in col_map_upper:
                        actual_col = next(c for c in columns if c.upper() == pk_upper)
                        on_cond_list.append(f"T.\"{actual_col}\" = S.{col_map_upper[pk_upper]}")
                        matched_pk_cols_upper.append(pk_upper)
                
                if not on_cond_list: raise ValueError(f"No valid PKs found for: {p_keys}")
                on_cond = " AND ".join(on_cond_list)
                
                update_cols = [col for col in columns if col.upper() not in matched_pk_cols_upper]
                update_clause = ""
                if update_cols:
                    updates = [f"T.\"{c}\"=S.{col_map[c]}" for c in update_cols]
                    update_clause = f"WHEN MATCHED THEN UPDATE SET {', '.join(updates)}"
                
                cols_str = ", ".join([f"\"{c}\"" for c in columns])
                vals_str = ", ".join([f"S.{col_map[c]}" for c in columns])
                
                merge_sql = f"MERGE INTO {table_name} T USING (SELECT {using_vals} FROM DUAL) S ON ({on_cond}) {update_clause} WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({vals_str})"

                input_sizes = [cx_Oracle.TIMESTAMP if isinstance(val, datetime) else (cx_Oracle.CLOB if isinstance(val, str) and len(val) > 4000 else None) for val in rows[0]]
                cursor.setinputsizes(*input_sizes)

                try:
                    cursor.executemany(merge_sql, rows)
                except cx_Oracle.Error:
                    connection.rollback()
                    for row in rows:
                        try: cursor.execute(merge_sql, row)
                        except Exception as e:
                            num_errors += 1
                            logging.error(f"[{table_name}] Merge Error: {e}")
            
            if num_errors < len(rows): connection.commit()
            return len(rows) - num_errors, num_errors
        except Exception as e:
            logging.error(f"[{table_name}] Fatal Merge Error: {e}", exc_info=True)
            connection.rollback()
            raise
    else:
        # INSERT Mode
        binds = ", ".join([f":{i+1}" for i in range(len(columns))])
        quoted_cols = ", ".join([f"\"{c}\"" for c in columns])
        sql = f"INSERT INTO {table_name} ({quoted_cols}) VALUES ({binds})"
        try:
            with connection.cursor() as cursor:
                try:
                    cursor.executemany(sql, rows, batcherrors=True)
                    for error in cursor.getbatcherrors():
                        num_errors += 1
                        logging.warning(f"[{table_name}] Batch Insert Error: {error.message}")
                except cx_Oracle.Error as e:
                    connection.rollback()
                    for row in rows:
                        try: cursor.execute(sql, row)
                        except: num_errors += 1
            if num_errors < len(rows): connection.commit()
            return len(rows) - num_errors, 0
        except Exception as e:
            logging.error(f"[{table_name}] Fatal Insert Error: {e}", exc_info=True)
            connection.rollback()
            raise

def migrate(table_name, p_keys, date_column_name, fetchsize, start_date, end_date, source_conn, target_conn, update_callback=None, hint_index_column=None, index_scan_gap_minutes=0):
    logging.info(f"[{table_name}] Migration start: {start_date} -> {end_date}")
    total_rows = 0
    total_err = 0
    
    # 1. 대상 컬럼 확인
    target_cols = get_table_columns(target_conn, table_name)
    if not target_cols:
        sel_clause = "*"
    else:
        sel_clause = ", ".join([f"\"{c[0]}\"" for c in target_cols])

    # 2. 시간 쪼개기 루프 시작
    current_start = start_date
    slice_delta = timedelta(minutes=TIME_SLICE_MINUTES)

    while current_start < end_date:
        current_slice_end = min(current_start + slice_delta, end_date)
        logging.info(f"[{table_name}] Processing Slice: {current_start} ~ {current_slice_end}")

        with source_conn.cursor() as s_cur:
            if cx_Oracle and hasattr(source_conn, 'username'):
                s_cur.outputtypehandler = OutputTypeHandler

            where = f"\"{date_column_name}\" >= :1 AND \"{date_column_name}\" < :2"
            params = [current_start, current_slice_end]

            if hint_index_column:
                idx_start = current_start - timedelta(minutes=index_scan_gap_minutes)
                where += f" AND \"{hint_index_column}\" >= :3"
                params.append(idx_start)
            
            query = f"SELECT {sel_clause} FROM \"{table_name}\" WHERE {where} ORDER BY \"{date_column_name}\""
            if "." in table_name:
                parts = table_name.split(".")
                quoted_table = ".".join([f"\"{p}\"" for p in parts])
                query = query.replace(f"\"{table_name}\"", quoted_table)

            s_cur.arraysize = fetchsize
            
            try:
                # 쿼리 실행 타임아웃 설정 (60초)
                if cx_Oracle and hasattr(s_cur, 'connection') and hasattr(s_cur.connection, 'callTimeout'):
                    s_cur.connection.callTimeout = 60000
                
                s_cur.execute(query, params)
                
                cols = [d[0] for d in s_cur.description]
                try: date_idx = cols.index(date_column_name)
                except: 
                    col_map = {c.upper(): i for i, c in enumerate(cols)}
                    date_idx = col_map[date_column_name.upper()]

                # 자율 주행 청킹 (Adaptive Chunking) 초기화
                current_fetchsize = fetchsize
                
                while True:
                    t_chunk_start = time.monotonic()
                    
                    logging.debug(f"[{table_name}] Fetching next chunk (Size: {current_fetchsize})...")
                    rows = s_cur.fetchmany(current_fetchsize)
                    if not rows: break
                    
                    chunk_max_ts = max(r[date_idx] for r in rows)
                    
                    suc, err = _load_data_merge(target_conn, table_name, p_keys, cols, rows)
                    t_chunk_end = time.monotonic()
                    
                    total_rows += suc
                    total_err += err
                    
                    # 처리 속도 계산 및 fetchsize 자율 조정
                    duration = max(t_chunk_end - t_chunk_start, 0.1)
                    rows_per_sec = len(rows) / duration
                    
                    # 목표 시간 5초에 맞춘 다음 fetchsize 계산
                    ideal_fetchsize = int(len(rows) * (5.0 / duration))
                    # 급격한 변화 방지 (±50% 제한) 및 범위 제한 (100 ~ 100,000)
                    next_fetchsize = max(min(ideal_fetchsize, current_fetchsize * 1.5), current_fetchsize * 0.5)
                    current_fetchsize = int(max(min(next_fetchsize, 100000), 100))
                    
                    logging.info(f"[{table_name}] 처리 속도: {rows_per_sec:.1f} row/s, 다음 Fetch Size: {current_fetchsize}")
                    
                    # 중간 진행 상황 콜백 보강
                    if update_callback:
                        update_callback({
                            "current_max_ts": chunk_max_ts,
                            "slice_start": current_start,
                            "slice_end": current_slice_end,
                            "processed": total_rows
                        })

            except Exception as e:
                logging.error(f"[{table_name}] Slice Failed ({current_start}): {e}")
                # 슬라이스 하나 실패해도 다음으로 넘어갈지 여부는 정책에 따라 다름. 여기서는 중단.
                raise

        # 다음 슬라이스로 이동
        current_start = current_slice_end

    return {"processed": total_rows, "errors": total_err, "max_ts": end_date}