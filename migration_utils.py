# migration_utils.py
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
import logging
import os
import json
import time
from datetime import datetime, timedelta
from config import LAST_SYNC_TIME_FILE
from database import get_table_columns

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
        
        # Default to 3 days ago if no history found
        default_start = (datetime.now() - timedelta(days=3)).isoformat()
        
        # Look up using uppercase table name
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
                with open(LAST_SYNC_TIME_FILE, 'r') as f: times = json.load(f)
            times[table_name] = sync_time.isoformat()
            with open(LAST_SYNC_TIME_FILE, 'w') as f: json.dump(times, f, indent=4)
        except Exception as e: logging.error(f"Time save failed: {e}")

def _load_data_merge(connection, table_name, p_keys, columns, rows):
    if not rows: return 0, 0
    num_errors = 0
    is_oracle = cx_Oracle and hasattr(connection, 'username')
    
    if is_oracle and p_keys:
        # Standard MERGE with Fallback
        try:
            with connection.cursor() as cursor:
                # SQL Execution Timeout (60s)
                if cx_Oracle: 
                    try:
                        cursor.connection.callTimeout = 60000
                    except AttributeError:
                         try:
                            cursor.callTimeout = 60000
                         except AttributeError:
                             pass # Timeout 설정 실패 시 무시
                
                # Construct MERGE SQL using DUAL with Safe Aliases to prevent ORA-01745
                # Map column name to safe alias V_0, V_1, ...
            col_map = {col: f"V_{i}" for i, col in enumerate(columns)}
            # For case-insensitive lookup
            col_map_upper = {col.upper(): f"V_{i}" for i, col in enumerate(columns)}
            
            # Using DUAL with safe aliases
            # SELECT :1 V_0, :2 V_1 ... FROM DUAL
            using_vals = ", ".join([f":{i+1} {col_map[col]}" for i, col in enumerate(columns)])
            
            # Match p_keys case-insensitively to actual DB columns
            on_cond_list = []
            matched_pk_cols_upper = []
            for pk in p_keys:
                pk_upper = pk.upper()
                if pk_upper in col_map_upper:
                    # Use actual DB column name via simple equality (Index-friendly)
                    # Find original column name for quoting
                    actual_col = next(c for c in columns if c.upper() == pk_upper)
                    on_cond_list.append(f"T.\"{actual_col}\" = S.{col_map_upper[pk_upper]}")
                    matched_pk_cols_upper.append(pk_upper)
                else:
                    logging.warning(f"[{table_name}] Configured PK '{pk}' not found in target columns. Skipping in MERGE condition.")
            
            if not on_cond_list:
                raise ValueError(f"No valid PKs found for MERGE ON clause among: {p_keys}")

            on_cond = " AND ".join(on_cond_list)
            
            # Update clause: Update all columns that are NOT part of the PK
            update_cols = [col for col in columns if col.upper() not in matched_pk_cols_upper]
            update_clause = ""
            if update_cols:
                updates = [f"T.\"{c}\"=S.{col_map[c]}" for c in update_cols]
                update_clause = f"WHEN MATCHED THEN UPDATE SET {', '.join(updates)}"
            
            cols_str = ", ".join([f"\"{c}\"" for c in columns])
            vals_str = ", ".join([f"S.{col_map[c]}" for c in columns])
            
            merge_sql = f"MERGE INTO {table_name} T USING (SELECT {using_vals} FROM DUAL) S ON ({on_cond}) {update_clause} WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({vals_str})"

            # logging.info(f"[{table_name}] MERGE SQL: {merge_sql}")

            with connection.cursor() as cursor:
                try:
                    # 밀리초 보존을 위해 datetime 객체는 TIMESTAMP 타입으로 명시적 바인딩
                    input_sizes = []
                    for val in rows[0]:
                        if isinstance(val, datetime):
                            input_sizes.append(cx_Oracle.TIMESTAMP)
                        elif isinstance(val, str) and len(val) > 4000: # CLOB 처리
                            input_sizes.append(cx_Oracle.CLOB)
                        else:
                            input_sizes.append(None)
                    cursor.setinputsizes(*input_sizes)
                except: pass

                try:
                    # 1. Try Batch Execution
                    cursor.executemany(merge_sql, rows)
                except cx_Oracle.Error:
                    # 2. Fallback to Single Row Execution on Error
                    connection.rollback()
                    for row in rows:
                        try:
                            cursor.execute(merge_sql, row)
                        except Exception as e:
                            num_errors += 1
                            logging.error(f"[{table_name}] Merge Error: {e} | Row: {row}")
            
            if num_errors < len(rows):
                connection.commit()
            return len(rows) - num_errors, num_errors

        except Exception as e:
            logging.error(f"[{table_name}] Fatal Merge Error: {e}", exc_info=True)
            # logging.error(f"[{table_name}] Failed SQL: {merge_sql}")
            connection.rollback()
            raise

    elif is_oracle:
        # Oracle Insert (No PK)
        binds = ", ".join([f":{i+1}" for i in range(len(columns))])
        quoted_cols = ", ".join([f"\"{c}\"" for c in columns])
        sql = f"INSERT INTO {table_name} ({quoted_cols}) VALUES ({binds})"
        try:
            with connection.cursor() as cursor:
                try:
                    input_sizes = []
                    for val in rows[0]:
                        if isinstance(val, datetime):
                            input_sizes.append(cx_Oracle.TIMESTAMP)
                        elif isinstance(val, str) and len(val) > 4000:
                            input_sizes.append(cx_Oracle.CLOB)
                        else:
                            input_sizes.append(None)
                    cursor.setinputsizes(*input_sizes)
                except: pass

                try:
                    cursor.executemany(sql, rows, batcherrors=True) # 성능을 위해 적용
                    for error in cursor.getbatcherrors():
                        num_errors += 1
                        logging.warning(f"[{table_name}] Batch Insert Error (Row {error.offset}): {error.message}")
                except cx_Oracle.Error as e:
                    logging.warning(f"[{table_name}] Batch Insert Failed (Fallback to row-by-row): {e}")
                    connection.rollback()
                    for row in rows:
                        try: cursor.execute(sql, row)
                        except Exception as e:
                            num_errors += 1 # Likely ORA-00001
            
            if num_errors < len(rows):
                connection.commit()
            return len(rows) - num_errors, 0
        except Exception as e:
            logging.error(f"[{table_name}] Fatal Insert Error: {e}", exc_info=True)
            connection.rollback()
            raise
    
    else:
        # Non-Oracle Logic (SQLite)
        binds = ", ".join(['?' for _ in columns])
        if p_keys:
            pk_c = ", ".join(p_keys)
            up_s = ", ".join([f"{c}=excluded.{c}" for c in columns if c not in p_keys])
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({binds}) ON CONFLICT({pk_c}) DO UPDATE SET {up_s}" if up_s else f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({binds}) ON CONFLICT({pk_c}) DO NOTHING"
        else:
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({binds})"
        
        try:
            cursor = connection.cursor()
            cursor.executemany(sql, rows)
            cursor.close()
            connection.commit()
            return len(rows), 0
        except Exception as e:
            logging.error(f"[{table_name}] Fatal Load Error: {e}", exc_info=True)
            connection.rollback()
            return 0, len(rows)

def migrate(table_name, p_keys, date_column_name, fetchsize, start_date, end_date, source_conn, target_conn, update_callback=None, hint_index_column=None, index_scan_gap_minutes=0):
    logging.debug(f"[{table_name}] 기간: {start_date} ~ {end_date}")
    logging.info(f"[{table_name}] Migration start. Fetching target columns...")
    total_rows = 0
    total_err = 0
    max_ts = start_date

    try:
        # Schema Matching & Optimization
        target_cols = get_table_columns(target_conn, table_name)
        if target_cols:
            logging.info(f"[{table_name}] Target Schema: {target_cols}")
            
        if not target_cols:
            sel_clause = "*"
        else:
            sel_clause = ", ".join([f"\"{c[0]}\"" for c in target_cols])

        with source_conn.cursor() as s_cur:
            if cx_Oracle and hasattr(source_conn, 'username'):
                s_cur.outputtypehandler = OutputTypeHandler

            # ORA-01745 fix: Positional binding
            where = f"\"{date_column_name}\" >= :1 AND \"{date_column_name}\" < :2"
            params = [start_date, end_date]

            if hint_index_column:
                idx_start = start_date - timedelta(minutes=index_scan_gap_minutes)
                where += f" AND \"{hint_index_column}\" >= :3"
                params.append(idx_start)
            
            # Quote table name
            query = f"SELECT {sel_clause} FROM \"{table_name}\" WHERE {where} ORDER BY \"{date_column_name}\""
            
            if "." in table_name:
                parts = table_name.split(".")
                quoted_table = ".".join([f"\"{p}\"" for p in parts])
                query = query.replace(f"\"{table_name}\"", quoted_table)

            s_cur.arraysize = fetchsize
            
            # 쿼리 실행 타임아웃 설정 (60초) - 무한 대기 방지
            if cx_Oracle:
                try:
                    # cx_Oracle 8.2+ 지원
                    s_cur.connection.callTimeout = 60000
                except AttributeError:
                    try:
                         # Older versions might support execution options or just ignore
                         s_cur.callTimeout = 60000
                    except AttributeError:
                        logging.warning(f"[{table_name}] cx_Oracle 버전이 낮아 Query Timeout 설정을 실패했습니다.")
            
            logging.info(f"[{table_name}] Source Query: {query}")
            logging.info(f"[{table_name}] Source Params: {params}")
            
            s_cur.execute(query, params)
            
            cols = [d[0] for d in s_cur.description]
            try: date_idx = cols.index(date_column_name)
            except: 
                col_map = {c.upper(): i for i, c in enumerate(cols)}
                date_idx = col_map[date_column_name.upper()]

            while True:
                t1 = time.monotonic()
                logging.debug(f"[{table_name}] Fetching next chunk (Size: {fetchsize})...")
                rows = s_cur.fetchmany(fetchsize)
                logging.debug(f"[{table_name}] Fetched {len(rows)} rows.")
                t2 = time.monotonic()
                if not rows: break
                
                cur_max = max(r[date_idx] for r in rows)
                
                t3 = time.monotonic()
                # Restore original signature (no target_col_defs)
                suc, err = _load_data_merge(target_conn, table_name, p_keys, cols, rows)
                t4 = time.monotonic()
                
                total_rows += suc
                total_err += err
                
                logging.info(f"[{table_name}] {len(rows)}건 (Get:{t2-t1:.2f}s Put:{t4-t3:.2f}s) -> 성공:{suc} 실패:{err}")
                
                if err > 0: break
                if cur_max > max_ts:
                    max_ts = cur_max
                    if update_callback: update_callback(max_ts)
                    
        return {"processed": total_rows, "errors": total_err, "max_ts": max_ts}

    except Exception as e:
        logging.error(f"[{table_name}] Migration Failed: {e}", exc_info=True)
        target_conn.rollback()
        raise