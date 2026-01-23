# realtime_sync_service.py
import logging
import json
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, TABLES_CONFIG_FILE, DASHBOARD_FILE
from database import get_db_connection, get_primary_keys
from migration_utils import migrate, get_last_sync_time, update_last_sync_time
from dashboard_generator import generate_html_dashboard

# --- 서비스 설정 --- #
SYNC_INTERVAL_SECONDS = 10 
MAX_WORKERS = 10
FETCH_SIZE = 1000
# -------------------- #

log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)

# 스레드간 공유 데이터 보호를 위한 Lock
file_update_lock = threading.Lock() # 동기화 시간 파일용
cycle_status_lock = threading.Lock() # 대시보드 데이터용

# 대시보드용 데이터를 담을 공유 딕셔너리
cycle_status = {
    "tables": {}
}

def load_table_configs():
    """JSON 설정 파일에서 동기화할 테이블 목록을 로드합니다."""
    try:
        with open(TABLES_CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 설정 파일을 찾을 수 없습니다.")
        return None
    except json.JSONDecodeError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 파일의 형식이 잘못되었습니다.")
        return None

def enrich_table_configs(configs):
    """테이블 설정에 PK 정보가 없으면 DB에서 조회하여 채워넣습니다. PK가 없으면 빈 리스트로 설정합니다."""
    enriched_configs = []
    source_conn = None
    try:
        logging.info("테이블 설정 보강을 위해 소스 DB에 연결합니다...")
        source_conn = get_db_connection(SOURCE_DB_CONFIG)
        for config in configs:
            table_name = config['table_name']
            # PK가 설정 파일에 명시되어 있지 않으면 DB에서 조회 시도
            if not config.get('primary_keys'):
                pk_columns = get_primary_keys(source_conn, table_name)
                if pk_columns:
                    config['primary_keys'] = pk_columns
                else:
                    logging.warning(f"[{table_name}] Primary Key를 찾을 수 없습니다. (PK 없이 INSERT 모드로 진행)")
                    config['primary_keys'] = [] # PK가 없으면 빈 리스트로 설정
            
            # PK 유무와 관계없이 모든 테이블을 동기화 대상에 추가
            enriched_configs.append(config)
            
    except Exception as e:
        logging.critical(f"테이블 설정 보강 중 오류: {e}", exc_info=True)
        return None
    finally:
        if source_conn:
            source_conn.close()
    return enriched_configs

def sync_single_table(table_config, sync_start_time):
    """단일 테이블 동기화 작업을 수행하고, 대시보드용 상태를 업데이트합니다."""
    table_name = table_config['table_name']
    start_op_time = time.monotonic()
    
    status_update = {"status": "in_progress", "message": "작업 시작"}
    with cycle_status_lock:
        cycle_status["tables"][table_name] = status_update

    source_conn, target_conn = None, None
    try:
        last_sync = get_last_sync_time(table_name)
        logging.info(f"[{table_name}] Previous sync time: {last_sync.isoformat()}")
        logging.info(f"[{table_name}] Current cycle window end: {sync_start_time.isoformat()}")
        
        source_conn = get_db_connection(SOURCE_DB_CONFIG)
        target_conn = get_db_connection(TARGET_DB_CONFIG)

        result = migrate(
            table_name=table_name, p_keys=table_config['primary_keys'],
            date_column_name=table_config['date_column'], fetchsize=FETCH_SIZE,
            start_date=last_sync, end_date=sync_start_time,
            source_conn=source_conn, target_conn=target_conn
        )

        # 마이그레이션 결과에서 실제 처리된 데이터의 마지막 타임스탬프를 가져옴
        new_last_sync_time = result['max_ts']
        logging.info(f"[{table_name}] New sync time from data: {new_last_sync_time.isoformat()}")

        # 마지막 동기화 시간을 실제 데이터의 마지막 시간으로 업데이트
        update_last_sync_time(table_name, new_last_sync_time, file_update_lock)
        
        status_update.update({
            "status": "success" if result["processed"] > 0 else "no_data",
            "processed": result["processed"],
            "errors": result["errors"],
            "message": f"성공 (처리: {result['processed'] - result['errors']}, 오류: {result['errors']})" if result["processed"] > 0 else "변경 데이터 없음"
        })

    except Exception as e:
        error_message = f"오류 발생: {str(e)}"
        logging.error(f"[{table_name}] 작업 실패. {error_message}")
        status_update.update({"status": "failed", "message": error_message})
    finally:
        if source_conn: source_conn.close()
        if target_conn: target_conn.close()
        
        duration = time.monotonic() - start_op_time
        status_update["duration"] = duration
        
        with cycle_status_lock:
            cycle_status["tables"][table_name].update(status_update)
            generate_html_dashboard(cycle_status, SYNC_INTERVAL_SECONDS, DASHBOARD_FILE)

def main_service_loop():
    """메인 서비스 루프. 주기적으로 모든 테이블의 동기화를 트리거하고 대시보드를 생성합니다."""
    logging.info("실시간 동기화 서비스를 시작합니다.")
    
    table_configs = load_table_configs()
    if not table_configs: return

    enriched_configs = enrich_table_configs(table_configs)
    if not enriched_configs:
        logging.critical("동기화할 유효한 테이블 설정이 없습니다. 서비스를 종료합니다.")
        return

    logging.info(f"--- 총 {len(enriched_configs)}개 테이블 동기화 준비 완료 ---")
    for cfg in enriched_configs:
        logging.info(f"  - 테이블: {cfg['table_name']}, PK: {cfg['primary_keys']}")
    logging.info("-----------------------------------------")

    # cycle_status의 초기 상태를 루프 밖에서 설정
    with cycle_status_lock:
        for config in enriched_configs:
            cycle_status["tables"][config['table_name']] = {"status": "pending", "message": "대기"}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='SyncWorker') as executor:
        while True:
            cycle_start_time = datetime.now()
            logging.info(f"--- 동기화 사이클 시작: {cycle_start_time.isoformat()} ---")

            # 1. 대시보드 데이터 상태를 '진행중'으로 업데이트
            with cycle_status_lock:
                cycle_status["last_updated"] = cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')
                for config in enriched_configs:
                    cycle_status["tables"][config['table_name']].update({"status": "in_progress", "message": "작업 시작 중..."})
            
            # 2. '진행중' 상태의 대시보드를 생성
            generate_html_dashboard(cycle_status, SYNC_INTERVAL_SECONDS, DASHBOARD_FILE)

            # 3. 동기화 작업 제출
            futures = [executor.submit(sync_single_table, config, cycle_start_time) for config in enriched_configs]
            
            # 4. 모든 작업이 완료될 때까지 기다림
            logging.info("모든 테이블의 동기화 작업이 완료될 때까지 기다립니다...")
            concurrent.futures.wait(futures) # 모든 future가 완료될 때까지 블록킹
            logging.info("모든 동기화 작업이 완료되었습니다.")

            logging.info(f"사이클 완료. 다음 사이클까지 {SYNC_INTERVAL_SECONDS}초 대기합니다.")
            time.sleep(SYNC_INTERVAL_SECONDS)

if __name__ == '__main__':
    try:
        main_service_loop()
    except KeyboardInterrupt:
        logging.info("사용자에 의해 서비스가 중지되었습니다.")
    except Exception as e:
        logging.critical(f"서비스 실행 중 심각한 오류 발생: {e}", exc_info=True)

