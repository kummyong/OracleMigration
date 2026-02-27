# realtime_sync_service.py
import logging
import json
import time
import threading
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from logging.handlers import RotatingFileHandler
import copy
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from config import (
    SOURCE_DB_CONFIG,
    TARGET_DB_CONFIG,
    TABLES_CONFIG_FILE,
    DASHBOARD_FILE,
)

# --- 서비스 전역 설정 파일 --- #
SERVICE_CONFIG_FILE = 'service_config.json'

def load_service_config():
    if os.path.exists(SERVICE_CONFIG_FILE):
        try:
            with open(SERVICE_CONFIG_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return {"service_running": False, "sync_interval": 30}

def save_service_config(config):
    try:
        with open(SERVICE_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save service config: {e}")

# 초기값 로드
_s_config = load_service_config()
SERVICE_RUNNING = _s_config.get("service_running", False)
SYNC_INTERVAL_SECONDS = _s_config.get("sync_interval", 30)
# ----------------------------- #

from database import get_db_connection, get_upsert_keys, get_session_pool
from migration_utils import migrate, get_last_sync_time, update_last_sync_time
from dashboard_generator import generate_html_dashboard

# --- 서비스 설정 --- #
SYNC_INTERVAL_SECONDS = 30
MAX_WORKERS = 10
FETCH_SIZE = 10000
MAX_CONSECUTIVE_FAILURES = 3  # 연속 실패 허용 횟수
SAFETY_MARGIN_SECONDS = 60  # 안전 마진 (1분) - 커밋 지연으로 인한 데이터 누락 방지 (기존 300초에서 단축)
SKIP_SOURCE_VALIDATION = True  # 소스 DB 설정 검증 과정을 건너뛸지 여부

# -------------------- #

# 스크립트의 절대 경로를 기준으로 파일 경로 설정
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DASHBOARD_ABS_PATH = os.path.join(SCRIPT_DIR, DASHBOARD_FILE)


def setup_logging():
    """로그 설정을 초기화합니다. 콘솔과 파일에 모두 로그를 남깁니다."""
    log_dir = os.path.join(SCRIPT_DIR, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_format = "%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"

    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 기존 핸들러 제거 (재호출 시 중복 방지)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(console_handler)

    # 파일 핸들러 (날짜 기반, 로테이팅)
    log_file = os.path.join(
        log_dir, f"sync_service_{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    # 10MB ?ш린, 5媛?諛깆뾽
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8-sig"
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)


# 스레드간 공유 데이터 보호를 위한 Lock
file_update_lock = threading.Lock()  # 동기화 시간 파일용
cycle_status_lock = threading.Lock()  # 대시보드 데이터용

# 대시보드용 데이터를 담을 공유 딕셔너리
cycle_status = {"tables": {}}

# API 서버를 위한 핸들러
class DashboardAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global SERVICE_RUNNING, SYNC_INTERVAL_SECONDS
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/toggle':
            query = parse_qs(parsed_path.query)
            table_name = query.get('table', [None])[0]
            action = query.get('action', [None])[0]
            
            if table_name and action:
                with cycle_status_lock:
                    if table_name in cycle_status["tables"]:
                        new_state = (action == 'resume')
                        cycle_status["tables"][table_name]["enabled"] = new_state
                        if not new_state:
                            cycle_status["tables"][table_name]["status"] = "paused"
                            cycle_status["tables"][table_name]["message"] = "사용자에 의해 중지됨"
                        else:
                            cycle_status["tables"][table_name]["status"] = "pending"
                            cycle_status["tables"][table_name]["message"] = "재개됨 (대기 중)"
                        
                        logging.info(f"[API] Table {table_name} set to {'ENABLED' if new_state else 'DISABLED'}")
                        
                        # 1. JSON 설정 파일에 영구 저장 (Persistence)
                        try:
                            with open(TABLES_CONFIG_FILE, 'r', encoding='utf-8-sig') as f:
                                configs = json.load(f)
                            for cfg in configs:
                                if cfg['table_name'] == table_name:
                                    cfg['enabled'] = new_state
                                    break
                            with open(TABLES_CONFIG_FILE, 'w', encoding='utf-8-sig') as f:
                                json.dump(configs, f, indent=4, ensure_ascii=False)
                        except Exception as e:
                            logging.error(f"[API] 설정 파일 업데이트 실패: {e}")

                        # 2. 대시보드 갱신을 위해 데이터 복사
                        status_copy = copy.deepcopy(cycle_status)
                        
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({"success": True}).encode())
                        
                        # 락 밖에서 대시보드 갱신
                        generate_html_dashboard(status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING)
                        return
            
            self.send_response(400)
            self.end_headers()
            self.wfile.write(json.dumps({"success": False, "message": "Invalid parameters"}).encode())
            return
        
        elif parsed_path.path == '/api/global':
            query = parse_qs(parsed_path.query)
            action = query.get('action', [None])[0]
            
            if action:
                if action == 'start_all':
                    SERVICE_RUNNING = True
                    logging.info("[API] 전체 서비스 시작 명령 수신")
                elif action == 'stop_all':
                    SERVICE_RUNNING = False
                    logging.info("[API] 전체 서비스 중지 명령 수신")
                
                # 설정 저장
                save_service_config({"service_running": SERVICE_RUNNING, "sync_interval": SYNC_INTERVAL_SECONDS})
                
                with cycle_status_lock:
                    status_copy = copy.deepcopy(cycle_status)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"success": True}).encode())
                
                generate_html_dashboard(status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING)
                return

        elif parsed_path.path == '/api/set_interval':
            query = parse_qs(parsed_path.query)
            new_val = query.get('value', [None])[0]
            
            if new_val and new_val.isdigit():
                SYNC_INTERVAL_SECONDS = int(new_val)
                logging.info(f"[API] 동기화 주기 변경: {SYNC_INTERVAL_SECONDS}초")
                
                # 설정 저장
                save_service_config({"service_running": SERVICE_RUNNING, "sync_interval": SYNC_INTERVAL_SECONDS})
                
                with cycle_status_lock:
                    status_copy = copy.deepcopy(cycle_status)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"success": True}).encode())
                
                generate_html_dashboard(status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING)
                return
            
        # 기본 대시보드 파일 서비스
        if parsed_path.path == '/' or parsed_path.path == '/dashboard.html':
            try:
                with open(DASHBOARD_ABS_PATH, 'rb') as f:
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(f.read())
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

def start_api_server(port=8081):
    server = HTTPServer(('127.0.0.1', port), DashboardAPIHandler)
    logging.info(f"API 서버가 포트 {port}에서 127.0.0.1로 시작되었습니다.")
    server.serve_forever()

def load_table_configs():
    """JSON 설정 파일에서 동기화할 테이블 목록을 로드합니다."""
    try:
        with open(TABLES_CONFIG_FILE, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 설정 파일을 찾을 수 없습니다.")
        return None
    except json.JSONDecodeError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 파일의 형식이 잘못되었습니다.")
        return None


def enrich_table_configs(configs):
    """테이블 설정에 PK/UK 정보가 없으면 DB에서 조회하여 채워넣습니다."""
    # 1. 모든 테이블 설정을 보강 대상으로 함 (중지된 테이블도 대시보드 표시 및 재개를 위해 필요)
    active_configs = configs

    # 2. 추가 조사 필요 여부 판단
    needs_enrichment = False
    if not SKIP_SOURCE_VALIDATION:
        needs_enrichment = True
    else:
        # SKIP 모드라도 하나라도 PK가 없으면 연결 해야 함
        for config in active_configs:
            if not config.get("primary_keys"):
                needs_enrichment = True
                logging.info(f"[{config['table_name']}] PK 설정이 없는 테이블이 있어 DB 조회가 필요합니다.")
                break
    
    if not needs_enrichment:
        logging.info("소스 DB 검증 과정을 생략합니다. (SKIP_SOURCE_VALIDATION=True 또는 모든 PK 존재)")
        return active_configs

    enriched_configs = []
    source_conn = None
    try:
        logging.info("테이블 설정 보강을 위해 소스 DB에 연결합니다...")
        source_conn = get_db_connection(SOURCE_DB_CONFIG)
        for config in active_configs:
            table_name = config["table_name"]
            # primary_keys가 설정 파일에 명시되어 있지 않으면 DB에서 조회 시도
            if not config.get("primary_keys"):
                upsert_keys = get_upsert_keys(source_conn, table_name)
                if upsert_keys:
                    config["primary_keys"] = upsert_keys
                else:
                    logging.warning(
                        f"[{table_name}] Primary Key 또는 Unique Key를 찾을 수 없습니다. (PK 없이 INSERT 모드로 진행)"
                    )
                    config["primary_keys"] = []  # PK/UK가 없으면 빈 리스트로 설정

            enriched_configs.append(config)

    except Exception as e:
        logging.critical(f"테이블 설정 보강 중 오류: {e}", exc_info=True)
        return None
    finally:
        if source_conn:
            source_conn.close()
    return enriched_configs


def acquire_connection_with_retry(pool, db_label, max_retries=3):
    """
    세션 풀에서 연결을 가져오고, 유효성을 검사(ping)합니다.
    연결이 끊어진 경우 재시도합니다.
    """
    last_exception = None
    for attempt in range(max_retries):
        conn = None
        try:
            logging.debug(
                f"[{db_label}] 세션 풀 연결 요청 중... (시도 {attempt+1}/{max_retries})"
            )
            conn = pool.acquire()
            logging.debug(f"[{db_label}] 세션 풀 연결 획득 성공. Ping 테스트 진행...")
            # 가져온 연결이 유효한지 확인 (Ping)
            conn.ping()
            return conn
        except Exception as e:
            last_exception = e
            logging.warning(
                f"[{db_label}] 세션 풀 연결 획득/검증 실패 (시도 {attempt+1}/{max_retries}): {e}"
            )
            if conn:
                try:
                    # 유효하지 않은 연결은 폐기
                    pool.drop(conn)
                except Exception:
                    pass
            # 잠시 대기 후 재시도
            time.sleep(1)

    logging.error(f"[{db_label}] 세션 풀에서 유효한 연결을 가져오는데 실패했습니다.")
    raise last_exception


def sync_single_table(
    table_config, sync_start_time, source_pool=None, target_pool=None
):
    """단일 테이블 동기화 작업을 수행하고, 대시보드용 상태를 업데이트합니다."""
    table_name = table_config["table_name"]
    start_op_time = time.monotonic()

    # 초기에 상태를 '진행중'으로 설정
    logging.info(f"[{table_name}] Worker 시작 - 초기 상태 설정 중")
    with cycle_status_lock:
        current_status = cycle_status["tables"][table_name]
        last_sync_init = get_last_sync_time(table_name, file_update_lock)
        current_status.update(
            {
                "status": "in_progress",
                "message": "작업 시작",
                "last_sync_time": last_sync_init.strftime("%Y-%m-%d %H:%M:%S"),
                "consecutive_failures": current_status.get("consecutive_failures", 0),
            }
        )

    source_conn, target_conn = None, None
    
    # [Retry Logic] 변수 초기화
    max_retries = 3
    retry_count = 0
    current_sync_start_date = None # 재시도 시 재개할 시작점

    try:
        # 1. 초기 동기화 시간 계산
        last_sync = get_last_sync_time(table_name, file_update_lock)
        logging.info(f"[{table_name}] Previous sync time: {last_sync.isoformat()}")
        
        # 안전 마진 적용
        start_date = last_sync - timedelta(seconds=SAFETY_MARGIN_SECONDS)
        logging.info(f"[{table_name}] 안전 마진 {SAFETY_MARGIN_SECONDS}초 적용: {last_sync.isoformat()}부터가 아닌 {start_date.isoformat()}부터 스캔합니다.")
        logging.info(f"[{table_name}] Current cycle window end: {sync_start_time.isoformat()}")
        
        current_sync_start_date = start_date

        while retry_count <= max_retries:
            try:
                # 2. DB 연결 획득 (없는 경우에만)
                if not source_conn:
                    if source_pool:
                        logging.info(f"[{table_name}] Source DB 연결 획득 시도... (Retry: {retry_count})")
                        source_conn = acquire_connection_with_retry(source_pool, "Source")
                    else:
                        source_conn = get_db_connection(SOURCE_DB_CONFIG)
                
                if not target_conn:
                    if target_pool:
                        logging.info(f"[{table_name}] Target DB 연결 획득 시도... (Retry: {retry_count})")
                        target_conn = acquire_connection_with_retry(target_pool, "Target")
                    else:
                        target_conn = get_db_connection(TARGET_DB_CONFIG)

                logging.info(f"[{table_name}] DB 연결 획득 완료. 마이그레이션 시작 (Start: {current_sync_start_date})...")

                # 3. 진행 상황 콜백 (재시도 시 이어서 하기 위해 current_sync_start_date 업데이트)
                def _sync_progress_callback(progress_info):
                    nonlocal current_sync_start_date
                    # 성공적으로 처리된 최신 슬라이스 끝 지점을 다음 시작점으로 기록
                    # 주의: slice_end는 exclusive하므로 그대로 다음 start로 사용 가능
                    current_sync_start_date = progress_info["slice_end"]
                    
                    with cycle_status_lock:
                        start_str = progress_info["slice_start"].strftime("%H:%M")
                        end_str = progress_info["slice_end"].strftime("%H:%M")
                        proc_count = progress_info["processed"]
                        
                        current_status.update({
                            "message": f"[{start_str}~{end_str}] {proc_count:,}건 처리 중 (Retry: {retry_count})",
                            "processed": proc_count,
                            "last_sync_time": progress_info["current_max_ts"].strftime('%Y-%m-%d %H:%M:%S')
                        })

                # 4. 마이그레이션 수행
                result = migrate(
                    table_name=table_name,
                    p_keys=table_config["primary_keys"],
                    date_column_name=table_config["date_column"],
                    fetchsize=FETCH_SIZE,
                    start_date=current_sync_start_date, # 재시도 시 업데이트된 시간부터
                    end_date=sync_start_time,
                    source_conn=source_conn,
                    target_conn=target_conn,
                    update_callback=_sync_progress_callback,
                    hint_index_column=table_config.get("hint_index_column"),
                    index_scan_gap_minutes=table_config.get("index_scan_gap_minutes", 0),
                    hint=table_config.get("hint"),
                )

                # 5. 성공 시 처리
                with cycle_status_lock:
                    current_status = cycle_status["tables"][table_name]
                    new_last_sync_time = result["max_ts"]
                    current_status["last_sync_time"] = new_last_sync_time.strftime("%Y-%m-%d %H:%M:%S")

                    if result["errors"] == 0:
                        update_last_sync_time(table_name, new_last_sync_time, file_update_lock)
                        logging.info(f"[{table_name}] New sync time from data: {new_last_sync_time.isoformat()}")

                        current_status.update({
                            "status": "success" if result["processed"] > 0 else "no_data",
                            "processed": result["processed"],
                            "errors": 0,
                            "message": (f"성공 (처리: {result['processed']})" if result["processed"] > 0 else "변경 데이터 없음"),
                            "consecutive_failures": 0,
                        })
                    else:
                         # 데이터 오류 (연결 오류 아님)
                         current_status["consecutive_failures"] += 1
                         logging.warning(f"[{table_name}] 데이터 처리 중 오류 발생. 연속 실패: {current_status['consecutive_failures']}")
                         
                         if current_status["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
                             logging.critical(f"[{table_name}] 최대 연속 실패 도달. 해당 구간 건너뜀.")
                             update_last_sync_time(table_name, sync_start_time, file_update_lock)
                             current_status.update({
                                 "status": "persistent_failure",
                                 "processed": result["processed"],
                                 "errors": result["errors"],
                                 "message": f"영구 실패: {result['errors']}건 오류. 구간 건너뜀.",
                                 "last_sync_time": sync_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                 "consecutive_failures": 0,
                             })
                         else:
                             current_status.update({
                                 "status": "failed",
                                 "processed": result["processed"],
                                 "errors": result["errors"],
                                 "message": f"일시적 실패: {result['errors']}건 오류. 재시도 예정.",
                             })
                
                # 성공적으로 마쳤으므로 루프 탈출
                break 

            except Exception as e:
                # 예외 처리 및 재시도 판단
                error_str = str(e)
                is_connection_error = "DPI-1010" in error_str or "DPI-1080" in error_str or "ORA-03113" in error_str or "ORA-03114" in error_str
                
                if is_connection_error:
                    retry_count += 1
                    logging.warning(f"[{table_name}] 연결 오류 감지 (DPI-1010/Timeout 등): {e}. 재시도 {retry_count}/{max_retries}...")
                    
                    # 연결 폐기
                    try:
                        if source_pool and source_conn: source_pool.drop(source_conn)
                        if target_pool and target_conn: target_pool.drop(target_conn)
                    except: pass
                    
                    source_conn = None
                    target_conn = None
                    
                    if retry_count > max_retries:
                        logging.error(f"[{table_name}] 최대 재시도 횟수 초과. 작업을 중단합니다.")
                        raise # 최종 실패 처리
                        
                    # 잠시 대기 후 재시도
                    time.sleep(5)
                else:
                    # 연결 오류가 아닌 일반 오류는 즉시 실패 처리
                    raise

    except Exception as e:
        error_message = f"오류 발생: {str(e)}"
        logging.error(f"[{table_name}] 작업 실패. {error_message}", exc_info=True)
        with cycle_status_lock:
            cycle_status["tables"][table_name].update(
                {"status": "failed", "message": error_message}
            )
    finally:
        # 풀에서 가져온 경우 release, 직접 연결한 경우 close
        # (재시도 로직에서 이미 drop된 경우 None일 수 있음)
        try:
            if source_pool and source_conn:
                source_pool.release(source_conn)
            elif source_conn:
                source_conn.close()

            if target_pool and target_conn:
                target_pool.release(target_conn)
            elif target_conn:
                target_conn.close()
        except Exception as e:
            logging.warning(f"[{table_name}] 연결 해제 중 오류 (무시됨): {e}")

        duration = time.monotonic() - start_op_time
        with cycle_status_lock:
            cycle_status["tables"][table_name]["duration"] = duration

        logging.info(f"[{table_name}] 테이블 작업 종료.")


def main_service_loop():
    """메인 서비스 루프. 주기적으로 모든 테이블의 동기화를 트리거하고 대시보드를 생성합니다."""
    logging.info("실시간 동기화 서비스를 시작합니다.")

    table_configs = load_table_configs()
    if not table_configs:
        return

    enriched_configs = enrich_table_configs(table_configs)
    if not enriched_configs:
        logging.critical("동기화할 유효한 테이블 설정이 없습니다. 서비스를 종료합니다.")
        return

    logging.info(f"--- 총 {len(enriched_configs)}개 테이블 동기화 준비 완료 ---")
    for cfg in enriched_configs:
        logging.info(
            f"  - 테이블: {cfg['table_name']}, Upsert Keys: {cfg['primary_keys']}"
        )
    logging.info("-----------------------------------------")

    # cycle_status의 초기 상태를 루프 밖에서 설정 (활성화된 테이블만 포함)
    with cycle_status_lock:
        cycle_status["tables"] = {}
        for config in enriched_configs:
            is_enabled = config.get("enabled", True)
            cycle_status["tables"][config["table_name"]] = {
                "status": "pending" if is_enabled else "paused",
                "message": "대기" if is_enabled else "사용자에 의해 중지됨",
                "enabled": is_enabled,
                "consecutive_failures": 0,
            }

    # 세션 풀 초기화 (Oracle의 경우 효율적인 연결 관리를 위함)
    source_pool = None
    target_pool = None
    try:
        # 교착 상태(Deadlock) 방지를 위해 세션 풀 크기를 워커 수보다 넉넉하게 설정 (+3)
        pool_size = MAX_WORKERS + 3
        source_pool = get_session_pool(SOURCE_DB_CONFIG, min_conn=2, max_conn=pool_size)
        target_pool = get_session_pool(TARGET_DB_CONFIG, min_conn=2, max_conn=pool_size)

        # API 서버 시작 (세션 풀 생성 성공 후 시작)
        api_thread = threading.Thread(target=start_api_server, kwargs={'port': 8081}, daemon=True)
        api_thread.start()

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS, thread_name_prefix="SyncWorker"
        ) as executor:
            while True:
                cycle_start_time = datetime.now()
                logging.info(
                    f"--- 동기화 사이클 시작: {cycle_start_time.isoformat()} (활성 스레드: {threading.active_count()}) ---"
                )

                # 1. 대시보드 데이터 상태를 '진행중'으로 업데이트
                with cycle_status_lock:
                    cycle_status["last_updated"] = cycle_start_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    for config in enriched_configs:
                        t_name = config["table_name"]
                        if cycle_status["tables"][t_name].get("enabled", True):
                            cycle_status["tables"][t_name].update(
                                {"status": "in_progress", "message": "작업 시작 중..."}
                            )
                    # 락 안에서 데이터를 복사
                    status_copy = copy.deepcopy(cycle_status)
                
                # 서비스 중지 상태이면 동기화 로직 건너뜀
                if not SERVICE_RUNNING:
                    logging.info("--- 서비스 중지 상태: 다음 사이클까지 대기합니다. ---")
                    generate_html_dashboard(status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING)
                    time.sleep(SYNC_INTERVAL_SECONDS)
                    continue

                # 락 밖에서 대시보드 생성
                logging.info("[Main] 사이클 초기 대시보드 생성 중...")
                generate_html_dashboard(
                    status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING
                )
                logging.info("[Main] 사이클 초기 대시보드 생성 완료.")

                # 2. 동기화 작업 제출 (세션 풀 전달)
                futures = []
                with cycle_status_lock: # 락 범위 축소 권장되나 여기선 일관성을 위해 유지
                    for config in enriched_configs:
                        t_name = config["table_name"]
                        if cycle_status["tables"][t_name].get("enabled", True):
                            futures.append(
                                executor.submit(
                                    sync_single_table,
                                    config,
                                    cycle_start_time,
                                    source_pool,
                                    target_pool,
                                )
                            )
                        else:
                            logging.info(f"[{t_name}] 중지 상태이므로 이번 사이클에서 제외합니다.")

                # 3. 모든 작업이 완료될 때까지 주기적으로 대시보드 갱신하며 기다림
                logging.info(
                    f"모든 테이블의 동기화 작업이 완료될 때까지 기다립니다... (현재 활성 스레드: {threading.active_count()})"
                )

                done, not_done = concurrent.futures.wait(futures, timeout=10)
                while not_done:
                    # 진행 중인 상태를 대시보드에 반영
                    with cycle_status_lock:
                        status_copy = copy.deepcopy(cycle_status)
                        
                        # 실제 진행 중인 테이블명 추출
                        running_tables = [
                            name for name, info in cycle_status["tables"].items() 
                            if info.get("status") == "in_progress"
                        ]
                    
                    generate_html_dashboard(status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING)
                    
                    logging.info(f"동기화 진행 중인 테이블({len(running_tables)}개): {running_tables}")
                    logging.info(f"대기 중... (미완료 작업: {len(not_done)}, 전체 활성 스레드: {threading.active_count()})")
                    
                    # 10초 후에 다시 확인
                    done, not_done = concurrent.futures.wait(futures, timeout=10)

                logging.info(
                    f"모든 동기화 작업이 완료되었습니다. (현재 활성 스레드: {threading.active_count()})"
                )

                # 사이클 소요 시간 계산
                cycle_end_time = datetime.now()
                cycle_duration = (cycle_end_time - cycle_start_time).total_seconds()

                # 5. 최종 집계 및 대시보드 업데이트
                with cycle_status_lock:
                    cycle_status["cycle_duration"] = cycle_duration
                    updated_tables_count = 0
                    for table_status in cycle_status["tables"].values():
                        # 성공적으로 처리된 데이터가 있거나, 영구 실패로 건너뛴 경우 '업데이트'로 간주
                        if (
                            table_status.get("status") == "success"
                            or table_status.get("status") == "persistent_failure"
                        ):
                            updated_tables_count += 1

                    cycle_status["updated_tables_count"] = updated_tables_count
                    status_copy = copy.deepcopy(cycle_status)
                
                generate_html_dashboard(
                    status_copy, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH, SERVICE_RUNNING
                )

                logging.info(
                    f"사이클 완료. 다음 사이클까지 {SYNC_INTERVAL_SECONDS}초 대기합니다."
                )
                time.sleep(SYNC_INTERVAL_SECONDS)
    finally:
        if source_pool:
            source_pool.close()
        if target_pool:
            target_pool.close()
        logging.info("데이터베이스 세션 풀이 종료되었습니다.")


if __name__ == "__main__":
    setup_logging()
    try:
        main_service_loop()
    except KeyboardInterrupt:
        logging.info("사용자에 의해 서비스가 중지되었습니다.")
    except Exception as e:
        logging.critical(f"서비스 실행 중 심각한 오류 발생: {e}", exc_info=True)
