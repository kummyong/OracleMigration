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

from config import (
    SOURCE_DB_CONFIG,
    TARGET_DB_CONFIG,
    TABLES_CONFIG_FILE,
    DASHBOARD_FILE,
)
from database import get_db_connection, get_upsert_keys, get_session_pool
from migration_utils import migrate, get_last_sync_time, update_last_sync_time
from dashboard_generator import generate_html_dashboard

# --- 서비스 설정 --- #
SYNC_INTERVAL_SECONDS = 10
MAX_WORKERS = 10
FETCH_SIZE = 10000
MAX_CONSECUTIVE_FAILURES = 3  # 연속 실패 허용 횟수
SAFETY_MARGIN_SECONDS = 300  # ?덉쟾 留덉쭊 (5遺? - 而ㅻ컠 吏???곗씠???꾨씫 諛⑹?)
SKIP_SOURCE_VALIDATION = True  # ?뚯뒪 DB ?ㅼ젙 寃??踰붾???吏?吏?

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
    # 10MB 크기, 5개 백업
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)


# 스레드간 공유 데이터 보호를 위한 Lock
file_update_lock = threading.Lock()  # 동기화 시간 파일용
cycle_status_lock = threading.Lock()  # 대시보드 데이터용

# 대시보드용 데이터를 담을 공유 딕셔너리
cycle_status = {"tables": {}}


def load_table_configs():
    """JSON 설정 파일에서 동기화할 테이블 목록을 로드합니다."""
    try:
        with open(TABLES_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 설정 파일을 찾을 수 없습니다.")
        return None
    except json.JSONDecodeError:
        logging.error(f"'{TABLES_CONFIG_FILE}' 파일의 형식이 잘못되었습니다.")
        return None


def enrich_table_configs(configs):
    """?뚯씠釉??ㅼ젙??PK/UK ?뺣낫媛 ?놁쑝硫?DB?먯꽌 議고쉶?섏뿬 梨꾩썙?ｌ뒿?덈떎."""
    # 1. ?ъ슜?섏? ?딅뒗 ?뚯씠釉?1李??꾪꽣留?
    active_configs = [c for c in configs if c.get("enabled", True) is not False]

    # 2. 異붽? 議곗궗 ?꾩슂 ?щ??먮떒
    needs_enrichment = False
    if not SKIP_SOURCE_VALIDATION:
        needs_enrichment = True
    else:
        # SKIP ?듦??? ?섎굹?쇰룄 PK媛 ?놁쑝硫??곌껐 ?댁빞 ??
        for config in active_configs:
            if not config.get("primary_keys"):
                needs_enrichment = True
                logging.info(f"[{config['table_name']}] PK ?ㅼ젙???꾪븳 DB 議고쉶媛??꾩슂?⑸땲??")
                break
    
    if not needs_enrichment:
        logging.info("?뚯뒪 DB 寃??怨쇱젙???앸왂?⑸땲?? (SKIP_SOURCE_VALIDATION=True or 紐⑤뱺 PK ?먯옱)")
        return active_configs

    enriched_configs = []
    source_conn = None
    try:
        logging.info("?뚯씠釉??ㅼ젙 蹂닿컯???꾪빐 ?뚯뒪 DB???곌껐?⑸땲??..")
        source_conn = get_db_connection(SOURCE_DB_CONFIG)
        for config in active_configs:
            table_name = config["table_name"]
            # primary_keys媛 ?ㅼ젙 ?뚯씪??紐낆떆?섏뼱 ?덉? ?딆쑝硫?DB?먯꽌 議고쉶 ?쒕룄
            if not config.get("primary_keys"):
                upsert_keys = get_upsert_keys(source_conn, table_name)
                if upsert_keys:
                    config["primary_keys"] = upsert_keys
                else:
                    logging.warning(
                        f"[{table_name}] Primary Key ?먮뒗 Unique Key瑜?李얠쓣 ???없?뒿?덈떎. (PK ?없?씠 INSERT 紐⑤뱶濡?吏꾪뻾)"
                    )
                    config["primary_keys"] = []  # PK/UK媛 ?없?쑝硫?鍮?由ъ뒪?몃줈 ?ㅼ젙

            enriched_configs.append(config)

    except Exception as e:
        logging.critical(f"?뚯씠釉??ㅼ젙 蹂닿컯 以??ㅻ쪟: {e}", exc_info=True)
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
    try:
        last_sync = get_last_sync_time(table_name, file_update_lock)
        logging.info(f"[{table_name}] Previous sync time: {last_sync.isoformat()}")
        
        # 안전 마진 적용: 커밋 지연으로 인한 누락 방지를 위해 5분 전부터 스캔
        start_date = last_sync - timedelta(seconds=SAFETY_MARGIN_SECONDS)
        logging.info(f"[{table_name}] 안전 마진 {SAFETY_MARGIN_SECONDS}초 적용: {last_sync.isoformat()}부터가 아닌 {start_date.isoformat()}부터 스캔합니다.")

        logging.info(
            f"[{table_name}] Current cycle window end: {sync_start_time.isoformat()}"
        )

        # 풀이 있으면 acquire, 없으면(SQLite 등) 신규 연결
        if source_pool:
            logging.info(f"[{table_name}] Source DB 연결 획득 시도...")
            source_conn = acquire_connection_with_retry(source_pool, "Source")
            logging.info(f"[{table_name}] Source DB 연결 획득 완료.")
        else:
            source_conn = get_db_connection(SOURCE_DB_CONFIG)

        if target_pool:
            logging.info(f"[{table_name}] Target DB 연결 획득 시도...")
            target_conn = acquire_connection_with_retry(target_pool, "Target")
            logging.info(f"[{table_name}] Target DB 연결 획득 완료.")
        else:
            target_conn = get_db_connection(TARGET_DB_CONFIG)

        logging.info(f"[{table_name}] DB 연결 획득 완료. 마이그레이션 시작...")

        # 진행 상황을 대시보드에 실시간으로 표시하기 위한 콜백
        def _sync_progress_callback(progress_info):
            with cycle_status_lock:
                start_str = progress_info["slice_start"].strftime("%H:%M")
                end_str = progress_info["slice_end"].strftime("%H:%M")
                proc_count = progress_info["processed"]
                
                # 대시보드 메시지 업데이트
                current_status.update({
                    "message": f"[{start_str}~{end_str}] {proc_count:,}건 처리 중",
                    "processed": proc_count,
                    "last_sync_time": progress_info["current_max_ts"].strftime('%Y-%m-%d %H:%M:%S')
                })

        result = migrate(
            table_name=table_name,
            p_keys=table_config["primary_keys"],
            date_column_name=table_config["date_column"],
            fetchsize=FETCH_SIZE,
            start_date=start_date,
            end_date=sync_start_time,
            source_conn=source_conn,
            target_conn=target_conn,
            update_callback=_sync_progress_callback,
            hint_index_column=table_config.get("hint_index_column"),
            index_scan_gap_minutes=table_config.get("index_scan_gap_minutes", 0),
        )

        with cycle_status_lock:
            current_status = cycle_status["tables"][table_name]
            new_last_sync_time = result["max_ts"]

            # 대시보드에 표시할 시간 업데이트 (오류 발생 시에도 처리된 지점까지 표시)
            current_status["last_sync_time"] = new_last_sync_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            if result["errors"] == 0:
                # 성공 시
                update_last_sync_time(table_name, new_last_sync_time, file_update_lock)
                logging.info(
                    f"[{table_name}] New sync time from data: {new_last_sync_time.isoformat()}"
                )

                current_status.update(
                    {
                        "status": "success" if result["processed"] > 0 else "no_data",
                        "processed": result["processed"],
                        "errors": 0,
                        "message": (
                            f"성공 (처리: {result['processed']})"
                            if result["processed"] > 0
                            else "변경 데이터 없음"
                        ),
                        "consecutive_failures": 0,  # 성공 시 카운터 리셋
                    }
                )
            else:
                # 오류 발생 시
                current_status["consecutive_failures"] += 1
                logging.warning(
                    f"[{table_name}] 오류 발생. 연속 실패 횟수: {current_status['consecutive_failures']}"
                )

                if current_status["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
                    # 최대 실패 횟수 도달 - 서킷 브레이커 작동
                    logging.critical(
                        f"[{table_name}] 최대 연속 실패 횟수({MAX_CONSECUTIVE_FAILURES})에 도달했습니다. "
                        f"문제가 되는 데이터 구간을 건너뛰고 다음 사이클부터 정상 진행을 시도합니다. "
                        f"마지막 동기화 시간을 현재 사이클 시간({sync_start_time.isoformat()})으로 강제 업데이트합니다."
                    )

                    # 동기화 시간을 강제로 현재 사이클 시간으로 업데이트하여 문제 구간을 건너뜀
                    update_last_sync_time(table_name, sync_start_time, file_update_lock)

                    current_status.update(
                        {
                            "status": "persistent_failure",
                            "processed": result[
                                "processed"
                            ],  # 이번 주기에 처리 시도한 내용
                            "errors": result["errors"],
                            "message": f"영구 실패: {result['errors']}개 오류. 데이터 구간을 건너뛰었습니다.",
                            "last_sync_time": sync_start_time.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "consecutive_failures": 0,  # 다음 정상 처리를 위해 리셋
                        }
                    )
                else:
                    # 아직 재시도 횟수가 남음
                    current_status.update(
                        {
                            "status": "failed",
                            "processed": result["processed"],
                            "errors": result["errors"],
                            "message": f"일시적 실패: {result['errors']}개 오류. 재시도 예정.",
                        }
                    )

        logging.info(f"[{table_name}] 테이블 작업 종료.")
    except Exception as e:
        error_message = f"오류 발생: {str(e)}"
        logging.error(f"[{table_name}] 작업 실패. {error_message}", exc_info=True)
        with cycle_status_lock:
            cycle_status["tables"][table_name].update(
                {"status": "failed", "message": error_message}
            )
    finally:
        # 풀에서 가져온 경우 release, 직접 연결한 경우 close
        if source_pool and source_conn:
            source_pool.release(source_conn)
        elif source_conn:
            source_conn.close()

        if target_pool and target_conn:
            target_pool.release(target_conn)
        elif target_conn:
            target_conn.close()

        duration = time.monotonic() - start_op_time
        with cycle_status_lock:
            cycle_status["tables"][table_name]["duration"] = duration
            # 개별 워커에서는 더 이상 대시보드를 직접 그리지 않음 (충돌 방지)

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
            if config.get("enabled", True) is not False:
                cycle_status["tables"][config["table_name"]] = {
                    "status": "pending",
                    "message": "대기",
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
                        cycle_status["tables"][config["table_name"]].update(
                            {"status": "in_progress", "message": "작업 시작 중..."}
                        )

                # 2. '진행중' 상태의 대시보드를 생성
                logging.info("[Main] 사이클 초기 대시보드 생성 중...")
                generate_html_dashboard(
                    cycle_status, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH
                )
                logging.info("[Main] 사이클 초기 대시보드 생성 완료.")

                # 3. 동기화 작업 제출 (세션 풀 전달)
                futures = [
                    executor.submit(
                        sync_single_table,
                        config,
                        cycle_start_time,
                        source_pool,
                        target_pool,
                    )
                    for config in enriched_configs
                ]

                # 4. 모든 작업이 완료될 때까지 주기적으로 대시보드 갱신하며 기다림
                logging.info(
                    f"모든 테이블의 동기화 작업이 완료될 때까지 기다립니다... (현재 활성 스레드: {threading.active_count()})"
                )

                done, not_done = concurrent.futures.wait(futures, timeout=10)
                while not_done:
                    # 진행 중인 상태를 대시보드에 반영
                    with cycle_status_lock:
                        generate_html_dashboard(cycle_status, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH)
                        
                        # 실제 진행 중인 테이블명 추출
                        running_tables = [
                            name for name, info in cycle_status["tables"].items() 
                            if info.get("status") == "in_progress"
                        ]
                    
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
                    generate_html_dashboard(
                        cycle_status, SYNC_INTERVAL_SECONDS, DASHBOARD_ABS_PATH
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
