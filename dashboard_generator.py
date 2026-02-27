# dashboard_generator.py
import logging
import os
from datetime import datetime

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="{refresh_interval}">
    <title>DB Sync Status</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background-color: #f4f7f9; color: #333; }}
        .container {{ max-width: 1200px; margin: 40px auto; background: white; padding: 25px 40px; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.05); }}
        .header-flex {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #e9ecef; margin-bottom: 20px; padding-bottom: 10px; }}
        h1 {{ color: #2c3e50; margin: 0; font-size: 2em; }}
        h2 {{ color: #2c3e50; font-size: 1.5em; margin-top: 30px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
        .global-controls {{ background-color: #fff; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px; display: flex; gap: 15px; align-items: center; }}
        .control-group {{ display: flex; align-items: center; gap: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px 15px; border: 1px solid #ddd; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: 600; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .status-success {{ color: #155724; background-color: #d4edda; }}
        .status-no_data {{ color: #555; }}
        .status-in_progress {{ color: #004085; background-color: #cce5ff; }}
        .status-failed {{ color: #721c24; background-color: #f8d7da; font-weight: bold; }}
        .status-persistent_failure {{ color: #fff; background-color: #dc3545; font-weight: bold; }}
        .status-paused {{ color: #856404; background-color: #fff3cd; }}
        .btn {{ padding: 8px 15px; border: none; border-radius: 4px; cursor: pointer; font-size: 0.9em; text-decoration: none; color: white; display: inline-block; font-weight: 500; }}
        .btn-stop {{ background-color: #dc3545; }}
        .btn-resume {{ background-color: #28a745; }}
        .btn-start-all {{ background-color: #007bff; }}
        .btn-stop-all {{ background-color: #6c757d; }}
        .btn-apply {{ background-color: #17a2b8; padding: 5px 10px; }}
        .btn:hover {{ opacity: 0.8; }}
        .footer {{ margin-top: 30px; font-size: 0.9em; color: #777; }}
        .footer ul {{ padding-left: 20px; }}
        .footer li {{ margin-bottom: 5px; }}
        .timestamp {{ font-weight: normal; font-size: 0.9em; color: #555; margin: 5px 0; }}
        .summary-box {{ background-color: #e9ecef; padding: 15px 20px; border-radius: 5px; margin-bottom: 25px; border: 1px solid #ced4da; }}
        .summary-box h2 {{ margin-top: 0; border-bottom: none; font-size: 1.2em; }}
        .summary-box p {{ margin: 5px 0 0; font-size: 1.1em; }}
        .service-status {{ font-weight: bold; padding: 3px 8px; border-radius: 4px; }}
        .status-running {{ color: #28a745; border: 1px solid #28a745; }}
        .status-stopped {{ color: #dc3545; border: 1px solid #dc3545; }}
        input[type="number"] {{ padding: 5px; border: 1px solid #ccc; border-radius: 4px; width: 60px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header-flex">
            <h1>DB 실시간 동기화 현황</h1>
            <div>
                <span class="service-status {service_status_class}">{service_status_text}</span>
            </div>
        </div>

        <div class="global-controls">
            <div class="control-group">
                <strong>전체 제어:</strong>
                <button class="btn btn-start-all" onclick="globalControl('start_all')">전체 시작</button>
                <button class="btn btn-stop-all" onclick="globalControl('stop_all')">전체 중지</button>
            </div>
            <div class="control-group" style="margin-left: auto;">
                <strong>동기화 주기:</strong>
                <input type="number" id="sync_interval_input" value="{refresh_interval}" min="5" max="3600"> 초
                <button class="btn btn-apply" onclick="setIntervalVal()">적용</button>
            </div>
        </div>
        
        <p class="timestamp">마지막 업데이트: {last_updated}</p>
        
        <script>
            async function toggleTable(tableName, action) {{
                if (!confirm(`테이블 ${{tableName}}을(를) ${{action === 'stop' ? '중지' : '재개'}}하시겠습니까?`)) return;
                try {{
                    const response = await fetch(`/api/toggle?table=${{tableName}}&action=${{action}}`);
                    const result = await response.json();
                    if (result.success) {{
                        location.reload();
                    }} else {{
                        alert('오류: ' + result.message);
                    }}
                }} catch (e) {{
                    alert('서버와 통신 중 오류가 발생했습니다.');
                }}
            }}

            async function globalControl(action) {{
                const msg = action === 'start_all' ? '전체 서비스를 시작하시겠습니까?' : '전체 서비스를 중지하시겠습니까?';
                if (!confirm(msg)) return;
                try {{
                    const response = await fetch(`/api/global?action=${{action}}`);
                    const result = await response.json();
                    if (result.success) {{
                        location.reload();
                    }}
                }} catch (e) {{
                    alert('서버와 통신 중 오류가 발생했습니다.');
                }}
            }}

            async function setIntervalVal() {{
                const val = document.getElementById('sync_interval_input').value;
                if (!val || val < 5) {{
                    alert('주기는 최소 5초 이상이어야 합니다.');
                    return;
                }}
                try {{
                    const response = await fetch(`/api/set_interval?value=${{val}}`);
                    const result = await response.json();
                    if (result.success) {{
                        alert('동기화 주기가 변경되었습니다.');
                        location.reload();
                    }}
                }} catch (e) {{
                    alert('서버와 통신 중 오류가 발생했습니다.');
                }}
            }}
        </script>

        {summary_section}

        <h2>테이블별 동기화 상태</h2>
        <table>
            <thead>
                <tr>
                    <th>테이블명</th>
                    <th>상태</th>
                    <th>최종 데이터 시간</th>
                    <th>처리 건수</th>
                    <th>오류 건수</th>
                    <th>소요 시간(초)</th>
                    <th>상세 정보</th>
                    <th>동작</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>

        <div class="footer">
            <p><strong>상태 설명:</strong></p>
            <ul>
                <li><strong style="color:#155724;">성공</strong>: 기간 내 변경 데이터를 성공적으로 동기화했습니다.</li>
                <li><strong style="color:#555;">변경 없음</strong>: 기간 내 변경된 데이터가 없습니다.</li>
                <li><strong style="color:#004085;">진행중</strong>: 현재 동기화 작업이 진행중입니다.</li>
                <li><strong style="color:#856404;">중지됨</strong>: 사용자에 의해 동기화가 일시 중지되었습니다.</li>
                <li><strong style="color:#721c24;">일시적 실패</strong>: 동기화 중 오류가 발생했습니다. 다음 주기에 재시도합니다.</li>
                <li><strong style="color:#dc3545;">영구 실패</strong>: 반복적인 오류로 인해 특정 데이터 구간을 건너뛰었습니다. 로그 확인이 필요합니다.</li>
            </ul>
        </div>
    </div>
</body>
</html>
"""

def generate_html_dashboard(status_data, interval, output_path, is_running=False):
    """주어진 상태 데이터로 HTML 대시보드 파일을 생성합니다."""

    service_status_text = "서비스 실행 중" if is_running else "서비스 중지됨"
    service_status_class = "status-running" if is_running else "status-stopped"
    
    updated_tables_count = status_data.get('updated_tables_count')
    cycle_duration = status_data.get('cycle_duration')
    duration_text = f"{cycle_duration:.2f}초" if cycle_duration is not None else "진행 중..."

    summary_section_html = ""
    if updated_tables_count is not None and updated_tables_count >= 0:
        total_tables = len(status_data.get('tables', {}))
        summary_section_html = f"""
        <div class="summary-box">
            <h2>최근 동기화 요약</h2>
            <p><strong>{updated_tables_count} / {total_tables}</strong> 개 테이블에서 데이터 변경이 감지되어 동기화 시간이 업데이트 되었습니다.</p>
            <p style="margin-top: 10px;"><strong>사이클 소요 시간:</strong> {duration_text}</p>
        </div>
        """
    
    table_rows = []
    # 테이블 이름을 기준으로 정렬하여 항상 같은 순서로 보이도록 함
    sorted_tables = sorted(status_data.get('tables', {}).items())
    
    # 상태 텍스트 한글화 매핑
    status_map = {
        'pending': '대기',
        'in_progress': '진행중',
        'success': '성공',
        'no_data': '변경 없음',
        'failed': '일시적 실패',
        'persistent_failure': '영구 실패',
        'paused': '중지됨'
    }
    
    for table_name, data in sorted_tables:
        if not data: continue # 데이터가 없으면 스킵
        
        status_key = data.get('status', 'pending')
        # 만약 enabled가 False라면 상태를 paused로 강제
        if data.get('enabled') is False:
            status_key = 'paused'
            
        status_text = status_map.get(status_key, status_key)
        message = data.get('message', '대기')
        status_class = f"status-{status_key}"

        duration = f"{data.get('duration', 0):.2f}" if data.get('duration') is not None else "N/A"
        processed = data.get('processed', '')
        errors = data.get('errors', '')
        last_sync = data.get('last_sync_time', 'N/A')

        # 버튼 생성
        if status_key == 'paused':
            action_btn = f'<button class="btn btn-resume" onclick="toggleTable(\'{table_name}\', \'resume\')">재개</button>'
        else:
            action_btn = f'<button class="btn btn-stop" onclick="toggleTable(\'{table_name}\', \'stop\')">중지</button>'

        row = f"""
        <tr>
            <td>{table_name}</td>
            <td class="{status_class}">{status_text}</td>
            <td style="font-family: monospace;">{last_sync}</td>
            <td>{processed if processed != '' else 'N/A'}</td>
            <td>{errors if errors != '' else '0'}</td>
            <td>{duration}</td>
            <td>{message}</td>
            <td>{action_btn}</td>
        </tr>
        """
        table_rows.append(row)

    html_content = HTML_TEMPLATE.format(
        refresh_interval=interval,
        last_updated=status_data.get('last_updated', 'N/A'),
        summary_section=summary_section_html,
        table_rows="\n".join(table_rows),
        service_status_text=service_status_text,
        service_status_class=service_status_class
    )

    temp_output_path = output_path + ".tmp"
    try:
        # Atomic Write: Write to temp file first, then rename
        with open(temp_output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Windows에서는 rename시 대상 파일이 있으면 에러가 날 수 있으므로 replace 사용
        if os.path.exists(output_path):
            os.replace(temp_output_path, output_path)
        else:
            os.rename(temp_output_path, output_path)
            
        logging.info(f"`{output_path}` 대시보드 파일이 업데이트되었습니다.")
    except IOError as e:
        logging.error(f"대시보드 파일 작성 실패: {e}")
        # Clean up temp file if exists
        try:
            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
        except: pass
