# dashboard_generator.py
import logging
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
        h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }}
        h1 {{ font-size: 2em; }}
        h2 {{ font-size: 1.5em; margin-top: 30px; }}
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
        .footer {{ margin-top: 30px; font-size: 0.9em; color: #777; }}
        .footer ul {{ padding-left: 20px; }}
        .footer li {{ margin-bottom: 5px; }}
        .timestamp {{ font-weight: normal; font-size: 0.9em; color: #555; }}
        .summary-box {{ background-color: #e9ecef; padding: 15px 20px; border-radius: 5px; margin-bottom: 25px; border: 1px solid #ced4da; }}
        .summary-box h2 {{ margin-top: 0; border-bottom: none; font-size: 1.2em; }}
        .summary-box p {{ margin: 5px 0 0; font-size: 1.1em; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>DB 실시간 동기화 현황</h1>
        <p class="timestamp">마지막 업데이트: {last_updated}</p>
        <p class="timestamp">동기화 주기: {refresh_interval}초 (페이지 자동 새로고침)</p>
        
        {summary_section}

        <h2>테이블별 동기화 상태</h2>
        <table>
            <thead>
                <tr>
                    <th>테이블명</th>
                    <th>상태</th>
                    <th>처리 건수</th>
                    <th>오류 건수</th>
                    <th>소요 시간(초)</th>
                    <th>상세 정보</th>
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
                <li><strong style="color:#721c24;">일시적 실패</strong>: 동기화 중 오류가 발생했습니다. 다음 주기에 재시도합니다.</li>
                <li><strong style="color:#dc3545;">영구 실패</strong>: 반복적인 오류로 인해 특정 데이터 구간을 건너뛰었습니다. 로그 확인이 필요합니다.</li>
            </ul>
        </div>
    </div>
</body>
</html>
"""

def generate_html_dashboard(status_data, interval, output_path):
    """주어진 상태 데이터로 HTML 대시보드 파일을 생성합니다."""

    updated_tables_count = status_data.get('updated_tables_count')
    summary_section_html = ""
    if updated_tables_count is not None and updated_tables_count >= 0:
        total_tables = len(status_data.get('tables', {}))
        summary_section_html = f"""
        <div class="summary-box">
            <h2>최근 동기화 요약</h2>
            <p><strong>{updated_tables_count} / {total_tables}</strong> 개 테이블에서 데이터 변경이 감지되어 동기화 시간이 업데이트 되었습니다.</p>
        </div>
        """
    
    table_rows = []
    # 테이블 이름을 기준으로 정렬하여 항상 같은 순서로 보이도록 함
    sorted_tables = sorted(status_data.get('tables', {}).items())
    
    for table_name, data in sorted_tables:
        status_text = data.get('status', 'pending')
        message = data.get('message', '대기')
        status_class = f"status-{status_text}"

        duration = f"{data.get('duration', 0):.2f}" if data.get('duration') is not None else "N/A"
        processed = data.get('processed', '')
        errors = data.get('errors', '')

        row = f"""
        <tr>
            <td>{table_name}</td>
            <td class="{status_class}">{status_text}</td>
            <td>{processed if processed != '' else 'N/A'}</td>
            <td>{errors if errors != '' else '0'}</td>
            <td>{duration}</td>
            <td>{message}</td>
        </tr>
        """
        table_rows.append(row)

    html_content = HTML_TEMPLATE.format(
        refresh_interval=interval,
        last_updated=status_data.get('last_updated', 'N/A'),
        summary_section=summary_section_html,
        table_rows="\n".join(table_rows)
    )

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"`{output_path}` 대시보드 파일이 업데이트되었습니다.")
    except IOError as e:
        logging.error(f"대시보드 파일 작성 실패: {e}")
