# CLAUDE.md

Airflow 배치 작업 연습 저장소. 개요와 실행 방법은 [README.md](README.md)에 있습니다.

## 코드 규칙
- Airflow 3.x 문법을 씁니다. `from airflow.sdk import dag, task`를 쓰고, 2.x 전용 import(`airflow.decorators`, `airflow.operators.python` 등)는 쓰지 않습니다.
- 배치 입출력 파일은 컨테이너의 `/opt/airflow/data`(호스트 `data/`)에 둡니다.
- 라이브러리나 패키지는 만들지 않습니다. 추가 pip 패키지가 필요하면 먼저 사용자에게 묻습니다.
- 화면 문구, 로그, 문서는 한국어로 씁니다.
- 커밋 메시지는 제목과 본문 모두 한국어로 씁니다. 코드 식별자, 파일 이름, 옵션은 원래 표기대로 둡니다.

## 검증
- DAG 불러오기 오류: `docker compose run --rm airflow-cli airflow dags list-import-errors`
- DAG 한 번 실행: `docker compose run --rm airflow-cli airflow dags test <dag_id> <날짜>`

## 안전 규칙
- `docker compose down -v`(DB 볼륨 삭제), Colima VM 재시작처럼 되돌리기 어려운 작업은 먼저 사용자에게 묻습니다.
- 호스트 포트 8080은 HyundaiETLPRJ 웹 화면이 씁니다. Airflow는 8081을 씁니다.
