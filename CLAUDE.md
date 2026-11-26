# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Airflow 배치 작업 연습 저장소. 개요와 실행 방법은 [README.md](README.md)에 있습니다.

## 응답 언어 (반드시 지킬 것)
**사용자에게 하는 모든 답변은 반드시 한국어로 씁니다.** 사용자가 영어로 묻거나, 도구 결과·오류 메시지·문서가 영어여도 답변은 한국어로 씁니다. 작업 중간에 남기는 짧은 진행 안내와 마지막 요약도 한국어로 씁니다. 사용자가 다른 언어를 직접 요청한 경우에만 예외로 합니다. 코드, 명령어, 파일 이름, 오류 원문은 번역하지 않고 그대로 둡니다.

## 구조
- `docker-compose.yml`은 공식 3.4.0 compose를 LocalExecutor로 줄인 것입니다(Redis, worker, flower 없음). 작업은 scheduler 컨테이너 안에서 실행됩니다.
- `dags/`, `data/`, `plugins/`, `config/`, `logs/`는 볼륨으로 붙어 있어서 DAG를 고치면 dag-processor가 다시 읽습니다. 컨테이너를 재시작할 필요가 없습니다.
- DAG 하나는 파일 하나로 끝냅니다. 공용 모듈이 없으니 [dags/sample_batch.py](dags/sample_batch.py)를 본보기로 씁니다(TaskFlow, `pendulum` 한국 시간 `start_date`, `catchup=False`).
- `AIRFLOW__CORE__DEFAULT_TIMEZONE`은 `Asia/Seoul`이지만 `logical_date`는 UTC로 들어옵니다. 그래서 위 날짜 규칙이 필요합니다.
- 추가 pip 패키지는 `.env`의 `_PIP_ADDITIONAL_REQUIREMENTS`로 넣습니다(사용자 승인 후).
- `airflow-cli`는 `debug` 프로필 서비스라 `up -d`로는 뜨지 않습니다. `docker compose run --rm airflow-cli ...`로만 씁니다.

## 코드 규칙
- Airflow 3.x 문법을 씁니다. `from airflow.sdk import dag, task`를 쓰고, 2.x 전용 import(`airflow.decorators`, `airflow.operators.python` 등)는 쓰지 않습니다.
- 날짜는 한국 시간 기준으로 씁니다. `ds`, `ds_nodash`는 UTC 날짜라 하루 밀리므로 `logical_date.in_timezone("Asia/Seoul")`을 씁니다.
- 배치 입출력 파일은 컨테이너의 `/opt/airflow/data`(호스트 `data/`)에 둡니다.
- 라이브러리나 패키지는 만들지 않습니다. 추가 pip 패키지가 필요하면 먼저 사용자에게 묻습니다.
- 화면 문구, 로그, 문서는 한국어로 씁니다.
- 커밋 메시지는 제목과 본문 모두 한국어로 씁니다. 코드 식별자, 파일 이름, 옵션은 원래 표기대로 둡니다.

## 검증
테스트와 린트 설정은 없습니다. 아래 두 명령으로 확인합니다. 결과 파일은 `data/`에서 봅니다.
- DAG 불러오기 오류: `docker compose run --rm airflow-cli airflow dags list-import-errors`
- DAG 한 번 실행: `docker compose run --rm airflow-cli airflow dags test <dag_id> <날짜>`

## 안전 규칙
- `docker compose down -v`(DB 볼륨 삭제), Colima VM 재시작처럼 되돌리기 어려운 작업은 먼저 사용자에게 묻습니다.
- 호스트 포트 8080은 HyundaiETLPRJ 웹 화면이 씁니다. Airflow는 8081을 씁니다.
