# airflowTest

Airflow 배치 작업을 연습하는 저장소입니다. 라이브러리는 만들지 않고 DAG만 둡니다.

## 구성
- Airflow 3.4.0 (`apache/airflow:3.4.0`), LocalExecutor, PostgreSQL 16
- 서비스: `postgres`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `airflow-init`(초기화 후 종료)

| 폴더 | 컨테이너 경로 | 용도 |
|---|---|---|
| `dags/` | `/opt/airflow/dags` | DAG 파일 |
| `plugins/` | `/opt/airflow/plugins` | 플러그인 |
| `config/` | `/opt/airflow/config` | `airflow.cfg` (처음 실행할 때 생성, Git 제외) |
| `logs/` | `/opt/airflow/logs` | 작업 로그 (Git 제외) |
| `data/` | `/opt/airflow/data` | 배치 입출력 파일 (Git 제외) |

## 실행
```bash
cp .env.example .env            # AIRFLOW_UID를 `id -u` 값으로
docker compose up airflow-init  # 처음 한 번: DB 마이그레이션, 계정 생성
docker compose up -d
```
- 웹 화면: http://localhost:8081 (계정 `airflow` / `airflow`)
- 8080은 다른 프로젝트(HyundaiETLPRJ 웹 화면)가 쓰므로 8081을 씁니다.

## 자주 쓰는 명령
```bash
docker compose ps
docker compose run --rm airflow-cli airflow dags list
docker compose run --rm airflow-cli airflow dags test sample_batch 2026-11-26
docker compose logs -f airflow-scheduler
docker compose down          # 중지 (DB 유지)
docker compose down -v       # 중지 + DB 볼륨 삭제 (초기화)
```

## DAG
- `sample_batch`: extract → transform → load. 결과를 `data/summary_<날짜>.json`에 씁니다.
- 새 DAG는 처음에 일시 정지 상태로 올라옵니다(`DAGS_ARE_PAUSED_AT_CREATION`). 화면에서 켜거나 직접 실행하세요.
