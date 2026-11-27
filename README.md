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
Docker Hub가 429(요청 횟수 제한)를 내면 미러에서 받아 원래 이름으로 태그를 붙입니다.
```bash
docker pull mirror.gcr.io/library/postgres:16 && docker tag mirror.gcr.io/library/postgres:16 postgres:16
docker pull mirror.gcr.io/apache/airflow:3.4.0 && docker tag mirror.gcr.io/apache/airflow:3.4.0 apache/airflow:3.4.0
```

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
- `hive_copy`: 10분마다 Hive 원본 클러스터의 테이블 하나를 대상 클러스터로 옮깁니다. 아래 "Hive 이관 DAG"를 보세요.
- 새 DAG는 처음에 일시 정지 상태로 올라옵니다(`DAGS_ARE_PAUSED_AT_CREATION`). 화면에서 켜거나 직접 실행하세요.

## Hive 이관 DAG (`hive_copy`)
옆 폴더 HyundaiETLPRJ의 두 Hive 클러스터와 이관 도구(`migrate/hive_migrate.py`)를 그대로 씁니다.

```bash
# HyundaiETLPRJ 클러스터를 먼저 띄웁니다 (HiveServer2가 뜨기까지 몇 분 걸림)
docker compose -f ../HyundaiETLPRJ/docker/docker-compose.yml up -d
docker compose run --rm airflow-cli airflow dags test hive_copy   # 한 번 실행
docker compose run --rm airflow-cli airflow dags unpause hive_copy
```

- 이관 목록은 [dags/hive_copy_sources.csv](dags/hive_copy_sources.csv)입니다. 실행마다 위에서부터 대상에 `OK`가 아닌 첫 번째 테이블 하나를 옮깁니다. 다 옮기면 건너뜀(skipped)으로 끝납니다.
- 진행 상태는 HyundaiETLPRJ의 `reports/` 결과 CSV를 씁니다. 그래서 HyundaiETLPRJ 웹 화면과 이력이 같습니다.
- 접속 설정은 HyundaiETLPRJ의 `conf/local.ini`(`docker exec ...`)를 그대로 씁니다. Airflow 이미지에 docker CLI가 없어서 [bin/docker](bin/docker)가 Docker 소켓으로 `docker exec`를 대신합니다.
- `docker-compose.yml`에서 Airflow 컨테이너에 Docker 소켓, HyundaiETLPRJ(읽기 전용), 그 `reports/`(쓰기)를 붙입니다. 위치와 소켓 그룹은 `.env`의 `HIVE_PRJ_DIR`, `DOCKER_GID`입니다.
