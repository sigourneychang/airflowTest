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
- `hive_cleanup`: `hive_copy`가 테이블을 옮길 때마다(Asset 이벤트) 그 테이블을 대상에서 지웁니다.
- 새 DAG는 처음에 일시 정지 상태로 올라옵니다(`DAGS_ARE_PAUSED_AT_CREATION`). 화면에서 켜거나 직접 실행하세요.
- 새 DAG 파일은 dag-processor가 폴더를 다시 훑을 때(기본 5분)까지 안 보일 수 있습니다. 바로 올리려면 `docker compose run --rm airflow-cli airflow dags reserialize`를 실행합니다.

## Hive 이관 DAG (`hive_copy`, `hive_cleanup`)
옆 폴더 HyundaiETLPRJ의 두 Hive 클러스터와 이관 도구(`migrate/hive_migrate.py`)를 그대로 씁니다.

```bash
# HyundaiETLPRJ 클러스터를 먼저 띄웁니다 (HiveServer2가 뜨기까지 몇 분 걸림)
docker compose -f ../HyundaiETLPRJ/docker/docker-compose.yml up -d
docker compose run --rm airflow-cli airflow dags test hive_copy   # 한 번 실행
docker compose run --rm airflow-cli airflow dags unpause hive_copy
docker compose run --rm airflow-cli airflow dags unpause hive_cleanup
```

- 이관 목록은 [dags/hive_copy_sources.csv](dags/hive_copy_sources.csv)입니다. 원본 인벤토리의 테이블 960개이고, 뷰 40개는 뺐습니다. 뷰는 참조하는 테이블이 대상에 있어야 만들어지는데 `hive_cleanup`이 옮긴 테이블을 바로 지우기 때문입니다.
- `hive_copy`(매시 0, 10, …, 50분)는 지난번에 옮긴 테이블(`data/hive_copy/last_copied.txt`) 다음 것을 옮기고, 끝까지 가면 처음부터 다시 돕니다. 대상에 이미 `OK`로 있는 테이블은 건너뜁니다. 한 바퀴는 약 6.7일입니다.
- `hive_copy`의 `copy` 작업이 성공하면 Asset `hive_copied_table`(`hive://tgt/copied_table`) 이벤트를 남깁니다. 이벤트의 extra에 옮긴 테이블과 행 수가 들어갑니다.
- `hive_cleanup`은 시간 스케줄 없이 이 이벤트로 실행되고, 이벤트의 테이블을 `--drop-target`으로 대상에서 지웁니다. 로그에 이벤트를 만든 `hive_copy` 실행(run_id)이 남습니다.
  - `copy`를 clear해서 다시 성공하거나 `hive_copy`를 수동 실행해도 이벤트가 생겨 `hive_cleanup`이 실행됩니다.
  - `copy`가 실패하거나 건너뛰면 이벤트가 없어서 `hive_cleanup`도 실행되지 않습니다.
  - `hive_cleanup`이 밀려 있는 동안 이벤트가 여러 개 쌓이면 한 번에 묶여 들어오고, 받은 테이블을 모두 지웁니다.
  - `catchup=True`라서 `hive_cleanup`을 꺼 둔 동안 생긴 이벤트도 처리합니다. 다시 켠 뒤 다음 이벤트가 오면 밀린 이벤트마다 실행이 하나씩 생깁니다(run_id에는 그 이벤트의 시각이 들어감). 켜는 순간 바로 돌지는 않습니다.
- 연결은 Airflow 화면의 Assets 메뉴에서 볼 수 있습니다.
- 로그에는 `이관 완료: … N행 복사 …`, `대상 삭제 완료: …`처럼 테이블과 행 수가 남습니다.
- 진행 상태는 HyundaiETLPRJ의 `reports/` 결과 CSV를 씁니다. 그래서 HyundaiETLPRJ 웹 화면과 이력이 같습니다.
- 접속 설정은 HyundaiETLPRJ의 `conf/local.ini`(`docker exec ...`)를 그대로 씁니다. Airflow 이미지에 docker CLI가 없어서 [bin/docker](bin/docker)가 Docker 소켓으로 `docker exec`를 대신합니다.
- `docker-compose.yml`에서 Airflow 컨테이너에 Docker 소켓, HyundaiETLPRJ(읽기 전용), 그 `reports/`(쓰기)를 붙입니다. 위치와 소켓 그룹은 `.env`의 `HIVE_PRJ_DIR`, `DOCKER_GID`입니다.
