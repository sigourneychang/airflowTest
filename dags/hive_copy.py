"""Hive 이관: 10분마다 원본 클러스터의 테이블 하나를 대상 클러스터로 복사한다.

HyundaiETLPRJ(컨테이너 /opt/hyundai, 읽기 전용)의 이관 도구 hive_migrate.py를 그대로 실행한다.
- 이관 목록: 이 폴더의 hive_copy_sources.csv (db,table). 위에서부터 차례로 한 개씩 옮긴다.
- 진행 상태: /opt/hyundai/reports 의 결과 CSV. HyundaiETLPRJ 웹 화면과 같은 기록을 쓰므로,
  웹 화면에서 옮긴 테이블은 건너뛰고 Airflow가 옮긴 테이블은 웹 화면에도 보인다.
- 클러스터 접속: /opt/hyundai/conf/local.ini 의 `docker exec ...` 명령.
  Airflow 컨테이너에는 docker CLI가 없어서 /opt/airflow/bin/docker(Docker 소켓 사용)가 대신한다.
"""
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

PRJ = Path("/opt/hyundai")
CONFIG = PRJ / "conf" / "local.ini"
SOURCES = Path(__file__).with_name("hive_copy_sources.csv")
REPORTS = PRJ / "reports"
WORK_DIR = Path("/opt/airflow/data/hive_copy")   # 실행마다 만드는 한 줄짜리 이관 목록

log = logging.getLogger(__name__)


def read_sources():
    """sources.csv 의 (원본 db.table, 대상 db.table) 목록. 머리글, 빈 줄, # 주석은 건너뛴다."""
    out = []
    with SOURCES.open(newline="") as f:
        for row in csv.reader(f):
            row = [c.strip() for c in row]
            if not row or not row[0] or row[0].startswith("#") or row[:2] == ["db", "table"]:
                continue
            if len(row) < 2 or row[1] == "*":
                raise ValueError("테이블 단위 줄만 지원합니다(db,table[,tgt_db]): %r" % row)
            tgt_db = row[2] if len(row) > 2 and row[2] else row[0]
            out.append(("%s.%s" % (row[0], row[1]), "%s.%s" % (tgt_db, row[1])))
    return out


def target_state():
    """대상 'db.table' -> 현재 상태(OK, MISMATCH, ...). hive_migrate 의 resume 과 같은 규칙(history.py)."""
    sys.path.insert(0, str(PRJ / "migrate"))
    from history import History

    h = History(str(REPORTS))
    h.load()
    return {tgt: r["status"] for tgt, r in h.target.items()}


def result_row(report, src):
    """hive_migrate 결과 CSV(reports/migration_<run_id>.csv)에서 원본 db.table 의 행."""
    if not report or not os.path.exists(report):
        return None
    with open(report, newline="") as f:
        for r in csv.DictReader(f):
            if r["src"] == src:
                return r
    return None


def describe(r):
    """결과 한 줄: 'sales.contract -> sales.contract (ACID): 8,262행 복사 (원본 8,262 / 대상 8,262) ...'"""
    def num(v):
        return "{:,}".format(int(v)) if v not in (None, "") else "-"

    kind = r.get("kind") or "?"
    if kind == "VIEW":
        what = "뷰 정의 복사 (데이터 없음)"
    else:
        what = "%s행 복사 (원본 %s / 대상 %s)" % (num(r.get("tgt_rows")), num(r.get("src_rows")), num(r.get("tgt_rows")))
        if r.get("src_partitions"):
            what += ", 파티션 %s/%s" % (num(r.get("tgt_partitions")), num(r.get("src_partitions")))
        if r.get("src_bytes"):
            what += ", 원본 %s바이트 파일 %s개" % (num(r.get("src_bytes")), num(r.get("src_files")))
    out = "%s -> %s (%s): %s, 상태 %s, 검증 %s, %s초" % (
        r["src"], r.get("tgt") or r["src"], kind, what, r["status"],
        r.get("validated_by") or "-", r.get("seconds") or "-")
    if r.get("message"):
        out += ", 메시지: %s" % r["message"]
    return out


@dag(
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 11, 27, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,      # 앞 실행이 10분을 넘기면 다음 실행은 기다린다
    tags=["hive"],
)
def hive_copy():
    @task
    def pick_next():
        sources, state = read_sources(), target_state()
        done = [s for s, t in sources if state.get(t) == "OK"]
        todo = [(s, t) for s, t in sources if state.get(t) != "OK"]
        log.info("이관 목록 %d개: 완료 %d, 남음 %d", len(sources), len(done), len(todo))
        if not todo:
            raise AirflowSkipException("모두 복사했습니다. 옮길 테이블이 없습니다.")
        src, tgt = todo[0]
        log.info("이번에 옮길 테이블: %s -> %s (대상 상태: %s)", src, tgt, state.get(tgt, "없음"))
        return {"src": src, "tgt": tgt}

    @task(execution_timeout=pendulum.duration(minutes=30))
    def copy(item, logical_date=None):
        src_db, table = item["src"].split(".", 1)
        tgt_db = item["tgt"].split(".", 1)[0]
        stamp = logical_date.in_timezone("Asia/Seoul").strftime("%Y%m%d_%H%M")
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        sources = WORK_DIR / ("sources_%s.csv" % stamp)
        sources.write_text("db,table,tgt_db\n%s,%s,%s\n" % (src_db, table, tgt_db))

        # -W: hive_migrate.py 의 docstring 이스케이프 경고(Python 3.12+)는 동작과 무관해서 숨긴다
        cmd = [sys.executable, "-W", "ignore::SyntaxWarning", str(PRJ / "migrate" / "hive_migrate.py"),
               "-c", str(CONFIG), "-s", str(sources),
               "--parallel", "1", "--if-exists", "resume", "--report-dir", str(REPORTS)]
        env = dict(os.environ, PATH="/opt/airflow/bin:" + os.environ["PATH"], PYTHONUNBUFFERED="1")
        log.info("이관 시작: %s -> %s", item["src"], item["tgt"])
        log.info("실행: %s", " ".join(cmd))

        # 출력을 한 줄씩 바로 로그로 남긴다 (끝날 때까지 기다리지 않음)
        report = None
        with subprocess.Popen(cmd, cwd=str(PRJ), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
            for line in p.stdout:
                line = line.rstrip("\n")
                log.info("[hive_migrate] %s", line)
                if line.startswith("report:"):
                    report = line.split(":", 1)[1].strip()
        rc = p.returncode

        row = result_row(report, item["src"])
        if row is None:
            raise RuntimeError("이관 결과를 찾지 못했습니다: %s (결과 CSV %s, 종료 코드 %d)"
                               % (item["src"], report, rc))
        summary = describe(row)
        if rc != 0 or row["status"] != "OK":
            log.error("이관 실패: %s (종료 코드 %d)", summary, rc)
            raise RuntimeError("이관 실패: %s" % summary)
        log.info("이관 완료: %s", summary)
        return {k: row.get(k) for k in ("src", "tgt", "kind", "status", "src_rows", "tgt_rows",
                                        "validated_by", "src_bytes", "seconds")}

    copy(pick_next())


hive_copy()
