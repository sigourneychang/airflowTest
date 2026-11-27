"""Hive 정리: hive_copy 가 옮긴 테이블을 대상 클러스터에서 지운다.

- 시간 스케줄이 없다. hive_copy 의 copy 작업이 성공할 때마다 남기는 Asset `hive_copied_table`
  이벤트로 실행된다. copy 를 clear 해서 다시 성공하거나 hive_copy 를 수동 실행해도 이벤트가 생겨 실행된다.
- 이벤트의 extra 에 옮긴 대상 db.table, source_run_id 에 그 이벤트를 만든 hive_copy 실행이 들어 있다.
- 이 DAG 가 밀려 있는 동안 이벤트가 여러 개 쌓이면 한 번의 실행에 묶여 들어오므로, 받은 테이블을 모두 지운다.
- catchup=True: 이 DAG 를 꺼 둔 동안의 이벤트는 다시 켠 뒤 다음 이벤트가 올 때, 밀린 이벤트마다
  실행이 하나씩 생겨 처리된다(켜는 순간 바로 실행되지는 않는다).
- HyundaiETLPRJ hive_migrate.py 의 --drop-target 으로 지운다. 지운 기록(DROPPED)이 reports/ 에 남아서
  hive_copy 와 웹 화면이 '대상에 없음'으로 본다. 원본 클러스터와 DB 는 건드리지 않는다.
"""
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task
from airflow.sdk.exceptions import AirflowSkipException

PRJ = Path("/opt/hyundai")
CONFIG = PRJ / "conf" / "local.ini"
REPORTS = PRJ / "reports"
WORK_DIR = Path("/opt/airflow/data/hive_copy")   # 실행마다 만드는 삭제 목록
# hive_copy.py 의 copy 작업 outlets 와 같은 이름과 URI
COPIED_TABLE = Asset(name="hive_copied_table", uri="hive://tgt/copied_table")

log = logging.getLogger(__name__)


def run_hive_migrate(args):
    """hive_migrate.py 를 실행하고 출력을 한 줄씩 로그로 남긴다. (종료 코드, 결과 CSV 경로)"""
    cmd = [sys.executable, "-W", "ignore::SyntaxWarning", str(PRJ / "migrate" / "hive_migrate.py"),
           "-c", str(CONFIG), "--report-dir", str(REPORTS)] + args
    env = dict(os.environ, PATH="/opt/airflow/bin:" + os.environ["PATH"], PYTHONUNBUFFERED="1")
    log.info("실행: %s", " ".join(cmd))
    report = None
    with subprocess.Popen(cmd, cwd=str(PRJ), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        for line in p.stdout:
            line = line.rstrip("\n")
            log.info("[hive_migrate] %s", line)
            if line.startswith("report:"):
                report = line.split(":", 1)[1].strip()
    return p.returncode, report


def result_rows(report):
    """결과 CSV 의 대상 db.table -> 행 (삭제 목록은 원본 자리에 대상 이름을 쓴다)."""
    if not report or not os.path.exists(report):
        return {}
    with open(report, newline="") as f:
        return {(r.get("tgt") or r["src"]): r for r in csv.DictReader(f)}


def num(v):
    return "{:,}".format(int(v)) if v not in (None, "") else "-"


@dag(
    schedule=[COPIED_TABLE],
    start_date=pendulum.datetime(2026, 11, 27, tz="Asia/Seoul"),
    # 꺼져 있던 동안 생긴 이벤트는 대기열에 안 올라간다. True 면 다시 켠 뒤 다음 이벤트가 올 때
    # 아직 소비하지 않은 이벤트마다 실행이 생겨서, 그동안 옮긴 테이블도 지운다.
    catchup=True,
    max_active_runs=1,
    tags=["hive"],
)
def hive_cleanup():
    @task
    def drop_copied(triggering_asset_events=None, logical_date=None):
        events = list(triggering_asset_events[COPIED_TABLE]) if triggering_asset_events else []
        todo = {}   # 대상 db.table -> 이벤트 (같은 테이블이 여러 번 오면 마지막 것)
        for ev in events:
            extra = ev.extra or {}
            log.info("받은 이벤트: %s 실행 %s -> %s (%s행, %s)", ev.source_dag_id, ev.source_run_id,
                     extra.get("tgt") or "?", num(extra.get("tgt_rows")), ev.timestamp)
            if extra.get("tgt"):
                todo[extra["tgt"]] = ev
        if not todo:
            raise AirflowSkipException("이벤트에 지울 테이블이 없습니다 (이벤트 %d개)." % len(events))

        stamp = (logical_date or pendulum.now()).in_timezone("Asia/Seoul").strftime("%Y%m%d_%H%M%S")
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        drop_list = WORK_DIR / ("drop_%s.csv" % stamp)
        drop_list.write_text("db,table\n" + "".join("%s,%s\n" % tuple(t.split(".", 1)) for t in todo))

        log.info("대상에서 삭제 시작: %s", ", ".join(todo))
        rc, report = run_hive_migrate(["-s", str(drop_list), "--drop-target"])
        rows, failed, out = result_rows(report), [], []
        for tgt, ev in todo.items():
            r = rows.get(tgt)
            status = r["status"] if r else "결과 없음"
            line = "%s (%s, 옮겼던 %s행, hive_copy 실행 %s)" % (
                tgt, (r or {}).get("kind") or "?", num(ev.extra.get("tgt_rows")), ev.source_run_id)
            if status == "DROPPED":
                log.info("대상 삭제 완료: %s %s", line, (r or {}).get("message") or "")
                out.append({"tgt": tgt, "status": status, "source_run_id": ev.source_run_id})
            else:
                log.error("대상 삭제 실패: %s 상태 %s %s", line, status, (r or {}).get("message") or "")
                failed.append(tgt)
        if rc != 0 or failed:
            raise RuntimeError("대상 삭제 실패: %s (hive_migrate 종료 코드 %d)" % (", ".join(failed) or "-", rc))
        return out

    drop_copied()


hive_cleanup()
