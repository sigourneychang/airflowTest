"""Hive 데이터 쌓기: 매분 원본(src) 클러스터의 db_appendtest.table_appendtest 에 3행을 넣는다.

- 파티션 컬럼 ymdhm 은 실행 시각(한국 시간)의 연월일시분 'yyyyMMddHHmm' 이다. 매분 파티션이 하나씩 생긴다.
- DB 와 테이블은 없으면 만든다(IF NOT EXISTS). 같은 beeline 세션에서 함께 실행해 JVM 을 한 번만 띄운다.
- 같은 분을 다시 실행하면(clear) INSERT OVERWRITE 라서 그 파티션이 같은 3행으로 다시 써진다.
- 원본 Hive 접속: Airflow 컨테이너의 /opt/airflow/bin/docker(Docker 소켓)로 src-hiveserver2 에서 beeline 을 실행한다.
"""
import logging
import os
import subprocess

import pendulum
from airflow.sdk import dag, task

DB = "db_appendtest"
TABLE = "table_appendtest"
BEELINE = ["docker", "exec", "-i", "src-hiveserver2", "timeout", "300",
           "/opt/hive/bin/beeline", "-u", "jdbc:hive2://localhost:10000/",
           "--silent=true", "--outputformat=tsv2", "--showHeader=false", "-f", "/dev/stdin"]

log = logging.getLogger(__name__)


@dag(
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 11, 27, 12, 0, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["hive", "append"],
)
def hive_append():
    @task(execution_timeout=pendulum.duration(minutes=5))
    def append_rows(logical_date=None):
        kst = logical_date.in_timezone("Asia/Seoul")
        part = kst.strftime("%Y%m%d%H%M")
        ts = kst.strftime("%Y-%m-%d %H:%M:%S")
        rows = [(i, "row_%s_%d" % (part, i), round(i * 1.5 + kst.minute, 2)) for i in (1, 2, 3)]
        values = ",\n  ".join("(%d, '%s', %s, '%s')" % (i, name, amount, ts) for i, name, amount in rows)
        sql = """
set hive.exec.mode.local.auto=true;
CREATE DATABASE IF NOT EXISTS {db};
CREATE TABLE IF NOT EXISTS {db}.{tb} (
  id INT,
  name STRING,
  amount DOUBLE,
  created_at STRING
)
PARTITIONED BY (ymdhm STRING)
STORED AS ORC;
INSERT OVERWRITE TABLE {db}.{tb} PARTITION (ymdhm='{part}') VALUES
  {values};
SELECT count(*) FROM {db}.{tb} WHERE ymdhm='{part}';
""".format(db=DB, tb=TABLE, part=part, values=values)

        log.info("파티션 %s.%s ymdhm=%s 에 %d행 넣기 시작", DB, TABLE, part, len(rows))
        env = dict(os.environ, PATH="/opt/airflow/bin:" + os.environ["PATH"])
        p = subprocess.run(BEELINE, input=sql, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out = p.stdout.strip().splitlines()
        if p.returncode != 0 or not out or not out[-1].strip().isdigit():
            log.error("beeline 오류 (종료 코드 %d)\n%s", p.returncode, p.stderr[-3000:])
            raise RuntimeError("파티션 %s 넣기 실패 (종료 코드 %d)" % (part, p.returncode))
        count = int(out[-1].strip())
        log.info("넣기 완료: %s.%s ymdhm=%s, 파티션 행 수 %d", DB, TABLE, part, count)
        if count != len(rows):
            raise RuntimeError("파티션 %s 행 수가 %d 입니다 (기대 %d)" % (part, count, len(rows)))
        return {"table": "%s.%s" % (DB, TABLE), "ymdhm": part, "rows": count}

    append_rows()


hive_append()
