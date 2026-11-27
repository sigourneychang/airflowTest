"""Hive 증분 복사: 10분마다 db_appendtest.table_appendtest 의 새 파티션을 원본(src)에서 대상(tgt)으로 옮긴다.

- 옮길 파티션은 워터마크로만 정한다. 원본이나 대상의 파티션 목록과 비교하지 않는다.
  - 워터마크: Airflow Variable `hive_append_copy_watermark` (ymdhm, 'yyyyMMddHHmm'). 여기까지는 처리가 끝났다는 뜻.
  - 이번 구간: 실행 시각(한국 시간)을 L 이라 하면 min(워터마크, L-20분) 다음 분부터 L-10분까지의 매분.
    한 번에 최대 MAX_MINUTES 분. 10분 안쪽은 아직 쌓이는 중일 수 있어 다음 실행으로 넘긴다.
    - 평소에는 워터마크가 L-20분이라 그 실행 자신의 10분(L-19분 ~ L-10분)이다.
    - 빠진 실행이 있으면(꺼져 있었거나 실패) 워터마크부터 이어서 가져온다.
    - clear 나 backfill 로 지난 실행을 다시 돌리면 그 실행 자신의 10분을 다시 복사해서 덮어쓴다.
  - 구간의 파티션 경로가 원본 HDFS 에 있는지만 확인한다. 없는 분(hive_append 가 그 분에 실패 등)은
    로그를 남기고 건너뛴다.
  - 복사와 검증이 성공하면 워터마크를 구간의 마지막 분으로 올린다(건너뛴 분도 지나감). 지금 값보다 클 때만
    올리므로 지난 실행을 다시 돌려도 뒤로 가지 않는다. 실패하면 그대로라서 다음 실행이 같은 구간부터 다시 한다.
  - Variable 이 없으면 실패한다. 시작할 분의 바로 앞 값으로 먼저 만든다(화면 Admin > Variables).
  - 워터마크가 지나간 뒤에 원본에 생긴 파티션(그 분보다 10분 넘게 늦게 쓰인 것, 예: 실패한 hive_append 를
    나중에 다시 돌림)은 옮기지 않는다. 옮기려면 Variable 값을 그 앞으로 낮춘다.
- 복사: 대상에 같은 파티션 폴더가 있으면 지우고(덮어쓰기), 대상 클러스터에서 `hadoop fs -cp` 로 파티션 폴더를
  원본 HDFS 에서 대상 HDFS 로 복사한 뒤, 대상에서 ALTER TABLE ... ADD IF NOT EXISTS PARTITION 으로 등록한다.
  덮어쓰는 동안(몇 초) 대상에서 그 파티션을 읽으면 데이터가 비어 보인다.
  대상에 DB/테이블이 없으면 원본과 같은 구조로 만든다.
- 검증: 옮긴 파티션의 행 수를 원본과 대상에서 각각 세어 비교한다(group by 라서 통계가 아닌 실제 데이터를 셈).
- 원본에는 DESCRIBE, SELECT 와 HDFS 읽기만 한다.
- 접속: Airflow 컨테이너의 /opt/airflow/bin/docker(Docker 소켓)로 src/tgt-hiveserver2 에서 beeline, hadoop 을 실행한다.
"""
import logging
import os
import subprocess

import pendulum
from airflow.sdk import Variable, dag, task

WATERMARK_KEY = "hive_append_copy_watermark"
DB = "db_appendtest"
TABLE = "table_appendtest"
PART_COL = "ymdhm"
FMT = "%Y%m%d%H%M"
LAG = pendulum.duration(minutes=10)       # 이 시간보다 최근 파티션은 다음 실행으로 넘김
INTERVAL_MINUTES = 10                     # 실행 간격 (schedule 과 맞춤). 실행 하나가 맡는 구간 길이
MAX_MINUTES = 120                         # 한 번에 처리할 최대 분 수 (오래 꺼져 있다가 켰을 때)
SRC_HDFS = "hdfs://src-namenode:8020"     # 대상 클러스터에서 원본 HDFS 에 접근하는 주소
TABLE_DDL = """CREATE TABLE IF NOT EXISTS {db}.{tb} (
  id INT,
  name STRING,
  amount DOUBLE,
  created_at STRING
)
PARTITIONED BY ({pc} STRING)
STORED AS ORC""".format(db=DB, tb=TABLE, pc=PART_COL)   # hive_append.py 와 같은 구조

log = logging.getLogger(__name__)


def run(cluster, args, stdin=None, check=True):
    """src/tgt-hiveserver2 컨테이너에서 명령 실행. check=True 면 실패할 때 예외. (종료 코드, 표준 출력)"""
    cmd = ["docker", "exec", "-i", "%s-hiveserver2" % cluster, "timeout", "600"] + args
    env = dict(os.environ, PATH="/opt/airflow/bin:" + os.environ["PATH"])
    p = subprocess.run(cmd, input=stdin, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and p.returncode != 0:
        log.error("명령 실패 (%s, 종료 코드 %d): %s\n%s", cluster, p.returncode, " ".join(args), p.stderr[-3000:])
        raise RuntimeError("%s 명령 실패: %s" % (cluster, " ".join(args[:3])))
    return p.returncode, p.stdout


def hive(cluster, sql):
    """beeline 으로 SQL 실행, 결과 줄(tsv2, 머리글 없음)."""
    _, out = run(cluster, ["/opt/hive/bin/beeline", "-u", "jdbc:hive2://localhost:10000/", "--silent=true",
                           "--outputformat=tsv2", "--showHeader=false", "-f", "/dev/stdin"], stdin=sql)
    return [l for l in out.split("\n") if l.strip()]


def location(cluster):
    """테이블 HDFS 경로 (경로만, hdfs://namenode:port 제외)."""
    for line in hive(cluster, "DESCRIBE FORMATTED %s.%s;" % (DB, TABLE)):
        if line.startswith("Location:"):
            uri = line.split("\t")[1].strip()
            return "/" + uri.split("://", 1)[1].split("/", 1)[1]
    raise RuntimeError("%s 테이블 경로를 찾지 못했습니다" % cluster)


def stat_names(paths):
    """대상 클러스터에서 경로들이 있는지 본다. 있는 경로의 파티션 값 집합 (원본 경로는 hdfs://src-namenode 로 줌)."""
    _, out = run("tgt", ["hadoop", "fs", "-stat", "%n"] + paths, check=False)
    return {l.strip().split("=", 1)[1] for l in out.split("\n") if l.strip().startswith(PART_COL + "=")}


def counts(cluster, parts):
    """파티션별 행 수 {ymdhm: count}."""
    inlist = ",".join("'%s'" % p for p in parts)
    rows = hive(cluster, "set hive.exec.mode.local.auto=true;\n"
                         "SELECT %s, count(1) FROM %s.%s WHERE %s IN (%s) GROUP BY %s;"
                % (PART_COL, DB, TABLE, PART_COL, inlist, PART_COL))
    return {r.split("\t")[0]: int(r.split("\t")[1]) for r in rows if "\t" in r}


@dag(
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 11, 27, 12, 0, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["hive", "append"],
)
def hive_append_copy():
    @task
    def plan(logical_date=None):
        """워터마크 다음 분부터 기준 시각까지의 분 목록을 만들고, 원본 HDFS 에 있는 것만 고른다."""
        wm = Variable.get(WATERMARK_KEY, default=None)
        if not wm:
            raise RuntimeError("Variable %s 가 없습니다. 시작할 분의 바로 앞 값(yyyyMMddHHmm)으로 먼저 만드세요."
                               % WATERMARK_KEY)
        wm_t = pendulum.from_format(wm, "YYYYMMDDHHmm", tz="Asia/Seoul")
        cutoff = (logical_date - LAG).in_timezone("Asia/Seoul").start_of("minute")
        own_start = cutoff.subtract(minutes=INTERVAL_MINUTES - 1)   # 이 실행 자신의 10분 (L-19분 ~ L-10분)
        # 평소: 워터마크 = L-20분 → 자기 10분. 빠진 실행이 있으면 워터마크부터 이어서.
        # clear/backfill 로 지난 실행을 다시 돌리면(워터마크가 이미 지남) 자기 10분을 다시 복사해서 덮어씀
        start = min(wm_t.add(minutes=1), own_start)
        rerun = wm_t >= cutoff
        end = min(cutoff, start.add(minutes=MAX_MINUTES - 1))
        minutes = []
        t = start
        while t <= end:
            minutes.append(t.strftime(FMT))
            t = t.add(minutes=1)

        # 구간의 파티션 경로가 원본에 있는지만 본다 (없는 경로는 stdout 에 안 나오고 종료 코드만 1)
        src_loc = location("src")
        found = stat_names(["%s%s/%s=%s" % (SRC_HDFS, src_loc, PART_COL, m) for m in minutes])
        parts = [m for m in minutes if m in found]
        missing = [m for m in minutes if m not in found]

        log.info("워터마크 %s, 기준 %s: 이번 구간 %s ~ %s (%d분)%s%s", wm, cutoff.strftime(FMT), minutes[0], minutes[-1],
                 len(minutes), ", 남은 분은 다음 실행으로" if end < cutoff else "",
                 " - 워터마크가 이미 지난 실행이라 이 실행의 구간을 다시 복사해 덮어씀" if rerun else "")
        log.info("원본에 있는 파티션 %d개, 없는 분 %d개", len(parts), len(missing))
        for m in missing:
            log.warning("원본에 파티션이 없어 건너뜀: %s=%s", PART_COL, m)
        return {"watermark": wm, "upto": minutes[-1], "parts": parts, "missing": missing}

    @task(execution_timeout=pendulum.duration(minutes=9))
    def copy_partitions(p):
        parts = p["parts"]
        if not parts:
            log.info("옮길 파티션이 없습니다.")
            return p
        hive("tgt", "CREATE DATABASE IF NOT EXISTS %s;\n%s;" % (DB, TABLE_DDL))
        src_loc, tgt_loc = location("src"), location("tgt")

        # 대상에 같은 파티션 폴더가 있으면(앞 실행이 중간에 실패해 워터마크가 안 올라간 경우) 지우고 새로 복사.
        # 이미 등록된 파티션이면 ADD IF NOT EXISTS 가 그대로 두므로 폴더만 새 파일로 바뀐다
        dirs = ["%s/%s=%s" % (tgt_loc, PART_COL, x) for x in parts]
        run("tgt", ["hadoop", "fs", "-rm", "-r", "-f", "-skipTrash"] + dirs)
        run("tgt", ["hadoop", "fs", "-mkdir", "-p", tgt_loc])
        sources = ["%s%s/%s=%s" % (SRC_HDFS, src_loc, PART_COL, x) for x in parts]
        log.info("HDFS 복사: %s/%s=... -> %s (%d개)", src_loc, PART_COL, tgt_loc, len(parts))
        run("tgt", ["hadoop", "fs", "-cp"] + sources + [tgt_loc + "/"])

        adds = " ".join("PARTITION (%s='%s')" % (PART_COL, x) for x in parts)
        hive("tgt", "ALTER TABLE %s.%s ADD IF NOT EXISTS %s;" % (DB, TABLE, adds))
        log.info("대상 파티션 등록 완료: %s ~ %s (%d개)", parts[0], parts[-1], len(parts))
        return p

    @task
    def verify_and_advance(p):
        parts, total = p["parts"], 0
        if parts:
            src, tgt = counts("src", parts), counts("tgt", parts)
            bad = []
            for x in parts:
                ok = src.get(x) == tgt.get(x)
                log.info("%s=%s: 원본 %s행 / 대상 %s행 %s", PART_COL, x, src.get(x, "-"), tgt.get(x, "-"),
                         "OK" if ok else "불일치")
                if not ok:
                    bad.append(x)
            if bad:
                raise RuntimeError("행 수 불일치 파티션 %d개: %s (워터마크는 그대로)" % (len(bad), ", ".join(bad)))
            total = sum(tgt.get(x, 0) for x in parts)
            log.info("증분 복사 완료: %s.%s 파티션 %d개 (%s ~ %s), 총 %d행",
                     DB, TABLE, len(parts), parts[0], parts[-1], total)
        # 워터마크는 앞으로만 간다 (지난 실행을 다시 돌려도 뒤로 가지 않음). 그사이 다른 실행이 올렸을 수 있어 다시 읽음
        now_wm = Variable.get(WATERMARK_KEY, default="") or ""
        if p["upto"] > now_wm:
            Variable.set(WATERMARK_KEY, p["upto"],
                         description="hive_append_copy: 여기(ymdhm)까지 처리 끝. 다음 실행은 이 다음 분부터")
            log.info("워터마크 갱신: %s -> %s (건너뛴 분 %d개)", now_wm or "(없음)", p["upto"], len(p["missing"]))
            new_wm = p["upto"]
        else:
            log.info("워터마크 %s 유지 (이번 구간 끝 %s 가 워터마크 이하라 다시 복사만 함)", now_wm, p["upto"])
            new_wm = now_wm
        return {"partitions": len(parts), "rows": total, "missing": p["missing"], "watermark": new_wm}

    verify_and_advance(copy_partitions(plan()))


hive_append_copy()
