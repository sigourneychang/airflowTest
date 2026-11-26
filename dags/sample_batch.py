"""샘플 배치: 숫자 목록을 만들고(extract) 집계해서(transform) 날짜별 파일로 남긴다(load)."""
import json
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

DATA_DIR = Path("/opt/airflow/data")


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["sample"],
)
def sample_batch():
    @task
    def extract():
        return list(range(1, 11))

    @task
    def transform(rows):
        return {"count": len(rows), "total": sum(rows)}

    @task
    def load(summary, logical_date=None):
        # ds는 UTC 날짜라 한국 시간 자정 실행이 전날로 찍힌다. 한국 날짜로 바꿔 쓴다.
        day = logical_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out = DATA_DIR / f"summary_{day}.json"
        out.write_text(json.dumps(summary, ensure_ascii=False))
        print(f"저장: {out} {summary}")

    load(transform(extract()))


sample_batch()
