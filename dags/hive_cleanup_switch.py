"""hive_cleanup 켜고 끄기: 매시 5, 15, 25, 35, 45, 55분에 hive_cleanup 이 켜져 있으면 끄고, 꺼져 있으면 켠다.

- Airflow 3 작업은 메타데이터 DB 에 직접 접근할 수 없어서 REST API 로 바꾼다.
  GET 으로 지금 상태를 읽고 PATCH /api/v2/dags/hive_cleanup {"is_paused": ...} 로 바꾼다.
- 접속 정보는 Airflow Connection `airflow_api`(http, 호스트 airflow-apiserver:8080, 로그인 계정)에서 읽는다.
  /auth/token 으로 토큰을 받아 쓴다.
- 수동 실행할 때는 action 을 pause / unpause 로 줄 수 있다. 기본값 toggle 은 지금 상태를 뒤집는다.

hive_cleanup 은 catchup=True 라서, 꺼져 있던 동안 hive_copy 가 남긴 이벤트는 다시 켠 뒤
다음 이벤트가 올 때 이벤트마다 실행돼 처리된다.
"""
import logging

import pendulum
import requests
from airflow.sdk import BaseHook, Param, dag, task

TARGET_DAG = "hive_cleanup"
CONN_ID = "airflow_api"

log = logging.getLogger(__name__)


def api_session():
    """Connection 의 계정으로 토큰을 받아 (기본 URL, requests 세션)을 돌려준다."""
    c = BaseHook.get_connection(CONN_ID)
    base = "%s://%s:%s" % (c.schema or "http", c.host, c.port or 8080)
    r = requests.post(base + "/auth/token", json={"username": c.login, "password": c.password}, timeout=30)
    r.raise_for_status()
    s = requests.Session()
    s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
    return base, s


def word(paused):
    return "꺼짐" if paused else "켜짐"


@dag(
    schedule="5,15,25,35,45,55 * * * *",
    start_date=pendulum.datetime(2026, 11, 27, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    params={"action": Param("toggle", enum=["toggle", "pause", "unpause"],
                            description="toggle: 켜져 있으면 끄고 꺼져 있으면 켬 / pause: 끄기 / unpause: 켜기")},
    tags=["hive"],
)
def hive_cleanup_switch():
    @task
    def switch(params=None):
        base, s = api_session()
        url = "%s/api/v2/dags/%s" % (base, TARGET_DAG)
        r = s.get(url, timeout=30)
        r.raise_for_status()
        before = r.json()["is_paused"]

        action = params["action"]
        want_paused = (not before) if action == "toggle" else (action == "pause")
        r = s.patch(url, params={"update_mask": "is_paused"}, json={"is_paused": want_paused}, timeout=30)
        r.raise_for_status()
        after = r.json()["is_paused"]

        log.info("%s %s: %s -> %s (action=%s)", TARGET_DAG, "끄기" if want_paused else "켜기",
                 word(before), word(after), action)
        if after != want_paused:
            raise RuntimeError("%s 상태가 바뀌지 않았습니다: %s" % (TARGET_DAG, word(after)))
        return {"dag_id": TARGET_DAG, "action": action, "before": word(before), "after": word(after)}

    switch()


hive_cleanup_switch()
