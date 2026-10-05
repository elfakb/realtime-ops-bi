"""
Polls v_alert_candidates, opens new alerts, resolves cleared ones.
Optional Slack notification via SLACK_WEBHOOK_URL.

Run:  python -m src.alerts.alert_checker
"""
import json
import time
import urllib.request

import psycopg2

from src.common.config import ALERT_INTERVAL_SECONDS, PG, SLACK_WEBHOOK_URL


def notify(severity: str, message: str) -> None:
    line = f"[ALERT][{severity}] {message}"
    print(line, flush=True)
    if not SLACK_WEBHOOK_URL:
        return
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": line}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as exc:  # alerting must never crash the checker
        print(f"Slack notification failed: {exc}", flush=True)


def check_once(cur) -> None:
    cur.execute(
        "SELECT alert_key, alert_type, severity, message, metric_value, threshold "
        "FROM v_alert_candidates"
    )
    candidates = cur.fetchall()
    active_keys = [row[0] for row in candidates]

    for key, a_type, severity, message, value, threshold in candidates:
        cur.execute(
            """
            INSERT INTO alerts (alert_key, alert_type, severity, message, metric_value, threshold)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM alerts WHERE alert_key = %s AND resolved_at IS NULL
            )
            RETURNING alert_id
            """,
            (key, a_type, severity, message, value, threshold, key),
        )
        if cur.fetchone():  # newly opened
            notify(severity, message)

    cur.execute(
        "UPDATE alerts SET resolved_at = now() "
        "WHERE resolved_at IS NULL AND alert_key <> ALL(%s::text[]) "
        "RETURNING alert_key",
        (active_keys,),
    )
    for (key,) in cur.fetchall():
        print(f"[RESOLVED] {key}", flush=True)


def main() -> None:
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    print(f"Alert checker running every {ALERT_INTERVAL_SECONDS}s", flush=True)
    with conn.cursor() as cur:
        while True:
            try:
                check_once(cur)
            except Exception as exc:
                print(f"Check failed: {exc}", flush=True)
            time.sleep(ALERT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()