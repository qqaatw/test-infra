import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, NamedTuple

import requests
import rockset  # type: ignore[import]
from check_alerts import (
    GRAPHQL_URL,
    REPO_OWNER,
    TEST_INFRA_REPO_NAME,
    clear_alerts,
    create_issue,
    headers,
    update_issue,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

PROD_VERSIONS_FILE = REPO_ROOT / "torchci" / "rockset" / "prodVersions.json"

QUEUE_ALERT_LABEL = "queue-alert"

MAX_HOURS = 3
MAX_MACHINES = 50
EXCEPTIONS = {"linux.gcp.a100.large": (0, 20)}
ISSUES_WITH_LABEL_QUERY = """
query ($owner: String!, $name: String!, $labels: [String!]) {
  repository(owner: $owner, name: $name, followRenames: false) {
    issues(last: 10, labels: $labels) {
      nodes {
        number
        body
        state
      }
    }
  }
}
"""


class QueueInfo(NamedTuple):
    machine: str
    count: int
    hours: float


def gen_queue_info_str(q: QueueInfo) -> str:
    return f"- {q.machine}, {q.count} machines, {round(q.hours, 2)} hours\n"


def gen_update_comment(original_issue: Any, new_queues: List[QueueInfo]) -> str:
    original_machines = []
    if original_issue["state"] == "OPEN":
        original_body = original_issue["body"]
        for line in original_body.splitlines():
            match = re.match(r"^- (.*), .* machines, .* hours$", line.strip())
            if match is not None:
                original_machines.append(match.group(1))

    started_queueing = [q for q in new_queues if q.machine not in original_machines]

    s = ""
    if len(started_queueing) > 0:
        s += "These machines started queueing:\n"
        for q in started_queueing:
            s += gen_queue_info_str(q)
        s += "\n"
    return s


def gen_issue(queues: List[QueueInfo]) -> Any:
    queues.sort(key=lambda q: q.machine)
    body = "Within the last 5 minutes, these machines had long queues (exact numbers may be out of date):\n"
    for q in queues:
        body += gen_queue_info_str(q)
    body += "\nPlease look at the hud metrics page for more info."

    issue = {}
    issue["title"] = f"[Pytorch] There are {len(queues)} machines with long queues"
    issue["body"] = body
    issue["labels"] = [QUEUE_ALERT_LABEL]
    issue["state"] = "open"

    return issue


def fetch_alert(owner: str, alert_repo: str, label=QUEUE_ALERT_LABEL) -> List[Any]:
    try:
        variables = {"owner": owner, "name": alert_repo, "labels": [label]}
        r = requests.post(
            GRAPHQL_URL,
            json={"query": ISSUES_WITH_LABEL_QUERY, "variables": variables},
            headers=headers,
        )
        r.raise_for_status()
        return json.loads(r.text)["data"]["repository"]["issues"]["nodes"]
    except Exception as e:
        raise RuntimeError("Error fetching alerts", e)


def filter_long_queues(rockset_result: List[Dict[str, Any]]) -> List[QueueInfo]:
    large_queue: List[QueueInfo] = []

    for result in rockset_result:
        avg_queue_s, count, machine_type = (
            result["avg_queue_s"],
            result["count"],
            result["machine_type"],
        )

        max_hours, max_machines = EXCEPTIONS.get(
            machine_type, (MAX_HOURS, MAX_MACHINES)
        )

        if avg_queue_s / 3600 > max_hours or count > max_machines:
            queue_info = QueueInfo(machine_type, count, avg_queue_s / 3600)
            large_queue.append(queue_info)

    return large_queue


def queuing_alert(dry_run: bool) -> None:
    rs_client = rockset.RocksetClient(
        host="api.usw2a1.rockset.com", api_key=os.environ["ROCKSET_API_KEY"]
    )
    with open(PROD_VERSIONS_FILE) as f:
        prod_versions = json.load(f)

    # same lambda as the same used by the chart on the hud metrics page
    response = rs_client.QueryLambdas.execute_query_lambda(
        query_lambda="queued_jobs_by_label",
        version=prod_versions["metrics"]["queued_jobs_by_label"],
        workspace="metrics",
    )

    large_queue = filter_long_queues(response.results)

    existing_alerts = fetch_alert(
        REPO_OWNER,
        TEST_INFRA_REPO_NAME,
    )

    if len(large_queue) == 0:
        print("Closing queuing alert")
        clear_alerts(existing_alerts, dry_run=dry_run)
        return

    if len(existing_alerts) == 0:
        # Generate a blank issue if there are no issues with the label and
        # re-fetch the issues so we can post an update comment, which will
        # trigger a more informative workchat ping
        create_issue(gen_issue([]), dry_run)
        existing_alerts = fetch_alert(
            REPO_OWNER,
            TEST_INFRA_REPO_NAME,
        )

    existing_issue = existing_alerts[0]
    update_comment = gen_update_comment(existing_issue, large_queue)

    if update_comment:
        new_issue = gen_issue(large_queue)
        update_issue(new_issue, existing_issue, update_comment, dry_run=dry_run)
    else:
        print(f"No new change for queuing alert")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=bool(os.getenv("DRY_RUN")),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    queuing_alert(args.dry_run)
