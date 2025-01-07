# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
#
# Copyright 2022 The NiPreps Developers <nipreps@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# We support and encourage derived works from this project, please read
# about our expectations at
#
#     https://www.nipreps.org/community/licensing/
#
"""Fetching fMRIPrep statistics from Sentry."""
import os
import sys
from time import sleep
import requests
import datetime
from pymongo import MongoClient

ISSUES = {
    "success": "758615130",
    "started": "540334560",
    "failed": "848853674",
    "no_disk": "767302904",
    "sigkill": "854282951",
}

epoch = datetime.datetime.utcfromtimestamp(0)


def filter_new(event, collection):
    cached = collection.count_documents(filter={"id": event["id"]})

    if not cached:
        event.update({
            f"{tag['key'].replace('.', '_')}": tag["value"]
            for tag in event.pop("tags")
        })
        event.pop("environment", None)
        return event


def get_events(event_name, token=None, limit=None, max_errors=10, cached_limit=10):
    """Retrieve events."""

    token = token or os.getenv("SENTRY_TOKEN", None)

    if token is None:
        raise RuntimeError("Token must be provided")

    issue_id = ISSUES[event_name]

    # Initiate session
    db_client = MongoClient()
    db = db_client.fmriprep_stats
    url = f"https://sentry.io/api/0/issues/{issue_id}/events/?query="
    counter = 0
    errors = []

    consecutive_cached = 0
    while limit is None or counter < limit:
        r = requests.get(url, headers={"Authorization": "Bearer %s" % token})

        if not r.ok:
            print("E", end="", flush=True)
            errors.append(f"{r.status_code}")
            if len(errors) >= max_errors:
                print(f"Too many errors: {', '.join(errors)}", file=sys.stderr)
                exit(1)

            sleep(len(errors) + 1)
            continue

        events_json = [
            event for event in r.json()
            if {'key': 'environment', 'value': 'prod'} in event["tags"]
        ]

        new_documents = [
            document
            for event in events_json
            if (document := filter_new(event, db[event_name])) is not None

        ]

        if new_documents:
            print(".", end="", flush=True)
            db[event_name].insert_many(new_documents)
            consecutive_cached = 0
        else:
            print("c", end="", flush=True)
            consecutive_cached += 1

        if consecutive_cached >= cached_limit:
            break

        cursor = (
            r.headers["Link"].split(",")[1].split(";")[3].split("=")[1].replace('"', "")
        )
        results_str = (
            r.headers["Link"].split(",")[1].split(";")[2].split("=")[1].replace('"', "")
        )

        if results_str.strip() != "true":
            break

        new_url = (
            f"https://sentry.io/api/0/issues/{issue_id}/events/?cursor={cursor}&query="
        )
        url = new_url
        counter += 1

    print("")

    if errors:
        print(f"Encountered {len(errors)} error(s): {', '.join(errors)}.")
