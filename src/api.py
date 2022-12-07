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
import requests
import pandas as pd
import datetime
from pandas import json_normalize

epoch = datetime.datetime.utcfromtimestamp(0)


def get_events(issue_id, token=None, limit=None):
    """Retrieve events."""

    token = token or os.getenv("SENTRY_TOKEN", None)

    if token is None:
        raise RuntimeError("Token must be provided")

    all_events = []
    results = True
    url = f"https://sentry.io/api/0/issues/{issue_id}/events/?query="
    counter = 0
    while limit is None or counter < limit:
        r = requests.get(url, headers={"Authorization": "Bearer %s" % token})

        if r.ok:
            events_json = r.json()
        else:
            raise RuntimeError(f"Error {r.status}")

        print(".", end="", flush=True)
        for event in events_json:
            for tag in event["tags"]:
                if tag["key"] == "environment" and tag["value"] in ("prod",):
                    all_events.append(event)
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

    for e in all_events:
        e["tags"] = dict([(a["key"], a["value"]) for a in e["tags"]])

    all_events_df = json_normalize(all_events)

    all_events_df.dateCreated = pd.to_datetime(all_events_df.dateCreated)

    all_events_df["date_minus_time"] = all_events_df["dateCreated"].apply(
        lambda df: datetime.datetime(year=df.year, month=df.month, day=df.day)
    )
    all_events_df["date_minus_time"] = all_events_df[
        "date_minus_time"
    ] - pd.to_timedelta(7, unit="d")
    all_events_df.set_index(all_events_df["date_minus_time"], inplace=True)

    return all_events_df
