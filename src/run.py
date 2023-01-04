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
"""CLI."""
import click


@click.group()
@click.version_option(message="fMRIPrep stats")
def cli():
    """Download stats from Sentry.io."""


@cli.command()
@click.option("-l", "--limit", type=click.IntRange(min=1), default=None)
@click.option("--mongo-user", envvar="MONGO_USERNAME", type=str, default=None)
@click.password_option(
    "--mongo-password",
    envvar="MONGO_PASSWORD",
    default=None,
)
def get(limit, mongo_password, mongo_user):
    from api import get_events

    # Get events
    for event in ("started", "success", "failed"):
        get_events(event, limit=limit, mongo_password=mongo_password, mongo_user=mongo_user)


if __name__ == "__main__":
    """ Install entry-point """
    cli()
