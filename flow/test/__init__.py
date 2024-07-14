import os, sys
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
# sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from flow.test import test

test_assets = load_assets_from_modules([test])

# Define a job that will materialize the assets
test__ingestion_job = define_asset_job("test__ingestion_job", selection='test__ingestion')
test__staging_job = define_asset_job("test__staging_job", selection='test__staging')
test__insertion_job = define_asset_job("test__insertion_job", selection='test__insertion')
test__job = define_asset_job("test_job", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
test__schedule = ScheduleDefinition(
    job=test__job,
    cron_schedule="0 4 * * *",  # every hour
)

defs = Definitions(
    assets=test_assets
    , schedules=[test__schedule]
    , jobs=[test__job]
)
