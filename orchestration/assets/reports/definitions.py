from dagster import (
    asset,
    get_dagster_logger,
    Output,
    DailyPartitionsDefinition
)
import datetime
import pandas as pd
import random

logger = get_dagster_logger()

# COMPUTE KINDS
compute_kinds = {
    "python": "Python",
    "pyspark": "Pyspark",
    "duckdb": "duckDB",
    "power_bi": "🟡 Power BI 🟡"}

# ENDPOINTS
ENDPOINT_LEAGUES = "leagues"
ENDPOINT_FIXTURES = "fixtures"
ENDPOINT_FIXTURE_EVENTS = "fixtures/events"
ENDPOINT_FIXTURE_PLAYER_STATS = "fixtures/players"
ENDPOINT_FIXTURE_STATS = "fixtures/statistics"

SELECTED_FIXTURE = "868209"  # Need a solution for handling many fixtures.


@asset(compute_kind=compute_kinds.get('power_bi'))
def downstream_report(league_tables, venues, dim_leagues, dim_seasons) -> None:
    return None
