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
    "pyspark": "Pyspark", "duckdb": "duckDB"}

# ENDPOINTS
ENDPOINT_LEAGUES = "leagues"
ENDPOINT_FIXTURES = "fixtures"
ENDPOINT_FIXTURE_EVENTS = "fixtures/events"
ENDPOINT_FIXTURE_PLAYER_STATS = "fixtures/players"
ENDPOINT_FIXTURE_STATS = "fixtures/statistics"

SELECTED_FIXTURE = "868209"  # Need a solution for handling many fixtures.


@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json",
    config_schema={
        "params": dict,
    },
    compute_kind=compute_kinds.get("python")
)
def extract_leagues(context) -> Output[dict]:
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_LEAGUES
    params = context.op_config["params"]
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})


@asset(
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-07-01", end_offset=14),
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json",
    compute_kind=compute_kinds.get("python")
)
def extract_fixtures(context) -> Output[dict]:
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURES
    partition_date = context.asset_partition_key_for_output()
    params = {"date": partition_date}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"]), "partition_expr": partition_date})


@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json",
    compute_kind=compute_kinds.get("python")
)
def extract_fixture_events(context) -> dict:
    # <!> Need to be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_EVENTS
    # Select Fulham - Arsenal (2023-03-12)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})


@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json",
    compute_kind=compute_kinds.get("python")
)
def extract_fixture_stats(context) -> dict:
    # <!> Need to be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_STATS
    # Select Fulham - Arsenal (2023-03-12)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})


@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json",
    compute_kind=compute_kinds.get("python")
)
def extract_fixture_player_stats(context) -> dict:
    # <!> Need to be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_PLAYER_STATS
    # Select Fulham - Arsenal (2023-03-12)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})
