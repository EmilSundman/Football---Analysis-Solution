from dagster import (
    asset,
    get_dagster_logger,
    Output,
    DailyPartitionsDefinition
)
import datetime
import pandas as pd

logger = get_dagster_logger()

# COMPUTE KINDS
compute_kinds = {
    "python": "Python",
    "pyspark": "Pyspark", "duckdb": "duckDB"}

# ENDPOINTS
ENDPOINT_LEAGUES = "leagues"
ENDPOINT_FIXTURES = "fixtures"


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
