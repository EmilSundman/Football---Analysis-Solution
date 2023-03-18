from dagster import (
    asset,
    get_dagster_logger,
    Output
)

logger = get_dagster_logger()

# COMPUTE KINDS
compute_kinds = {
    "python": "Python",
    "pyspark": "Pyspark", "duckdb": "duckDB"}

# ENDPOINTS
ENDPOINT_LEAGUES = "leagues"
ENDPOINT_FIXTURES = "fixtures"


@asset(
    compute_kind=compute_kinds.get("python"),
    config_schema={
        "params": dict,
    },
    required_resource_keys={"api_football_client"},
    io_manager_key="api_pickle_json"
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
    compute_kind=compute_kinds.get("duckdb"),
    io_manager_key="api_pickle_json", code_version="1.0.0")
def downstream(context, extract_leagues) -> str:
    """
    A downstream asset 🦞
    """

    return "Downstream stuff"
