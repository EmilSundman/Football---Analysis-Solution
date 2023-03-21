# %%
import os

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    fs_io_manager,
    load_assets_from_package_module,
)
from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager

from orchestration.assets import forecasting, raw_data, extractors, converters, reports
from orchestration.resources import api_football_client

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")

# all assets live in the default dbt_schema
dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    # prefix the output assets based on the database they live in plus the name of the schema
    # key_prefix=["duckdb", "raw_data"],
    # prefix the source assets based on just the database
    # (dagster populates the source schema information automatically)
    # source_key_prefix=["raw_data"],
)

raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="raw_data",
    # all of these assets live in the duckdb database, under the schema raw_data
    # key_prefix=["duckdb", "raw_data"],
)

api_extractors = load_assets_from_package_module(
    extractors,
    group_name="extractors",
    # all of these assets live in the duckdb database, under the schema raw_data
    # key_prefix=["duckdb", "raw_data"],
)
api_converters = load_assets_from_package_module(
    converters,
    group_name="converters",
    key_prefix=["raw_data"]
)
report_assets = load_assets_from_package_module(
    reports,
    group_name="reports",
    # key_prefix=["duckdb", "raw_data", "reports"],

)

forecasting_assets = load_assets_from_package_module(
    forecasting,
    group_name="forecasting",
)

# define jobs as selections over the larger graph
everything_job = define_asset_job("everything_everywhere_job", selection="*")
forecast_job = define_asset_job(
    "refresh_forecast_model_job", selection="*order_forecast_model")

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": duckdb_pandas_io_manager.configured(
        {"database": os.path.join(
            DBT_PROJECT_DIR, "localdb.db")}
    ),
    # this io_manager is responsible for storing/loading our pickled machine learning model
    "model_io_manager": fs_io_manager,
    "api_pickle_json": fs_io_manager,
    "local_pickle": fs_io_manager,
    # this resource is used to execute dbt cli commands
    "dbt": dbt_cli_resource.configured(
        {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
    ),
    "api_football_client": api_football_client}

defs = Definitions(
    assets=[
        *dbt_assets,
        *report_assets,
        # *raw_data_assets, *forecasting_assets,
        * api_extractors, *api_converters
    ],
    resources=resources,
    # schedules=[
    #     ScheduleDefinition(job=everything_job, cron_schedule="@weekly"),
    #     ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
    # ],
)
