{{
        config(
                dagster_freshness_policy={"maximum_lag_minutes": 0, "cron_schedule": "0 4 * * *"}
        )
}}
select
    *
from {{ref('leagues')}}