{{
        config(
                dagster_freshness_policy={"maximum_lag_minutes": 60, "cron_schedule": "0 2 * * *"}, 
        )
}}
select
    *
from {{ref('fixtures')}}