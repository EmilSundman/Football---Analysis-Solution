{{
        config(
                dagster_freshness_policy={"maximum_lag_minutes": 10, "cron_schedule": "0 6 * * *"}
        )
}}

select 
LeagueName, Season
-- Expand for logic of home and away games. 
from {{ref('fact_fixtures')}}
group by LeagueName, Season