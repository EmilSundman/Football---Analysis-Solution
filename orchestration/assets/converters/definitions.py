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
    "pyspark": "Pyspark",
    "duckdb": "duckDB",
    "pandas": "Pandas"
}


@asset(
    io_manager_key="io_manager",
    compute_kind=compute_kinds.get("pandas")
)
def leagues_df(extract_leagues: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted leagues and defines the DataFrame schema for that entity.

    Columns are defined explicitly and are assigned a matching value for each league.
    """
    list_records = []
    for league in extract_leagues["response"]:
        dict_records = {}
        dict_records.update(
            {
                "leagueid": league["league"]["id"],
                "name": league["league"]["name"],
                "type": league["league"]["type"],
                "logomedia": league["league"]["logo"],
                "country": league["country"]["name"],
                "countrycode": league["country"]["code"],
                "countryflagmedia": league["country"]["flag"]
            }
        )

        list_records.append(dict_records.copy())
    # Create a dataframe
    leagues_df = pd.DataFrame(list_records)
    return leagues_df


@asset(
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-07-01", end_offset=14),
    metadata={"partition_expr": "DatePartition"},
    io_manager_key="io_manager",
    compute_kind=compute_kinds.get("pandas")
)
def fixtures_df(context, extract_fixtures: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted fixtures and defines the DataFrame schema for that entity.

    Columns are defined explicitly and are assigned a matching value for each fixture.
    """
    list_records = []
    for fixture in extract_fixtures["response"]:
        dict_records = {}
        dict_records.update({
            "FixtureId": fixture["fixture"]["id"],
            "Referee": fixture["fixture"]["referee"],
            "DateTimeUTC": fixture["fixture"]["date"],
            "VenueId": fixture["fixture"]["venue"]["id"],
            "VenueName": fixture["fixture"]["venue"]["name"],
            "VenueCity": fixture["fixture"]["venue"]["city"],
            "LeagueId": fixture["league"]["id"],
            "LeagueName": fixture["league"]["name"],
            "LeagueCountry": fixture["league"]["country"],
            "LeagueLogoMedia": fixture["league"]["logo"],
            "LeagueFlagMedia": fixture["league"]["flag"],
            "Season": fixture["league"]["season"],
            "Round": fixture["league"]["round"],
            "HomeTeamId": fixture["teams"]["home"]["id"],
            "HomeTeamName": fixture["teams"]["home"]["name"],
            "HomeTeamLogo": fixture["teams"]["home"]["logo"],
            "HomeTeamGoals": fixture["goals"]["home"],
            "AwayTeamId": fixture["teams"]["away"]["id"],
            "AwayTeamName": fixture["teams"]["away"]["name"],
            "AwayTeamLogo": fixture["teams"]["away"]["logo"],
            "AwayTeamGoals": fixture["goals"]["away"],
            "MatchStatus": fixture["fixture"]["status"]["short"],
            "Elapsed": fixture["fixture"]["status"]["elapsed"],
            "DatePartition": context.asset_partition_key_for_output()
        }
        )
        list_records.append(dict_records.copy())
    # Create a dataframe
    fixtures_df = pd.DataFrame(list_records)
    return fixtures_df


@asset(
    io_manager_key="io_manager",
    compute_kind=compute_kinds.get("pandas")
)
def seasons_df(extract_leagues: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted leagues and processes the corresponding seasons that are available for each league in the league endpoint.

    Columns are defined explicitly and are assigned a matching value for each league-season pair.
    """
    list_records = []
    for each in extract_leagues["response"]:
        dict_records = {}
        league_id = each["league"]["id"]

        # Loop through seasons
        for each_season in each["seasons"]:

            dict_records.update(
                {
                    "leagueid": league_id,
                    "seasonid": each_season["year"],
                    "year": each_season["year"],
                    "startdate": each_season["start"],
                    "enddate": each_season["end"],
                    "current": each_season["current"],
                    "fixtureevents": each_season["coverage"]["fixtures"]["events"],
                    "fixturelineups": each_season["coverage"]["fixtures"]["lineups"],
                    "fixturestatistics": each_season["coverage"]["fixtures"]["statistics_fixtures"],
                    "fixtureplayerstatistics": each_season["coverage"]["fixtures"]["statistics_players"],
                    "standings": each_season["coverage"]["standings"],
                    "players": each_season["coverage"]["players"],
                    "topscorers": each_season["coverage"]["top_scorers"],
                    "topassists": each_season["coverage"]["top_assists"],
                    "topcards": each_season["coverage"]["top_cards"],
                    "injuries": each_season["coverage"]["injuries"],
                    "predictions": each_season["coverage"]["predictions"],
                    "odds": each_season["coverage"]["odds"]
                }
            )
            list_records.append(dict_records.copy())
    # Create a dataframe from the list of dicts
    seasons_df = pd.DataFrame(list_records)
    return seasons_df
