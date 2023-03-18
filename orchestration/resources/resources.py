from email import header
from dagster import resource, get_dagster_logger
import os
import requests


# Parameters
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")
API_FOOTBALL_BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"
API_FOOTBALL_HEADERS = {
    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
    "X-RapidAPI-Key": f"{API_FOOTBALL_KEY}",
}
logger = get_dagster_logger()


class APIFootballFetcher:
    def fetch_data(self, endpoint: str, params: dict) -> dict:
        base_url = API_FOOTBALL_BASE_URL
        headers = API_FOOTBALL_HEADERS

        logger.info(
            f"""Sending request to endpoint {endpoint} with query parameters: 
        {params}"""
        )
        response = requests.request(
            "GET",
            url=f"{base_url}/{endpoint}",
            headers=headers,
            params=params,
        )
        json_response = response.json()
        if response.status_code != 200:
            logger.error(
                f"""STATUS: {response.status_code}.
            {json_response}"""
            )
            return
        elif len(json_response["response"]) == 0:
            logger.warning(
                f"""EMPTY RESPONSE: {response.status_code}.
            {json_response}"""
            )
            return
        else:
            logger.info(
                f"""First Element in reponse: 
            {json_response["response"][0]}"""
            )
            return json_response


@resource(description=f"An API client that retrieves data from {API_FOOTBALL_BASE_URL}")
def api_football_client():
    return APIFootballFetcher()
