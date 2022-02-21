from functools import reduce
from operator import add
from typing import Any, Dict, Generator, List
from datetime import datetime, timedelta
import requests
import anyconfig


def _get_next_n_days(n:int) -> Generator:
    """This method will return a iterable of datetime
    objects incremented by 1 until 'n' days from today
    """
    return map(lambda x: datetime.now() + timedelta(x), range(0, n))


def _get_single_availability(
    date_obj: datetime,
    venue_name: str,
    activity_name: str,
    base_url: str,
    headers: Dict[str, Any],
) -> requests.Response:
    """This method retrieves the activity times from the better.org
    API endpoint.

    Args:
        date_obj (datetime): The day in question
        venue_name (str): The venue in question
        activity_name (str): The activity in question
        base_url (str): The URL to format
        headers (Dict[str, Any]): The required GET request headers

    Returns:
        requests.Response: An object that contains the server response.
            Successful responses will be code 200, Unsuccessful responses
            will return 422
    """
    date_params = (("date", date_obj.strftime("%Y-%m-%d")),)

    response = requests.get(
        url=base_url.format(venue_name=venue_name, activity_name=activity_name),
        headers=headers,
        params=date_params,
    )
    return response


def _get_batch_availability(
    days: int,
    venue_name: str,
    activity_name: str,
    base_url: str,
    headers: Dict[str, Any],
) -> List[requests.Response]:
    """This method will query the better.org API for a
    series of days retrieving the responses for all days 
    from today to day 'n'

    Args:
        days (int): The number of days to query in this batch
        venue_name (str): The venue in question
        activity_name (str): The activity in question
        base_url (str): The URL to format
        headers (Dict[str, Any]): The required GET request headers

    Returns:
        List[requests.Response]: A list of responses
    """
    days_to_check = _get_next_n_days(days)
    responses = [_get_single_availability(
            date_obj=day,
            venue_name=venue_name,
            activity_name=activity_name,
            base_url=base_url,
            headers=headers,
        )
        for day in days_to_check
    ]
    if responses:
        return responses
    raise IOError('Unable to retrieve any availability from the API')


def _parse_json_payload(payload: Dict[str, Any]) -> List[Any]:
    """The JSON payloads provided by the better API are different 
    depending on the day this process makes them all consistent

    Args:
        payload (Dict[str, Any]): The dictionary object to process

    Returns:
        List[Any]: The consistent list object for all payloads
    """
    data = payload["data"]
    if isinstance(data, dict):
        working_payload = list(data.values())
    if isinstance(data, list):
        working_payload = data

    return working_payload


def _process_json(responses: List[requests.Response]) -> List[Dict[str, Any]]:
    """This method processes the JSON payloads and returns a simple list 
    of dictionaries that have been processed, filtered and transformed.

    Args:
        responses (List[requests.Response]): The responses to filter and process

    Returns:
        List[Dict[str, Any]]: The processed dictionary objects 
    """
    valid_json = [x.json() for x in responses if x.status_code == 200]
    flat_json = reduce(add, [_parse_json_payload(x) for x in valid_json])
    columns = {
        "timestamp": datetime.fromtimestamp,
        "spaces": int,
        "duration": lambda x: timedelta(minutes=int(x.replace("min", ""))),
        "price": lambda x: x.get("formatted_amount"),
    }
    processed_json = [
        {k: columns[k](v) for k, v in x.items() if k in columns.keys()}
        for x in flat_json
    ]
    return processed_json


if __name__ == "__main__":
    config = anyconfig.load('config.yaml')
    kwargs = config['search_params'] | config['better_api']
    responses = _get_batch_availability(**kwargs)
    data = _process_json(responses=responses)
    