from functools import reduce
from operator import add
from typing import Any, Dict, Generator, List
from datetime import datetime, timedelta
import requests
import anyconfig


def _get_next_n_days(n:int) -> Generator:
    """_summary_

    Args:
        n (_type_): _description_

    Returns:
        _type_: _description_

    Yields:
        Generator: _description_
    """
    return map(lambda x: datetime.now() + timedelta(x), range(0, n))


def _get_activity_times_json(
    date_obj: datetime,
    venue_name: str,
    activity_name: str,
    base_url: str,
    headers: Dict[str, Any],
) -> requests.Response:
    """_summary_

    Args:
        date_obj (datetime): _description_
        venue_name (str): _description_
        activity_name (str): _description_
        base_url (str): _description_
        headers (Dict[str, Any]): _description_

    Returns:
        requests.Response: _description_
    """
    date_params = (("date", date_obj.strftime("%Y-%m-%d")),)

    response = requests.get(
        url=base_url.format(venue_name=venue_name, activity_name=activity_name),
        headers=headers,
        params=date_params,
    )
    return response


def _get_court_availability(
    days: int,
    venue_name: str,
    activity_name: str,
    base_url: str,
    headers: Dict[str, Any],
) -> Dict[str, requests.Response]:
    """_summary_

    Args:
        days (int): _description_
        venue_name (str): _description_
        activity_name (str): _description_
        base_url (str): _description_
        headers (Dict[str, Any]): _description_

    Returns:
        Dict[str, requests.Response]: _description_
    """
    days_to_check = _get_next_n_days(days)
    responses = {
        day: _get_activity_times_json(
            date_obj=day,
            venue_name=venue_name,
            activity_name=activity_name,
            base_url=base_url,
            headers=headers,
        )
        for day in days_to_check
    }
    return responses


def _parse_json_payload(payload: Dict[str, Any]) -> List[Any]:
    """_summary_

    Args:
        payload (Dict[str, Any]): _description_

    Returns:
        List[Any]: _description_
    """
    data = payload["data"]
    if isinstance(data, dict):
        working_payload = list(data.values())
    if isinstance(data, list):
        working_payload = data

    return working_payload


def _process_json(responses: Dict[str, requests.Response]) -> List[Dict[str, Any]]:
    """_summary_

    Args:
        responses (Dict[str, requests.Response]): _description_

    Returns:
        List[Dict[str, Any]]: _description_
    """
    valid_json = {k: v.json() for k, v in responses.items() if v.status_code == 200}
    flat_json = reduce(add, [_parse_json_payload(v) for k, v in valid_json.items()])
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
    responses = _get_court_availability(**kwargs)
    data = _process_json(responses=responses)
    