from fastapi import Request, Response
from typing import Callable
import json

from fastapi.responses import StreamingResponse


json_key_case_mapping = {
    "uidWell": "uid_well",
    "uidWellbore": "uid_wellbore",
    "MnemonicAliases": "mnemonic_aliases",
    "TimeZone": "timezone",
    "RequestLatestValues": "request_latest_values",
    "request": "Request",
    "response": "Response",
}


def camel_to_snake(data: dict) -> dict:
    """
    Convert keys in a dictionary from camel case to snake case.

    Args:
        data (dict): The dictionary containing the data to be converted.

    Returns:
        dict: A new dictionary with keys converted to snake case.

    Example:
        >>> data = {'firstName': 'John', 'lastName': 'Doe'}
        >>> camel_to_snake(data)
        {'first_name': 'John', 'last_name': 'Doe'}
    """
    new_data: dict = dict()
    for key in data:
        new_data[json_key_case_mapping.get(key, key)] = data[key]
    return new_data


async def camel_to_snake_middleware(request: Request, call_next: Callable):
    """
    Middleware function to convert camel case request body keys to snake case.

    Args:
        request (Request): The incoming request object.
        call_next (Callable): The next middleware or endpoint handler to call.

    Returns:
        Response: The response returned by the next middleware or endpoint handler.
    """
    if request.method in ["POST"]:
        body = await request.json()
        snake_body = camel_to_snake(body)
        request._body = json.dumps(snake_body).encode("utf-8")
    response = await call_next(request)
    return response
