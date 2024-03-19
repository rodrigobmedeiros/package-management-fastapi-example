import json
import random
from datetime import datetime
from datetime import timedelta
from rts_data_emulator.api.schemas import ResponseBodySchema
from pydantic.type_adapter import TypeAdapter
from pathlib import Path


JSON_EXAMPLE_PATH = "rts_data_emulator/source/python/rts_data_emulator/mock/data/rts_response_example.json"
# Based on the value showed in RTS demo interface a range was defined for each TAG.
WHP_LIMITS = (20_000, 22_000)
PIP_LIMITS = (1_800, 1_900)
PDP_LIMITS = (275_000, 285_000)


def generate_timestamps(current_datetime: datetime, n_samples: int) -> list[datetime]:
    """
    Generate a list of timestamps starting from the given current_datetime and going backwards by 1 second for each sample.

    Args:
        current_datetime (datetime): The starting datetime for generating timestamps.
        n_samples (int): The number of timestamps to generate.

    Returns:
        list[datetime]: A list of datetime objects representing the generated timestamps.
    """
    current_datetime = current_datetime.replace(microsecond=0)
    timestamps = [None] * n_samples

    for idx in range(n_samples):
        timestamps[idx * (-1) - 1] = current_datetime - timedelta(seconds=1)
        current_datetime -= timedelta(seconds=1)

    return timestamps


def generate_random_tag_data(tag: str, n_samples: int) -> list[float]:
    """
    Generate a list of random data for a given tag.

    Args:
        tag (str): The tag for which to generate the data.
        n_samples (int): The number of data samples to generate.

    Returns:
        list[float]: A list of random data samples for the given tag.
    """
    match tag:
        case "WHP":
            return [round(random.uniform(*WHP_LIMITS), 2) for _ in range(n_samples)]
        case "PIP":
            return [round(random.uniform(*PIP_LIMITS), 2) for _ in range(n_samples)]
        case "PDP":
            return [round(random.uniform(*PDP_LIMITS), 2) for _ in range(n_samples)]
        case _:
            raise ValueError(f"Invalid tag: {tag}")


def generate_mock_data(current_datetime: datetime, n_samples: int) -> list[list]:
    """
    Generate mock data for a given current datetime and number of samples.

    Args:
        current_datetime (datetime): The current datetime.
        n_samples (int): The number of samples to generate.

    Returns:
        List[List[Union[datetime, float]]]: A list of lists containing the generated data.
            Each inner list contains the timestamp, WHP, PIP, and PDP values for a sample.
    """
    timestamps = generate_timestamps(current_datetime, n_samples)
    pdp = generate_random_tag_data("PDP", n_samples)
    pip = generate_random_tag_data("PIP", n_samples)
    whp = generate_random_tag_data("WHP", n_samples)
    data = [[timestamps[idx], pdp[idx], pip[idx], whp[idx]] for idx in range(n_samples)]
    return data


def update_timestamps_into_response(
    start_time: datetime, end_time: datetime, response_body: ResponseBodySchema
) -> ResponseBodySchema:
    """
    Updates the timestamps in the given response body with the provided start time and end time.

    Args:
        start_time (datetime): The start time to be set for the timestamps.
        end_time (datetime): The end time to be set for the timestamps.
        response_body (ResponseBodySchema): The response body to update.

    Returns:
        ResponseBodySchema: The updated response body with updated timestamps.
    """
    for log in response_body.response.log:
        log.start_date_time_index = start_time
        log.end_date_time_index = end_time
        for log_curve_info in log.log_curve_info:
            log_curve_info.min_date_time_index = start_time
            log_curve_info.max_date_time_index = end_time
    return response_body


def generate_response_with_mock_data(
    current_datetime: datetime, n_samples: int = 1
) -> ResponseBodySchema:
    """
    Retrieves example data and generates mock data for the given datetime.

    Args:
        current_datetime (datetime): The current datetime.
        n_samples (int, optional): The number of mock data samples to generate. Defaults to 1.

    Returns:
        ResponseBodySchema: The response body schema containing the example data with generated mock data.
    """
    with open(Path(JSON_EXAMPLE_PATH)) as f:
        json_data = json.load(f)
    response_body = TypeAdapter(ResponseBodySchema).validate_python(json_data)
    data = generate_mock_data(current_datetime, n_samples)
    response_body.response.log[0].log_data.data = data
    response_body = update_timestamps_into_response(
        data[0][0], data[-1][0], response_body
    )
    return response_body
