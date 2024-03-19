from datetime import datetime
from datetime import timezone
from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse
from fastapi import FastAPI, Header
from typing import Annotated
from postgresql import (
    PostgresController,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
)
from api.middlewares import camel_to_snake_middleware
from api.schemas import RequestBodySchema
from api.schemas import ResponseBodySchema
from mock.rts_data import generate_response_with_mock_data

load_dotenv()

ROOT_PATH = "/services/dev-rts-data-emulator"

_start = datetime.now()
print(f"rts_api: start - {_start}")
pg_db = PostgresController(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, True)
regs_limit = 10000

fastapi_app = FastAPI(root_path=ROOT_PATH)
fastapi_app.middleware("http")(camel_to_snake_middleware)


async def validate_db_conn():
    if pg_db.aconn == None:
        if await pg_db.connect_psql() != True:
            return False
    return True


async def validate_user(apikey: str) -> bool:
    try:
        if apikey == "infra_test":
            return True
    except Exception as e:
        print(f"validate_user: {e}")
    return False


async def validate_general(
    apikey: str,
    well: str | None = None,
    pi_tag: str | None = None,
    use_control: bool = False,
    control_tag=None,
    db_only: bool = False,
) -> bool | JSONResponse:
    try:
        if await validate_db_conn() != True:
            return (
                JSONResponse(
                    status_code=500,
                    content={
                        "error": "db connection issue",
                        "db_name": DB_NAME,
                        "db_ip": DB_HOST,
                        "db_password": DB_PASSWORD,
                        "db_user": DB_USER,
                    },
                ),
                None,
                None,
                None,
                None,
            )
        # =========================================== api_key start
        if apikey is None:
            return (
                JSONResponse(status_code=401, content={"error": "unauthorized-a"}),
                None,
                None,
                None,
                None,
            )

        if await validate_user(apikey) != True:
            return (
                JSONResponse(status_code=401, content={"error": "unauthorized-b"}),
                None,
                None,
                None,
                None,
            )
        # =========================================== api_key end
        # body = await req.json()

        if db_only == True:
            return True, None, None, None, None

        if well is None or pi_tag is None:
            return (
                JSONResponse(status_code=400, content={"error": "missing parameters"}),
                None,
                None,
                None,
                None,
            )

        well_tags_cache = await pg_db.get_well_tags_cache()
        pi_tags_cache = await pg_db.get_pi_tags_cache()

        if well not in well_tags_cache:
            return (
                JSONResponse(status_code=400, content={"error": "well not found"}),
                None,
                None,
                None,
                None,
            )

        if pi_tag not in pi_tags_cache:
            return (
                JSONResponse(status_code=400, content={"error": "pi_tag not found"}),
                None,
                None,
                None,
                None,
            )

        if well_tags_cache[well]["unique_id"] != pi_tags_cache[pi_tag]["well_tag"]:
            return (
                JSONResponse(
                    status_code=400, content={"error": "well and pi_tag mismatch"}
                ),
                None,
                None,
                None,
                None,
            )

        if use_control == False:
            date_min, date_max = await pg_db.get_measurements_date_limits(
                well_tags_cache[well]["unique_id"], pi_tags_cache[pi_tag]["unique_id"]
            )
        else:
            date_min, date_max = await pg_db.get_control_measurements_date_limits(
                well_tags_cache[well]["unique_id"],
                pi_tags_cache[pi_tag]["unique_id"],
                control_tag,
            )

        if date_min is None or date_max is None:
            return (
                JSONResponse(status_code=500, content={"error": "date time issue"}),
                None,
                None,
                None,
                None,
            )

        return (
            True,
            date_min,
            date_max,
            well_tags_cache[well]["unique_id"],
            pi_tags_cache[pi_tag]["unique_id"],
        )
    except Exception as e:
        print(f"validate_general: {e}")
    return False, None, None, None, None


async def validate_time_and_limit(
    date_min: datetime,
    date_max: datetime,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int | None = None,
) -> bool:
    try:
        if start_time is None:
            start_time = date_min
        else:
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").astimezone(
                timezone.utc
            )
            if start_time < date_min:
                start_time = date_min
            if start_time > date_max:
                start_time = date_max

        if end_time is None:
            end_time = date_max
        else:
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").astimezone(
                timezone.utc
            )
            if end_time < date_min:
                end_time = date_min
            if end_time > date_max:
                end_time = date_max

        if start_time > end_time:
            end_time, start_time = start_time, end_time

        if limit is not None:
            if limit < 1:
                limit = 1
            if limit > regs_limit:
                limit = regs_limit
        else:
            limit = regs_limit

        return True, start_time, end_time, limit
    except Exception as e:
        print(f"validate_time_and_limit: {e}")
    return False, None, None, None


@fastapi_app.get("/control_measurements/dt_limit")
async def get_meas_dt_limit(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
    well: str | None = None,
    pi_tag: str | None = None,
    control_tag: str | None = None,
):
    try:
        validate, date_min, date_max, well_tag_id, pi_tag_id = await validate_general(
            apikey, well, pi_tag, use_control=True, control_tag=control_tag
        )
        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )

        return JSONResponse(
            status_code=200,
            content={
                "message": "ok",
                "well": well,
                "pi_tag": pi_tag,
                "date_min": date_min.strftime("%Y-%m-%d %H:%M:%S"),
                "date_max": date_max.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
    except Exception as e:
        print(f"error /control_measurements/dt_limit: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})


@fastapi_app.get("/control_measurements/")
async def get_meas(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
    well: str | None = None,
    pi_tag: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int | None = None,
):
    try:
        validate, date_min, date_max, well_tag_id, pi_tag_id = await validate_general(
            apikey, well, pi_tag, use_control=True
        )
        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )

        valid_dt, start_time, end_time, limit = await validate_time_and_limit(
            date_min, date_max, start_time, end_time, limit
        )
        if valid_dt != True:
            return JSONResponse(status_code=500, content={"error": "date time issue"})

        available_control_tags = await pg_db.get_pi_control_tags(well_tag_id, pi_tag_id)
        print(f"available_control_tags: {available_control_tags}")

        get_control_tags_cache = await pg_db.get_control_tags_cache()
        get_control_tags_cache_inv = {v: k for k, v in get_control_tags_cache.items()}
        print(f"get_control_tags_cache: {get_control_tags_cache_inv}")

        data_ret = {}
        for control_tag in available_control_tags:
            # print(f'control_tag: {control_tag}')
            control_tag_name = get_control_tags_cache_inv[control_tag]
            # print(f'control_tag_name: {control_tag_name}')
            measurements = await pg_db.get_control_measurements(
                well_tag_id, pi_tag_id, control_tag, start_time, end_time, limit
            )

            if len(measurements) == 0 or measurements is None:
                continue
            data_ret[control_tag_name] = []

            for m in measurements:
                data_ret[control_tag_name].append(
                    (m["timestamp"].strftime("%Y-%m-%d %H:%M:%S,%f"), m["value"])
                )

        return JSONResponse(
            status_code=200,
            content={
                "message": "ok",
                "well": well,
                "pi_tag": pi_tag,
                "control_tags": data_ret,
            },
        )
    except Exception as e:
        print(f"error /control_measurements/: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})

    return await fastapi_handler.handle(req)


@fastapi_app.get("/measurements/dt_limit")
async def get_meas_dt_limit(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
    well: str | None = None,
    pi_tag: str | None = None,
):
    try:
        validate, date_min, date_max, well_tag_id, pi_tag_id = await validate_general(
            apikey, well, pi_tag
        )
        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )

        return JSONResponse(
            status_code=200,
            content={
                "message": "ok",
                "well": well,
                "pi_tag": pi_tag,
                "date_min": date_min.strftime("%Y-%m-%d %H:%M:%S"),
                "date_max": date_max.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
    except Exception as e:
        print(f"error /measurements/dt_limit: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})


@fastapi_app.get("/measurements/")
async def get_meas(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
    well: str | None = None,
    pi_tag: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int | None = None,
):
    try:
        validate, date_min, date_max, well_tag_id, pi_tag_id = await validate_general(
            apikey, well, pi_tag
        )
        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )

        valid_dt, start_time, end_time, limit = await validate_time_and_limit(
            date_min, date_max, start_time, end_time, limit
        )
        if valid_dt != True:
            return JSONResponse(status_code=500, content={"error": "date time issue"})

        measurements = await pg_db.get_measurements(
            well_tag_id, pi_tag_id, start_time, end_time, limit
        )
        if measurements is None:
            return JSONResponse(
                status_code=500, content={"error": "measurements issue"}
            )

        data_ret = []
        for m in measurements:
            data_ret.append(
                (m["timestamp"].strftime("%Y-%m-%d %H:%M:%S,%f"), m["value"])
            )

        return JSONResponse(
            status_code=200,
            content={
                "message": "ok",
                "well": well,
                "pi_tag": pi_tag,
                "measurements": data_ret,
            },
        )
    except Exception as e:
        print(f"error /measurements/: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})

    return await fastapi_handler.handle(req)


@fastapi_app.get("/wells/")
async def get_meas_dt_limit(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
):
    try:
        validate, *_ = await validate_general(apikey, db_only=True)

        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )

        # wells = await pg_db.get_wells()
        wells = await pg_db.get_well_tags_cache()
        if wells is None:
            return JSONResponse(status_code=500, content={"error": "wells issue"})

        return JSONResponse(status_code=200, content={"message": "ok", "wells": wells})

    except Exception as e:
        print(f"error /measurements/dt_limit: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})


@fastapi_app.get("/pi_tags/")
async def get_meas_dt_limit(
    req: Request,
    apikey: Annotated[str | None, Header(convert_underscores=False)] = None,
    well: str | None = None,
):
    try:
        validate, *_ = await validate_general(apikey, db_only=True)

        if validate != True:
            if isinstance(validate, JSONResponse):
                return validate
            else:
                return JSONResponse(
                    status_code=404, content={"message": "error validating"}
                )
        well_tags_cache = await pg_db.get_well_tags_cache()
        pi_tags_cache = await pg_db.get_pi_tags_cache()
        # wells = await pg_db.get_wells()

        if well_tags_cache is None:
            return JSONResponse(status_code=500, content={"error": "no wells"})

        answ = {}
        for w in well_tags_cache.keys():
            answ[w] = []
            for p in pi_tags_cache.keys():
                if pi_tags_cache[p]["well_tag"] == well_tags_cache[w]["unique_id"]:
                    answ[w].append(p)

        if answ is None:
            return JSONResponse(status_code=500, content={"error": "answer issue"})

        return JSONResponse(status_code=200, content={"message": "ok", "pi_tags": answ})

    except Exception as e:
        print(f"error /measurements/dt_limit: {e}")
    return JSONResponse(status_code=404, content={"message": "not found"})


@fastapi_app.post(
    "/find-logs",
    description="Endpoint to get RTS emulated data",
    summary="Get RTS emulated data",
    response_model=ResponseBodySchema,
)
async def find_logs(request_body: RequestBodySchema) -> ResponseBodySchema:
    current_datetime = datetime.now().replace(microsecond=0)
    n_samples = request_body.request_latest_values
    response_body: ResponseBodySchema = generate_response_with_mock_data(
        current_datetime, n_samples
    )
    return response_body
