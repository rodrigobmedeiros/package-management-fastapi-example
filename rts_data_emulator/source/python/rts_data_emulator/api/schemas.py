from datetime import datetime
from pydantic import BaseModel
from pydantic import Field
from typing import Literal


class ConfigBase(BaseModel):
    class Config:
        populate_by_name = True


class RequestBodySchema(ConfigBase):
    uid_well: str
    uid_wellbore: str
    uid: str
    mnemonic_aliases: list[str]
    timezone: str
    request_latest_values: int
    size: Literal["s", "m", "l"]


class RequestSchema(ConfigBase):
    operation: str
    execution_time: float = Field(..., alias="executiontime")


class StepIncrementSchema(ConfigBase):
    uom: str
    value: float


class MnemonicSchema(ConfigBase):
    naming_system: str = Field(..., alias="namingSystem")
    value: str


class SensorOffsetSchema(ConfigBase):
    uom: str
    value: float


class LogCurveInfoSchema(ConfigBase):
    mnemonic: MnemonicSchema
    unit: str
    unit_type: str = Field(..., alias="unitType")
    mnem_alias: MnemonicSchema = Field(..., alias="menmAlias")
    alternate_index: bool = Field(..., alias="alternateIndex")
    min_date_time_index: datetime = Field(..., alias="minDateTimeIndex")
    max_date_time_index: datetime = Field(..., alias="maxDateTimeIndex")
    curve_description: str = Field(..., alias="curveDescription")
    sensor_offset: SensorOffsetSchema = Field(..., alias="sensorOffset")
    type_log_data: str = Field(..., alias="typeLogData")
    uid: str


class LogDataSchema(ConfigBase):
    mnemonic_list: list[str] = Field(..., alias="mnemonicList")
    unit_list: list[str] = Field(..., alias="unitList")
    data: list[list[float | datetime]]


class LogSchema(ConfigBase):
    name_well: str = Field(..., alias="nameWell")
    name_wellbore: str = Field(..., alias="nameWellbore")
    name: str
    object_growing: bool = Field(..., alias="objectGrowing")
    run_number: str = Field(..., alias="runNumber")
    creation_date: str = Field(..., alias="creationDate")
    description: str
    index_type: str = Field(..., alias="indexType")
    step_increment: StepIncrementSchema = Field(..., alias="stepIncrement")
    start_date_time_index: datetime = Field(..., alias="startDateTimeIndex")
    end_date_time_index: datetime = Field(..., alias="endDateTimeIndex")
    direction: str
    index_curve: str = Field(..., alias="indexCurve")
    log_curve_info: list[LogCurveInfoSchema] = Field(..., alias="logCurveInfo")
    log_data: LogDataSchema = Field(..., alias="logData")


class ResponseSchema(ConfigBase):
    log: list[LogSchema]


class ResponseBodySchema(ConfigBase):
    request: RequestSchema
    response: ResponseSchema
