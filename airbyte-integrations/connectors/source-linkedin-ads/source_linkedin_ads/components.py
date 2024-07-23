#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import datetime
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib.parse import urlencode, urljoin

import pendulum
import requests
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.extractors.record_filter import RecordFilter
from airbyte_cdk.sources.declarative.incremental import CursorFactory, DatetimeBasedCursor, PerPartitionCursor
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.partition_routers import CartesianProductStreamSlicer
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import SinglePartitionRouter
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
    RequestInput,
)
from airbyte_cdk.sources.declarative.retrievers import SimpleRetriever
from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.declarative.transformations import AddFields
from airbyte_cdk.sources.declarative.transformations.add_fields import AddedFieldDefinition
from airbyte_cdk.sources.declarative.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
from airbyte_cdk.sources.streams.http.http import BODY_REQUEST_METHODS
from isodate import Duration, parse_duration

from .utils import ANALYTICS_FIELDS_V2, FIELDS_CHUNK_SIZE, transform_data


@dataclass
class SafeEncodeHttpRequester(HttpRequester):
    """
    This custom component safely validates query parameters, ignoring the symbols ():,% for UTF-8 encoding.

    Attributes:
        request_body_json: Optional JSON body for the request.
        request_headers: Optional headers for the request.
        request_parameters: Optional parameters for the request.
        request_body_data: Optional data body for the request.
    """

    request_body_json: Optional[RequestInput] = None
    request_headers: Optional[RequestInput] = None
    request_parameters: Optional[RequestInput] = None
    request_body_data: Optional[RequestInput] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.request_options_provider = InterpolatedRequestOptionsProvider(
            request_body_data=self.request_body_data,
            request_body_json=self.request_body_json,
            request_headers=self.request_headers,
            request_parameters=self.request_parameters,
            config=self.config,
            parameters=parameters or {},
        )
        super().__post_init__(parameters)

    def _create_prepared_request(
        self,
        path: str,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
        json: Any = None,
        data: Any = None,
    ) -> requests.PreparedRequest:
        url = urljoin(self.get_url_base(), path)
        http_method = str(self._http_method.value)
        query_params = self.deduplicate_query_params(url, params)
        query_params = urlencode(query_params, safe="():,%")
        args = {"method": http_method, "url": url, "headers": headers, "params": query_params}
        if http_method.upper() in BODY_REQUEST_METHODS:
            if json and data:
                raise RequestBodyException(
                    "At the same time only one of the 'request_body_data' and 'request_body_json' functions can return data"
                )
            elif json:
                args["json"] = json
            elif data:
                args["data"] = data

        return self._session.prepare_request(requests.Request(**args))


@dataclass
class AnalyticsDatetimeBasedCursor(DatetimeBasedCursor):
    @staticmethod
    def chunk_analytics_fields(
        fields: List = ANALYTICS_FIELDS_V2,
        fields_chunk_size: int = FIELDS_CHUNK_SIZE,
    ) -> Iterable[List]:
        """
        Chunks the list of available fields into the chunks of equal size.
        """
        # Make chunks
        chunks = list((fields[f : f + fields_chunk_size] for f in range(0, len(fields), fields_chunk_size)))
        # Make sure base_fields are within the chunks
        for chunk in chunks:
            if "dateRange" not in chunk:
                chunk.append("dateRange")
            if "pivotValues" not in chunk:
                chunk.append("pivotValues")
        yield from chunks

    def _partition_daterange(
        self, start: datetime.datetime, end: datetime.datetime, step: Union[datetime.timedelta, Duration]
    ) -> List[StreamSlice]:
        start_field = self._partition_field_start.eval(self.config)
        end_field = self._partition_field_end.eval(self.config)
        dates = []
        while start <= end:
            next_start = self._evaluate_next_start_date_safely(start, step)
            end_date = self._get_date(next_start - self._cursor_granularity, end, min)
            date_slice_with_fields: List = []
            for fields_set in self.chunk_analytics_fields():
                date_range = {
                    "start.day": start.day,
                    "start.month": start.month,
                    "start.year": start.year,
                    "end.day": end_date.day,
                    "end.month": end_date.month,
                    "end.year": end_date.year,
                }

                fields = ",".join(fields_set)
                date_slice_with_fields.append(
                    {
                        start_field: self._format_datetime(start),
                        end_field: self._format_datetime(end_date),
                        "fields": fields,
                        **date_range,
                    }
                )
            dates.append(StreamSlice(partition={}, cursor_slice={"field_date_chunks": date_slice_with_fields}))
            start = next_start
        return dates


@dataclass
class LinkedInAdsRecordExtractor(RecordExtractor):
    def _date_time_to_rfc3339(self, record: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        """
        Transform 'date-time' items to RFC3339 format
        """
        for item in record:
            if item in ["lastModified", "created"] and record[item]:
                record[item] = pendulum.parse(record[item]).to_rfc3339_string()
        return record

    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        for record in transform_data(response.json().get("elements")):
            yield self._date_time_to_rfc3339(record)


@dataclass
class LinkedInSemiIncrementalFilter(RecordFilter):
    """
    Custom filter to implement semi-incremental syncing for the Comments endpoints, which does not support sorting or filtering.
    This filter emulates incremental behavior by filtering out records based on the comparison of the cursor value with current value in state,
    ensuring only records updated after the cutoff timestamp are synced.
    """

    cursor_field: str = "lastModified"
    format: str = ""

    def filter_records(
        self, records: List[Mapping[str, Any]], stream_state: StreamState, stream_slice: Optional[StreamSlice] = None, **kwargs
    ) -> List[Mapping[str, Any]]:
        """
        Filters a list of records, returning only those with a cursor_value greater than the current value in state.
        """
        current_state = [
            state_value
            for state_value in stream_state.get("states", [])
            if state_value.get("partition", {}).get("id") == stream_slice.get("id")
        ]

        start_date = (
            datetime.datetime.strptime(self.config.get("start_date"), "%Y-%m-%d").timestamp() * 1000
            if self.cursor_field == "lastModifiedAt" or self.format == "timestamp"
            else self.config.get("start_date")
        )

        cursor_value = self._get_filter_date(start_date, current_state)

        if cursor_value:
            return [record for record in records if record[self.cursor_field] >= cursor_value]
        return records

    def _get_filter_date(self, start_date: str, state_value: list) -> str:
        """
        Calculates the filter date to pass in the request parameters by comparing the start_date with the value of state obtained from the stream_slice.
        If only the start_date exists, use it by default.
        """

        start_date_timestamp = start_date or None
        state_value_timestamp = state_value[0]["cursor"][self.cursor_field] if state_value else None

        if state_value_timestamp:
            return max(filter(None, [start_date_timestamp, state_value_timestamp]), default=start_date_timestamp)
        return start_date_timestamp


@dataclass
class LinkedInAdsCustomRetriever(SimpleRetriever):
    partition_router: Optional[Union[List[StreamSlicer], StreamSlicer]] = field(
        default_factory=lambda: SinglePartitionRouter(parameters={})
    )

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(parameters)
        self.cursor = self._initialize_cursor()

    def _initialize_cursor(self):
        partition_router = (
            CartesianProductStreamSlicer(self.partition_router, parameters={})
            if isinstance(self.partition_router, list)
            else self.partition_router
        )

        return PerPartitionCursor(
            cursor_factory=CursorFactory(
                lambda: deepcopy(self.stream_slicer),
            ),
            partition_router=partition_router,
        )

    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        return self.cursor.stream_slices()

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        merged_records = defaultdict(dict)

        self._apply_transformations()

        for field_slice in stream_slice.cursor_slice.get("field_date_chunks", []):
            updated_slice = StreamSlice(partition=stream_slice.partition, cursor_slice={**field_slice})
            for record in super().read_records(records_schema, stream_slice=updated_slice):
                merged_records[f"{record['end_date']}-{record['pivotValues']}"].update(record)

        yield from merged_records.values()

    def _apply_transformations(self):
        transformations = [
            AddFields(
                fields=[
                    AddedFieldDefinition(
                        path=field["path"],
                        value=InterpolatedString(string=field["value"], default=field["value"], parameters={}),
                        value_type=str,
                        parameters={},
                    )
                    for field in transformation.get("fields", [])
                ],
                parameters={},
            )
            for transformation in self.record_selector.transformations
            if isinstance(transformation, dict)
        ]

        if transformations:
            self.record_selector.transformations = transformations

@dataclass
class LinkedInAdsCustomRetriever(SimpleRetriever):
    partition_router: Optional[Union[List[StreamSlicer], StreamSlicer]] = field(
        default_factory=lambda: SinglePartitionRouter(parameters={})
    )

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        super().__post_init__(parameters)
        self.cursor = self._initialize_cursor()

    def _initialize_cursor(self):
        partition_router = (
            CartesianProductStreamSlicer(self.partition_router, parameters={})
            if isinstance(self.partition_router, list)
            else self.partition_router
        )

        return PerPartitionCursor(
            cursor_factory=CursorFactory(
                lambda: deepcopy(self.stream_slicer),
            ),
            partition_router=partition_router,
        )

    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        return self.cursor.stream_slices()

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        merged_records = defaultdict(dict)

        self._apply_transformations()

        for field_slice in stream_slice.cursor_slice.get("field_date_chunks", []):
            updated_slice = StreamSlice(partition=stream_slice.partition, cursor_slice={**field_slice})
            for record in super().read_records(records_schema, stream_slice=updated_slice):
                merged_records[f"{record['end_date']}-{record['pivotValues']}"].update(record)

        yield from merged_records.values()


    # This logic is similar to _read_pages in the HttpStream class. When making changes here, consider making changes there as well.
    def _read_pages(
        self,
        records_generator_fn: Callable[[Optional[requests.Response]], Iterable[StreamData]],
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
    ) -> Iterable[StreamData]:
        pagination_complete = False
        next_page_token = None
        while not pagination_complete:
            response = self._fetch_next_page(stream_state, stream_slice, next_page_token)
            breakpoint()
            yield from records_generator_fn(response)

            if not response:
                pagination_complete = True
            else:
                next_page_token = self._next_page_token(response)
                if not next_page_token:
                    pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _apply_transformations(self):
        transformations = [
            AddFields(
                fields=[
                    AddedFieldDefinition(
                        path=field["path"],
                        value=InterpolatedString(string=field["value"], default=field["value"], parameters={}),
                        value_type=str,
                        parameters={},
                    )
                    for field in transformation.get("fields", [])
                ],
                parameters={},
            )
            for transformation in self.record_selector.transformations
            if isinstance(transformation, dict)
        ]

        if transformations:
            self.record_selector.transformations = transformations



@dataclass
class ChunkedHttpRequester(SafeEncodeHttpRequester):
    def send_request(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        path: Optional[str] = None,
        request_headers: Optional[Mapping[str, Any]] = None,
        request_params: Optional[Mapping[str, Any]] = None,
        request_body_data: Optional[Union[Mapping[str, Any], str]] = None,
        request_body_json: Optional[Mapping[str, Any]] = None,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        breakpoint()
        request, response = self._http_client.send_request(
            http_method=self.get_method().value,
            url=self._join_url(
                self.get_url_base(),
                path or self.get_path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            ),
            request_kwargs={},
            headers=self._request_headers(stream_state, stream_slice, next_page_token, request_headers),
            params=self._request_params(stream_state, stream_slice, next_page_token, request_params),
            json=self._request_body_json(stream_state, stream_slice, next_page_token, request_body_json),
            data=self._request_body_data(stream_state, stream_slice, next_page_token, request_body_data),
            dedupe_query_params=True,
            log_formatter=log_formatter,
            exit_on_rate_limit=self._exit_on_rate_limit,
        )

        return response

