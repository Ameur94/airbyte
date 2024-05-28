# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib.parse import urlencode, urljoin

import pendulum
import requests
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.incremental import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
    RequestInput,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
from airbyte_cdk.sources.streams.http.http import BODY_REQUEST_METHODS
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState
from isodate import Duration, parse_duration

from .utils import transform_data

# Number of days ahead for date slices, from start date.
WINDOW_IN_DAYS = 30
# List of Reporting Metrics fields available for fetch
ANALYTICS_FIELDS_V2: List = [
    "actionClicks",
    "adUnitClicks",
    "approximateUniqueImpressions",
    "cardClicks",
    "cardImpressions",
    "clicks",
    "commentLikes",
    "comments",
    "companyPageClicks",
    "conversionValueInLocalCurrency",
    "costInLocalCurrency",
    "costInUsd",
    "dateRange",
    "documentCompletions",
    "documentFirstQuartileCompletions",
    "documentMidpointCompletions",
    "documentThirdQuartileCompletions",
    "downloadClicks",
    "externalWebsiteConversions",
    "externalWebsitePostClickConversions",
    "externalWebsitePostViewConversions",
    "follows",
    "fullScreenPlays",
    "impressions",
    "jobApplications",
    "jobApplyClicks",
    "landingPageClicks",
    "leadGenerationMailContactInfoShares",
    "leadGenerationMailInterestedClicks",
    "likes",
    "oneClickLeadFormOpens",
    "oneClickLeads",
    "opens",
    "otherEngagements",
    "pivotValues",
    "postClickJobApplications",
    "postClickJobApplyClicks",
    "postClickRegistrations",
    "postViewJobApplications",
    "postViewJobApplyClicks",
    "postViewRegistrations",
    "reactions",
    "registrations",
    "sends",
    "shares",
    "talentLeads",
    "textUrlClicks",
    "totalEngagements",
    "validWorkEmailLeads",
    "videoCompletions",
    "videoFirstQuartileCompletions",
    "videoMidpointCompletions",
    "videoStarts",
    "videoThirdQuartileCompletions",
    "videoViews",
    "viralCardClicks",
    "viralCardImpressions",
    "viralClicks",
    "viralCommentLikes",
    "viralComments",
    "viralCompanyPageClicks",
    "viralDocumentCompletions",
    "viralDocumentFirstQuartileCompletions",
    "viralDocumentMidpointCompletions",
    "viralDocumentThirdQuartileCompletions",
    "viralDownloadClicks",
    "viralExternalWebsiteConversions",
    "viralExternalWebsitePostClickConversions",
    "viralExternalWebsitePostViewConversions",
    "viralFollows",
    "viralFullScreenPlays",
    "viralImpressions",
    "viralJobApplications",
    "viralJobApplyClicks",
    "viralLandingPageClicks",
    "viralLikes",
    "viralOneClickLeadFormOpens",
    "viralOneClickLeads",
    "viralOtherEngagements",
    "viralPostClickJobApplications",
    "viralPostClickJobApplyClicks",
    "viralPostClickRegistrations",
    "viralPostViewJobApplications",
    "viralPostViewJobApplyClicks",
    "viralPostViewRegistrations",
    "viralReactions",
    "viralRegistrations",
    "viralShares",
    "viralTotalEngagements",
    "viralVideoCompletions",
    "viralVideoFirstQuartileCompletions",
    "viralVideoMidpointCompletions",
    "viralVideoStarts",
    "viralVideoThirdQuartileCompletions",
    "viralVideoViews",
]
FIELDS_CHUNK_SIZE = 18


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
                    StreamSlice(
                        partition={},
                        cursor_slice={
                            start_field: self._format_datetime(start),
                            end_field: self._format_datetime(end_date),
                            "fields": fields,
                            **date_range,
                        },
                    )
                )
            dates.append(StreamSlice(partition={}, cursor_slice={"field_date_chunks": date_slice_with_fields}))
            start = next_start
        return dates


@dataclass
class LinkedInAdsRecordExtractor(RecordExtractor):
    """
    Unnesting nested bans: `visitor`, `ip_address`.
    """

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
class LinkedInAdsCustomRetriever(SimpleRetriever):
    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        merged_records = defaultdict(dict)
        for field_slice in stream_slice.get("field_date_chunks", []):
            print(field_slice)
            for record in super().read_records(records_schema, stream_slice=field_slice):
                merged_records[f"{record[self.stream_slicer.cursor_field]}-{record['pivotValues']}"].update(record)
        yield from merged_records.values()
