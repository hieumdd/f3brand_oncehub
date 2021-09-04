import os
import json
from datetime import datetime
from abc import ABCMeta, abstractmethod

import requests
from google.cloud import bigquery

HEADERS = {
    "API-Key": os.getenv("API_KEY"),
}
BASE_URL = "https://api.oncehub.com/v2"

NOW = datetime.utcnow()

BQ_CLIENT = bigquery.Client()
DATASET = "api_extracted_data"


class OnceHub(metaclass=ABCMeta):
    @staticmethod
    def factory(table, start, end):
        args = (start, end)
        if table == "Bookings":
            return Bookings(*args)
        elif table == "Contacts":
            return Contacts(*args)
        else:
            raise NotImplementedError(table)

    def __init__(self, start, end):
        self.start, self.end = self._get_time_range(start, end)

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    @abstractmethod
    def keys(self):
        pass

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start, end = _start, _end
        else:
            query = f"""
            SELECT MAX({self.keys['incre_key']}) AS max_incre
            FROM {DATASET}.{self.table}"""
            result = BQ_CLIENT.query(query).result()
            max_incre = [dict(row.items()) for row in result][0]["max_incre"]
            start = max_incre
            end = NOW
        return start, end

    def _get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        params = {
            "last_updated_time.gt": self.start,
            "last_updated_time.lt": self.end,
            "limit": 100,
        }
        rows = []
        with requests.Session() as session:
            while True:
                with session.get(
                    url,
                    params=params,
                    headers=HEADERS,
                ) as r:
                    res = r.json()
                _rows = res["data"]
                if _rows:
                    rows.extend(_rows)
                    params["after"] = _rows[-1]["id"]
                else:
                    break
        return rows

    @abstractmethod
    def transform(self, rows):
        pass

    def _load(self, rows):
        # with open("test.json", "w") as f:
        #     json.dump(rows, f)
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT (row_num) FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {','.join(self.keys['p_key'])} ORDER BY {self.keys['incre_key']} DESC)
                AS row_num
            FROM {DATASET}._stage_{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query).result()

    def run(self):
        rows = self._get()
        response = {
            "table": self.table,
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self._load(rows)
            self._update()
            response["output_rows"] = loads.output_rows
        return response


class Bookings(OnceHub):
    endpoint = "bookings"
    table = "new_oncehub_bookings"
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "last_updated_time", "type": "TIMESTAMP"},
        {"name": "starting_time", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "subject", "type": "STRING"},
        {"name": "tracking_id", "type": "STRING"},
        {"name": "form_submission_company", "type": "STRING"},
        {"name": "form_submission_email", "type": "STRING"},
        {"name": "form_submission_guests", "type": "STRING"},
        {"name": "form_submission_mobile_phone", "type": "STRING"},
        {"name": "form_submission_name", "type": "STRING"},
        {"name": "form_submission_note", "type": "STRING"},
        {"name": "form_submission_phone", "type": "STRING"},
        {"name": "booking_page", "type": "STRING"},
        {"name": "cancel_reschedule_information_actioned_by", "type": "STRING"},
        {"name": "cancel_reschedule_information_reason", "type": "STRING"},
        {"name": "cancel_reschedule_information_user_id", "type": "STRING"},
        {"name": "creation_time", "type": "STRING"},
        {"name": "customer_timezone", "type": "STRING"},
        {"name": "duration_minutes", "type": "INTEGER"},
        {"name": "event_type", "type": "STRING"},
        {"name": "external_calendar_event_id", "type": "STRING"},
        {"name": "external_calendar_id", "type": "STRING"},
        {"name": "external_calendar_name", "type": "STRING"},
        {"name": "external_calendar_type", "type": "STRING"},
        {"name": "in_trash", "type": "BOOLEAN"},
        {"name": "location_description", "type": "STRING"},
        {"name": "master_page", "type": "STRING"},
        {"name": "object", "type": "STRING"},
        {"name": "owner", "type": "STRING"},
        {"name": "rescheduled_booking_id", "type": "STRING"},
        {"name": "virtual_conferencing_join_url", "type": "STRING"},
    ]
    keys = {
        "p_key": [
            "id",
        ],
        "incre_key": "last_updated_time",
    }

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "last_updated_time": row["last_updated_time"],
                "starting_time": row.get("starting_time"),
                "status": row.get("status"),
                "subject": row.get("subject"),
                "tracking_id": row.get("tracking_id"),
                "form_submission_company": json.dumps(
                    row.get("form_submission").get("company")
                ),
                "form_submission_email": row.get("form_submission").get("email"),
                "form_submission_guests": json.dumps(
                    row.get("form_submission").get("guests")
                ),
                "form_submission_mobile_phone": json.dumps(
                    row.get("form_submission").get("mobile_phone")
                ),
                "form_submission_name": json.dumps(
                    row.get("form_submission").get("name")
                ),
                "form_submission_note": json.dumps(
                    row.get("form_submission").get("note")
                ),
                "form_submission_phone": json.dumps(
                    row.get("form_submission").get("phone")
                ),
                "booking_page": row.get("booking_page"),
                "cancel_reschedule_information_actioned_by": row.get(
                    "cancel_reschedule_information_actioned_by"
                ),
                "cancel_reschedule_information_reason": row.get(
                    "cancel_reschedule_information_reason"
                ),
                "cancel_reschedule_information_user_id": row.get(
                    "cancel_reschedule_information_user_id"
                ),
                "creation_time": row.get("creation_time"),
                "customer_timezone": row.get("customer_timezone"),
                "duration_minutes": row.get("duration_minutes"),
                "event_type": row.get("event_type"),
                "external_calendar_event_id": row.get("external_calendar_event_id"),
                "external_calendar_id": row.get("external_calendar_id"),
                "external_calendar_name": row.get("external_calendar_name"),
                "external_calendar_type": row.get("external_calendar_type"),
                "in_trash": row.get("in_trash"),
                "location_description": row.get("location_description"),
                "master_page": row.get("master_page"),
                "object": row.get("object"),
                "owner": row.get("owner"),
                "rescheduled_booking_id": row.get("rescheduled_booking_id"),
                "virtual_conferencing_join_url": row.get(
                    "virtual_conferencing_join_url"
                ),
            }
            for row in rows
        ]


class Contacts(OnceHub):
    endpoint = "contacts"
    table = "new_oncehub_contacts"
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "last_updated_time", "type": "TIMESTAMP"},
        {"name": "creation_time", "type": "TIMESTAMP"},
        {"name": "owner", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "company_size", "type": "STRING"},
        {"name": "company", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "employees", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "job_title", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "mobile_phone", "type": "STRING"},
        {"name": "phone", "type": "STRING"},
        {"name": "post_code", "type": "STRING"},
        {"name": "salutation", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "street_address", "type": "STRING"},
        {"name": "terms_of_service", "type": "STRING"},
        {"name": "timezone", "type": "STRING"},
    ]
    keys = {
        "p_key": [
            "id",
        ],
        "incre_key": "last_updated_time",
    }

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "last_updated_time": row["last_updated_time"],
                "creation_time": row.get("creation_time"),
                "owner": row.get("owner"),
                "city": row.get("city"),
                "company_size": row.get("company_size"),
                "company": row.get("company"),
                "country": row.get("country"),
                "email": row.get("email"),
                "employees": row.get("employees"),
                "first_name": row.get("first_name"),
                "job_title": row.get("job_title"),
                "last_name": row.get("last_name"),
                "mobile_phone": row.get("mobile_phone"),
                "phone": row.get("phone"),
                "post_code": row.get("post_code"),
                "salutation": row.get("salutation"),
                "state": row.get("state"),
                "street_address": row.get("street_address"),
                "terms_of_service": row.get("terms_of_service"),
                "timezone": row.get("timezone"),
            }
            for row in rows
        ]
