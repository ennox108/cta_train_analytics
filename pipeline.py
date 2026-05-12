"""
CTA Train Tracker → Snowflake pipeline (dlt scaffold).

Data source: CTA Train Tracker API
  https://www.transitchicago.com/developers/ttdocs/

Credentials are read from .dlt/secrets.toml.
Schema configuration is in .dlt/config.toml.

Run:
    python pipeline.py
"""

import dlt
from dlt.sources.helpers import requests


CTA_API_BASE = "http://lapi.transitchicago.com/api/1.0"

# All CTA 'L' route codes
ALL_ROUTES = ["red", "blue", "brn", "g", "org", "p", "pink", "y"]


@dlt.source(name="cta_train")
def cta_train_source(api_key: str = dlt.secrets.value):
    """dlt source that yields CTA Train Tracker resources."""
    yield arrivals(api_key=api_key)
    yield locations(api_key=api_key)


@dlt.resource(
    name="arrivals",
    write_disposition="append",
    primary_key=["run_number", "stop_id", "predicted_arrival_time"],
)
def arrivals(api_key: str, routes: list[str] = ALL_ROUTES):
    """
    Calls ttarrivals.aspx for each route and yields arrival predictions.

    Snowflake table: cta_raw.arrivals
    Key fields:
        station_id, stop_id, station_name, stop_description,
        run_number, route, destination_stop, destination_name,
        train_direction, prediction_generated, predicted_arrival_time,
        is_approaching, is_scheduled, is_fault, is_delayed,
        latitude, longitude, heading
    """
    for route in routes:
        response = requests.get(
            f"{CTA_API_BASE}/ttarrivals.aspx",
            params={"key": api_key, "rt": route, "outputType": "JSON"},
        )
        response.raise_for_status()
        data = response.json()

        for eta in data.get("ctatt", {}).get("eta", []):
            yield {
                "station_id":             eta.get("staId"),
                "stop_id":                eta.get("stpId"),
                "station_name":           eta.get("staNm"),
                "stop_description":       eta.get("stpDe"),
                "run_number":             eta.get("rn"),
                "route":                  eta.get("rt"),
                "destination_stop":       eta.get("destSt"),
                "destination_name":       eta.get("destNm"),
                "train_direction":        eta.get("trDr"),
                "prediction_generated":   eta.get("prdt"),
                "predicted_arrival_time": eta.get("arrT"),
                "is_approaching":         eta.get("isApp"),
                "is_scheduled":           eta.get("isSch"),
                "is_fault":               eta.get("isFlt"),
                "is_delayed":             eta.get("isDly"),
                "latitude":               eta.get("lat"),
                "longitude":              eta.get("lon"),
                "heading":                eta.get("heading"),
            }


@dlt.resource(
    name="train_locations",
    write_disposition="append",
    primary_key=["run_number", "timestamp"],
)
def locations(api_key: str, routes: list[str] = ALL_ROUTES):
    """
    Calls ttpositions.aspx for each route and yields live train positions.

    Snowflake table: cta_raw.train_locations
    Key fields:
        route, run_number, destination_stop, destination_name,
        train_direction, next_station_id, next_stop_id,
        timestamp, is_approaching, is_delayed,
        latitude, longitude, heading
    """
    for route in routes:
        response = requests.get(
            f"{CTA_API_BASE}/ttpositions.aspx",
            params={"key": api_key, "rt": route, "outputType": "JSON"},
        )
        response.raise_for_status()
        data = response.json()

        for train in data.get("ctatt", {}).get("route", [{}])[0].get("train", []):
            yield {
                "route":            route,
                "run_number":       train.get("rn"),
                "destination_stop": train.get("destSt"),
                "destination_name": train.get("destNm"),
                "train_direction":  train.get("trDr"),
                "next_station_id":  train.get("nextStaId"),
                "next_stop_id":     train.get("nextStpId"),
                "timestamp":        train.get("prdt"),
                "is_approaching":   train.get("isApp"),
                "is_delayed":       train.get("isDly"),
                "latitude":         train.get("lat"),
                "longitude":        train.get("lon"),
                "heading":          train.get("heading"),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="cta_train",
        destination="snowflake",
        dataset_name="cta_raw",      # Snowflake schema
    )

    load_info = pipeline.run(cta_train_source())
    print(load_info)
