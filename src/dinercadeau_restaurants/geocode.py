"""Geocoding helpers for restaurant addresses."""

from __future__ import annotations

import logging
import time
from typing import Optional, Sequence

import requests
from geopy.distance import geodesic

from .models import GeocodeResult, Restaurant
from .settings import GeocodeSettings, UTRECHT_COORDINATES

logger = logging.getLogger(__name__)


class NominatimGeocoder:
    """Thin wrapper around the public Nominatim API."""

    def __init__(self, settings: Optional[GeocodeSettings] = None) -> None:
        self.settings = settings or GeocodeSettings()
        self._session = requests.Session()
        headers = {"User-Agent": "dinercadeau-restaurants-index/0.1.0"}
        self._session.headers.update(headers)

    def geocode(self, query: str) -> Optional[GeocodeResult]:
        response = self._session.get(
            self.settings.provider_url,
            params=self.settings.query_params(query),
            timeout=self.settings.timeout,
        )
        response.raise_for_status()
        items = response.json()
        if not items:
            return None
        item = items[0]
        try:
            latitude = float(item["lat"])
            longitude = float(item["lon"])
        except (KeyError, TypeError, ValueError):
            return None
        return GeocodeResult(
            address=item.get("display_name", query),
            latitude=latitude,
            longitude=longitude,
            raw=item,
        )


def annotate_with_coordinates(restaurants: Sequence[Restaurant], geocoder: Optional[NominatimGeocoder] = None) -> None:
    """Enrich restaurants in-place with latitude/longitude when missing."""

    geocoder = geocoder or NominatimGeocoder()

    for restaurant in restaurants:
        if restaurant.latitude is not None and restaurant.longitude is not None:
            continue
        query_parts = [restaurant.address, restaurant.postal_code, restaurant.city, restaurant.country]
        query = ", ".join(part for part in query_parts if part)
        if not query:
            continue
        try:
            result = geocoder.geocode(query)
        except requests.RequestException:
            logger.warning("Geocoding failed for %s", restaurant.name, exc_info=True)
            continue
        if result:
            restaurant.latitude = result.latitude
            restaurant.longitude = result.longitude
            logger.debug("Geocoded %s -> %s", restaurant.name, result.address)
        pause = getattr(geocoder.settings, "pause_seconds", 1.0) or 0.0
        if pause > 0:
            time.sleep(pause)


def compute_distance_to_utrecht(restaurants: Sequence[Restaurant]) -> None:
    """Compute and store the distance in kilometers from Utrecht city center."""

    for restaurant in restaurants:
        if restaurant.latitude is None or restaurant.longitude is None:
            continue
        restaurant.distance_km_from_utrecht = float(
            geodesic((restaurant.latitude, restaurant.longitude), UTRECHT_COORDINATES).kilometers
        )
