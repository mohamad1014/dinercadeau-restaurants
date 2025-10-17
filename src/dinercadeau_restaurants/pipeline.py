"""End-to-end pipeline that builds the restaurant index."""

from __future__ import annotations

import csv
import logging
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Optional

from .fetcher import DinerCadeauFetcher
from .geocode import NominatimGeocoder, annotate_with_coordinates, compute_distance_to_utrecht
from .models import Restaurant
from .parser import parse_listing_page
from .settings import PipelineSettings, default_output_fields

logger = logging.getLogger(__name__)


def run_pipeline(settings: Optional[PipelineSettings] = None) -> List[Restaurant]:
    """Run the scraping pipeline using the provided settings."""

    settings = settings or PipelineSettings()
    fetcher = DinerCadeauFetcher(settings.fetch)

    restaurants: List[Restaurant] = []
    for page in fetcher.iter_pages():
        parsed = parse_listing_page(
            page.html,
            city=fetcher.settings.city,
            page_number=page.page_number,
            page_url=page.url,
            session=fetcher.session,
        )
        logger.info("Parsed %d restaurants from %s", len(parsed), page.url)
        restaurants.extend(parsed)

    restaurants = deduplicate_restaurants(restaurants)
    logger.info("Retained %d unique restaurants after de-duplication", len(restaurants))

    if settings.include_geocoding:
        geocoder = NominatimGeocoder(settings.geocode)
        annotate_with_coordinates(restaurants, geocoder=geocoder)

    compute_distance_to_utrecht(restaurants)

    if settings.output_csv:
        write_to_csv(restaurants, settings.output_csv, append=settings.append)

    return restaurants


def write_to_csv(restaurants: Iterable[Restaurant], path: str | Path, append: bool = False) -> None:
    """Persist restaurants to CSV."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if append and path.exists() else "w"
    write_header = mode == "w"
    count = 0

    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(default_output_fields())
        for restaurant in restaurants:
            writer.writerow(restaurant.as_row())
            count += 1

    logger.info("Wrote %d rows to %s", count, path)


def deduplicate_restaurants(restaurants: Iterable[Restaurant]) -> List[Restaurant]:
    """Return restaurants with duplicates (by URL) removed while preserving order."""

    seen: OrderedDict[str, Restaurant] = OrderedDict()
    for restaurant in restaurants:
        key = restaurant.url.strip().lower() if restaurant.url else restaurant.name.lower()
        if key not in seen:
            seen[key] = restaurant
    return list(seen.values())
