"""HTML parsing helpers for DinerCadeau listing pages."""

from __future__ import annotations

import json
import logging
import re
from typing import Iterable, Iterator, List, Optional

from bs4 import BeautifulSoup

from .models import Restaurant, merge_tags

logger = logging.getLogger(__name__)

SCRIPT_JSON_RE = re.compile(r"window\.__NUXT__\s*=\s*(\{.*\})", re.DOTALL)


def _iter_ld_json(soup: BeautifulSoup) -> Iterator[dict]:
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = node.string
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.debug("Failed to decode ld+json block", exc_info=True)
            continue
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item


def _parse_rating(data: dict) -> tuple[Optional[float], Optional[int]]:
    rating = data.get("aggregateRating") or {}
    value = rating.get("ratingValue")
    count = rating.get("reviewCount") or rating.get("ratingCount")
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        value_f = None
    try:
        count_i = int(count)
    except (TypeError, ValueError):
        count_i = None
    return value_f, count_i


def _parse_offer(data: dict) -> tuple[Optional[str], Optional[str]]:
    offers = data.get("offers")
    if isinstance(offers, dict):
        price = offers.get("price")
        currency = offers.get("priceCurrency")
        if price and currency:
            return f"{price} {currency}", None
        if price:
            return str(price), None
    return None, None


def _extract_from_ld_json(data: dict) -> Optional[Restaurant]:
    if data.get("@type") not in {"Restaurant", "FoodEstablishment"}:
        return None
    name = data.get("name")
    if not name:
        return None
    url = data.get("url") or data.get("@id")
    if not url:
        return None
    address_data = data.get("address") or {}
    if isinstance(address_data, list) and address_data:
        address_data = address_data[0]
    if not isinstance(address_data, dict):
        address_data = {}

    description = data.get("description") or data.get("disambiguatingDescription")
    rating_value, review_count = _parse_rating(data)
    price_range, _ = _parse_offer(data)

    tags = []
    cuisines = data.get("servesCuisine")
    if isinstance(cuisines, str):
        tags.append(cuisines)
    elif isinstance(cuisines, list):
        tags.extend(str(item) for item in cuisines)

    return Restaurant(
        name=name,
        url=url,
        city=address_data.get("addressLocality"),
        address=address_data.get("streetAddress"),
        postal_code=address_data.get("postalCode"),
        country=address_data.get("addressCountry") or "Netherlands",
        description=description,
        tags=merge_tags(tags, [data.get("@type", ""), data.get("category", "")]),
        price_range=price_range or data.get("priceRange"),
        rating=rating_value,
        review_count=review_count,
        latitude=_safe_float(data.get("latitude")),
        longitude=_safe_float(data.get("longitude")),
    )


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_listing_page(html: str) -> List[Restaurant]:
    """Parse a single listing page into restaurant entries."""

    soup = BeautifulSoup(html, "html.parser")
    restaurants: List[Restaurant] = []

    # First, attempt to leverage structured data provided via ld+json blocks.
    for data in _iter_ld_json(soup):
        restaurant = _extract_from_ld_json(data)
        if restaurant:
            restaurants.append(restaurant)

    if restaurants:
        logger.debug("Extracted %d restaurants from ld+json blocks", len(restaurants))
        return restaurants

    # As a fallback, attempt to parse Nuxt bootstrap data.
    script_text = None
    for script in soup.find_all("script"):
        if not script.string:
            continue
        match = SCRIPT_JSON_RE.search(script.string)
        if match:
            script_text = match.group(1)
            break
    if script_text:
        try:
            payload = json.loads(script_text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse Nuxt payload", exc_info=True)
        else:
            restaurants.extend(_parse_nuxt_payload(payload))

    return restaurants


def _parse_nuxt_payload(payload: dict) -> Iterable[Restaurant]:
    """Parse a subset of the Nuxt payload structure used by the website."""

    if not isinstance(payload, dict):
        return []

    data = payload.get("data") or []
    results: List[Restaurant] = []

    def parse_entry(entry: dict) -> Optional[Restaurant]:
        if not isinstance(entry, dict):
            return None
        name = entry.get("name") or entry.get("title")
        if not name:
            return None
        url = entry.get("slug") or entry.get("url")
        if url and not url.startswith("http"):
            url = f"https://www.diner-cadeau.nl{url}"
        description = entry.get("excerpt") or entry.get("description")
        tags = entry.get("categories") or entry.get("labels") or []
        if isinstance(tags, dict):
            tags = tags.values()
        elif isinstance(tags, str):
            tags = [tags]
        rating = entry.get("rating") or entry.get("score")
        review_count = entry.get("reviews") or entry.get("review_count")
        location = entry.get("location") or {}
        if not isinstance(location, dict):
            location = {}

        return Restaurant(
            name=name,
            url=url or "",
            city=location.get("city"),
            address=location.get("address"),
            postal_code=location.get("postal_code"),
            description=description,
            tags=merge_tags(tags),
            rating=_safe_float(rating),
            review_count=_safe_int(review_count),
            latitude=_safe_float(location.get("lat")),
            longitude=_safe_float(location.get("lng")),
        )

    for item in data:
        if isinstance(item, dict):
            results.extend(filter(None, (parse_entry(entry) for entry in item.get("restaurants", []))))
            candidate = parse_entry(item)
            if candidate:
                results.append(candidate)

    return results


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
