"""HTML parsing helpers for DinerCadeau listing pages."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from bs4 import BeautifulSoup
from urllib.parse import urljoin

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

    # As a fallback, attempt to parse the Nuxt bootstrap data which is exposed
    # either as ``window.__NUXT__`` or inside ``<script type="application/json"``
    # blocks (Nuxt 3's ``__NUXT_DATA__`` payloads).
    seen_urls: set[str] = set()
    for payload in _iter_script_payloads(soup):
        for restaurant in _parse_nuxt_payload(payload):
            if restaurant.url and restaurant.url not in seen_urls:
                restaurants.append(restaurant)
                seen_urls.add(restaurant.url)

    return restaurants


def _iter_script_payloads(soup: BeautifulSoup) -> Iterator[Any]:
    for script in soup.find_all("script"):
        text = script.string or script.get_text()
        if not text:
            continue

        data: Any = None
        if script.get("type") == "application/json":
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                logger.debug("Failed to decode JSON script block", exc_info=True)
                continue
        else:
            match = SCRIPT_JSON_RE.search(text)
            if match:
                try:
                    data = json.loads(match.group(1))
                except json.JSONDecodeError:
                    logger.debug("Failed to decode window.__NUXT__ payload", exc_info=True)
                    continue
        if data is not None:
            yield data


def _parse_nuxt_payload(payload: Any) -> Iterable[Restaurant]:
    """Parse a subset of the Nuxt payload structure used by the website."""

    results: List[Restaurant] = []

    for entry in _iter_candidate_dicts(payload):
        restaurant = _convert_candidate(entry)
        if restaurant:
            results.append(restaurant)

    return results


def _iter_candidate_dicts(payload: Any) -> Iterator[Dict[str, Any]]:
    stack: List[Any] = [payload]
    seen: set[int] = set()

    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            ident = id(current)
            if ident in seen:
                continue
            seen.add(ident)
            if _looks_like_restaurant(current):
                yield current
            stack.extend(current.values())
        elif isinstance(current, (list, tuple, set)):
            stack.extend(current)


def _looks_like_restaurant(entry: Dict[str, Any]) -> bool:
    name = entry.get("name") or entry.get("title")
    if not name:
        return False
    if not any(key in entry for key in ("slug", "url", "link", "permalink")):
        return False
    if not any(key in entry for key in ("address", "location", "city", "plaats")):
        return False
    return True


def _convert_candidate(entry: Dict[str, Any]) -> Optional[Restaurant]:
    name = entry.get("name") or entry.get("title")
    if not name:
        return None

    url = entry.get("url") or entry.get("permalink") or entry.get("link") or entry.get("slug")
    if url:
        if isinstance(url, dict):
            url = url.get("href") or url.get("url")
        if isinstance(url, Sequence) and not isinstance(url, (str, bytes)):
            url = next((item for item in url if isinstance(item, str)), None)
    if isinstance(url, str) and not url.startswith("http"):
        url = urljoin("https://www.diner-cadeau.nl", url)

    description = entry.get("excerpt") or entry.get("description") or entry.get("intro")

    tags_source: List[str] = []
    for key in ("categories", "labels", "tags", "cuisines"):
        value = entry.get(key)
        tags_source.extend(_normalize_iterable(value))

    rating = entry.get("rating") or entry.get("score") or entry.get("averageRating")
    review_count = entry.get("reviews") or entry.get("review_count") or entry.get("ratingCount")

    location = _coerce_location(entry)

    return Restaurant(
        name=name,
        url=url or "",
        city=location.get("city") or location.get("plaats"),
        address=location.get("address") or location.get("street") or location.get("streetAddress"),
        postal_code=location.get("postal_code") or location.get("postalCode") or location.get("zip") or location.get("zipcode"),
        description=description,
        tags=merge_tags(tags_source),
        rating=_safe_float(rating),
        review_count=_safe_int(review_count),
        latitude=_safe_float(location.get("lat") or location.get("latitude")),
        longitude=_safe_float(location.get("lng") or location.get("longitude")),
    )


def _normalize_iterable(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [str(item) for item in value.values()]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return [str(value)]


def _coerce_location(entry: Dict[str, Any]) -> Dict[str, Any]:
    location = entry.get("location") or entry.get("address") or {}
    if isinstance(location, list) and location:
        location = location[0]
    if not isinstance(location, dict):
        location = {}
    return location


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
