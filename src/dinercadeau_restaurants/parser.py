"""HTML parsing helpers for DinerCadeau listing pages."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import Restaurant, merge_tags
from .settings import FetchSettings

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 50
_DATASET_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_CHUNK_HASH_CACHE: Dict[str, Dict[int, str]] = {}
_CONTEXT_CACHE: Dict[str, Dict[str, Tuple[int, int]]] = {}
_SESSION: Optional[requests.Session] = None


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


def parse_listing_page(
    html: str,
    *,
    city: Optional[str] = None,
    page_number: Optional[int] = None,
    page_url: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[Restaurant]:
    """Parse a single listing page into restaurant entries.

    The current production site exposes a ``dc-live`` dataset as a lazily loaded
    Next.js chunk.  We mirror the browser's behaviour by locating the relevant
    assets, downloading the JSON payload once, and slicing it locally to satisfy
    pagination requests.  If the chunk can not be resolved, the legacy parsers
    remain as a defensive fallback.
    """

    soup = BeautifulSoup(html, "html.parser")
    base_url = _infer_base_url(page_url)

    restaurants: List[Restaurant] = []
    try:
        http = _ensure_session(session)
        dataset = _load_dc_live_dataset(soup, base_url, http)
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("Falling back to legacy parsers", exc_info=True)
    else:
        entries = _filter_dataset(dataset, city=city, page_number=page_number)
        restaurants = [item for item in (_convert_entry(entry, base_url) for entry in entries) if item]
        logger.debug("Extracted %d restaurants from dc-live dataset", len(restaurants))
        return restaurants

    # Legacy fallback: ld+json and embedded payloads.
    for data in _iter_ld_json(soup):
        restaurant = _extract_from_ld_json(data)
        if restaurant:
            restaurants.append(restaurant)

    if restaurants:
        logger.debug("Extracted %d restaurants from ld+json blocks", len(restaurants))
        return restaurants

    seen_urls: set[str] = set()
    for payload in _iter_script_payloads(soup):
        for restaurant in _parse_nuxt_payload(payload):
            if restaurant.url and restaurant.url not in seen_urls:
                restaurants.append(restaurant)
                seen_urls.add(restaurant.url)

    return restaurants


def _ensure_session(session: Optional[requests.Session]) -> requests.Session:
    if session is not None:
        return session
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(FetchSettings().headers())
    return _SESSION


def _infer_base_url(page_url: Optional[str]) -> str:
    if not page_url:
        return FetchSettings().base_url
    parsed = urlparse(page_url)
    if not parsed.scheme or not parsed.netloc:
        return FetchSettings().base_url
    return f"{parsed.scheme}://{parsed.netloc}"


def _filter_dataset(
    dataset: List[Dict[str, Any]],
    *,
    city: Optional[str],
    page_number: Optional[int],
) -> List[Dict[str, Any]]:
    entries = dataset
    if city:
        target = city.strip().lower()
        entries = [entry for entry in entries if (entry_city := (entry.get("city") or "").strip().lower()) == target]
    if page_number and page_number > 0:
        start = (page_number - 1) * _DEFAULT_PAGE_SIZE
        end = start + _DEFAULT_PAGE_SIZE
        entries = entries[start:end]
    return entries


def _load_dc_live_dataset(soup: BeautifulSoup, base_url: str, session: requests.Session) -> List[Dict[str, Any]]:
    page_chunk_url, webpack_url = _discover_asset_urls(soup, base_url)
    if page_chunk_url in _DATASET_CACHE:
        return _DATASET_CACHE[page_chunk_url]

    context = _load_dataset_context(page_chunk_url, session)
    dataset_entry = context.get("./dc-live.json")
    if not dataset_entry:
        raise ValueError("dc-live dataset mapping not present in page chunk")

    module_id, chunk_id = dataset_entry
    chunk_hashes = _load_chunk_hashes(webpack_url, session)
    chunk_hash = chunk_hashes.get(chunk_id)
    if not chunk_hash:
        raise ValueError(f"Chunk hash missing for dataset chunk {chunk_id}")

    chunk_url = urljoin(base_url, f"/_next/static/chunks/{chunk_id}.{chunk_hash}.js")
    chunk_source = _fetch_text(chunk_url, session)
    dataset = _parse_dataset_chunk(chunk_source, module_id)
    _DATASET_CACHE[page_chunk_url] = dataset
    return dataset


def _discover_asset_urls(soup: BeautifulSoup, base_url: str) -> Tuple[str, str]:
    webpack_url: Optional[str] = None
    page_chunk_url: Optional[str] = None
    for script in soup.find_all("script", src=True):
        src = script["src"]
        if "static/chunks/webpack-" in src:
            webpack_url = urljoin(base_url, src)
        elif "static/chunks/pages/%5Blayout%5D/%5B%5B...slug%5D%5D" in src:
            page_chunk_url = urljoin(base_url, src)
        if webpack_url and page_chunk_url:
            break
    if not webpack_url or not page_chunk_url:
        raise ValueError("Unable to locate Next.js assets in listing HTML")
    return page_chunk_url, webpack_url


def _load_dataset_context(page_chunk_url: str, session: requests.Session) -> Dict[str, Tuple[int, int]]:
    if page_chunk_url in _CONTEXT_CACHE:
        return _CONTEXT_CACHE[page_chunk_url]
    source = _fetch_text(page_chunk_url, session)
    target = '{"./dc-live.json":'
    marker_index = source.find(target)
    if marker_index == -1:
        raise ValueError("Dataset context block not found in page chunk")
    brace_count = 0
    index = marker_index
    end_index = -1
    while index < len(source):
        char = source[index]
        if char == "{":
            brace_count += 1
        elif char == "}":
            brace_count -= 1
            if brace_count == 0:
                end_index = index
                break
        index += 1
    if end_index == -1:
        raise ValueError("Dataset context object boundaries not determined")
    context_fragment = source[marker_index : end_index + 1]
    context_data = json.loads(context_fragment)
    context_map = {key: (value[0], value[1]) for key, value in context_data.items()}
    _CONTEXT_CACHE[page_chunk_url] = context_map
    return context_map


def _load_chunk_hashes(webpack_url: str, session: requests.Session) -> Dict[int, str]:
    if webpack_url in _CHUNK_HASH_CACHE:
        return _CHUNK_HASH_CACHE[webpack_url]
    source = _fetch_text(webpack_url, session)
    prefix = 'return"static/chunks/"+e+"."+('
    prefix_index = source.find(prefix)
    if prefix_index == -1:
        raise ValueError("Chunk hash mapping not found in webpack runtime")
    brace_start = source.find("{", prefix_index)
    if brace_start == -1:
        raise ValueError("Chunk hash object start not found")
    brace_count = 0
    index = brace_start
    end_index = -1
    while index < len(source):
        char = source[index]
        if char == "{":
            brace_count += 1
        elif char == "}":
            brace_count -= 1
            if brace_count == 0:
                end_index = index
                break
        index += 1
    if end_index == -1:
        raise ValueError("Chunk hash object boundaries not determined")
    mapping_text = source[brace_start + 1 : end_index]
    mapping: Dict[int, str] = {}
    for entry in mapping_text.split(","):
        if not entry.strip():
            continue
        key_part, value_part = entry.split(":")
        key_int = int(key_part.strip())
        value = value_part.strip()
        if value.endswith("}"):
            value = value[:-1]
        mapping[key_int] = value.strip().strip('"')
    _CHUNK_HASH_CACHE[webpack_url] = mapping
    return mapping


def _fetch_text(url: str, session: requests.Session) -> str:
    response = session.get(url, timeout=FetchSettings().request_timeout)
    response.raise_for_status()
    return response.text


def _parse_dataset_chunk(source: str, module_id: int) -> List[Dict[str, Any]]:
    marker = f"{module_id}:"
    start_index = source.find(marker)
    if start_index == -1:
        raise ValueError(f"Dataset module {module_id} not present in chunk")
    module_source = source[start_index:]
    prefix = "e.exports=JSON.parse("
    prefix_index = module_source.find(prefix)
    if prefix_index == -1:
        raise ValueError("JSON.parse payload not found in dataset chunk")
    literal_start = start_index + prefix_index + len(prefix)
    quote = source[literal_start]
    if quote not in {"'", '"'}:
        raise ValueError("Unexpected string delimiter in dataset chunk")
    raw_json, _ = _extract_js_string(source, literal_start, quote)
    json_text = bytes(raw_json, "utf-8").decode("unicode_escape")
    data = json.loads(json_text)
    if not isinstance(data, list):
        raise ValueError("Dataset payload was not a list")
    return data  # type: ignore[return-value]


def _extract_js_string(source: str, start: int, quote: str) -> Tuple[str, int]:
    buffer: List[str] = []
    index = start + 1
    length = len(source)
    while index < length:
        char = source[index]
        if char == quote:
            backslashes = 0
            lookbehind = index - 1
            while lookbehind >= start and source[lookbehind] == "\\":
                backslashes += 1
                lookbehind -= 1
            if backslashes % 2 == 0:
                return "".join(buffer), index
        buffer.append(char)
        index += 1
    raise ValueError("Unterminated JavaScript string literal")


def _convert_entry(entry: Dict[str, Any], base_url: str) -> Optional[Restaurant]:
    title = entry.get("title")
    link = entry.get("linkHref")
    if not title or not link:
        return None
    url = urljoin(base_url, link)
    description = _extract_description(entry)
    website = entry.get("websiteLinkHref")
    tag_sources: List[Iterable[str]] = []
    label = entry.get("label")
    if isinstance(label, str):
        tag_sources.append([label])
    region = entry.get("region")
    if isinstance(region, str):
        tag_sources.append([region])
    meta_tags = _coerce_tags(entry.get("metaData"))
    if meta_tags:
        tag_sources.append(meta_tags)
    if isinstance(website, str):
        tag_sources.append([website])
    tags = merge_tags(*tag_sources)
    country = entry.get("country") or "Netherlands"
    if country == "Nederland":
        country = "Netherlands"
    latitude = _safe_float(entry.get("latitude"))
    longitude = _safe_float(entry.get("longitude"))
    return Restaurant(
        name=str(title),
        url=url,
        city=entry.get("city"),
        address=entry.get("streetAndHouseNumber"),
        postal_code=entry.get("postcode"),
        country=country,
        description=description,
        tags=tags,
        price_range=None,
        rating=None,
        review_count=None,
        latitude=latitude,
        longitude=longitude,
    )


def _extract_description(entry: Dict[str, Any]) -> Optional[str]:
    description = entry.get("description")
    if not isinstance(description, dict):
        return None
    contents = description.get("content")
    if not isinstance(contents, list):
        return None
    fragments: List[str] = []
    for block in contents:
        block_content = block.get("content") if isinstance(block, dict) else None
        if not isinstance(block_content, list):
            continue
        for node in block_content:
            text = node.get("text") if isinstance(node, dict) else None
            if text:
                fragments.append(str(text))
    if not fragments:
        return None
    html = "\n".join(fragments)
    soup = BeautifulSoup(html, "html.parser")
    text_content = soup.get_text(" ", strip=True)
    return text_content or None


def _coerce_tags(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [str(item) for item in value.values() if isinstance(item, str)]
    if isinstance(value, (list, tuple, set)):
        tags: List[str] = []
        for item in value:
            tags.extend(_coerce_tags(item))
        return tags
    return [str(value)]


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
