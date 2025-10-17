"""Configuration objects for the scraping pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Mapping, Optional

DEFAULT_BASE_URL = "https://www.diner-cadeau.nl"
DEFAULT_LIST_PATH = "/restaurant"
UTRECHT_COORDINATES = (52.0907, 5.1214)


@dataclass(slots=True)
class FetchSettings:
    """Settings that influence how listing pages are fetched."""

    base_url: str = DEFAULT_BASE_URL
    list_path: str = DEFAULT_LIST_PATH
    city: Optional[str] = None
    max_pages: int = 5
    request_timeout: int = 30
    pause_seconds: float = 1.0
    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
    )
    extra_headers: Mapping[str, str] = field(default_factory=dict)

    def headers(self) -> Dict[str, str]:
        headers = {"User-Agent": self.user_agent, "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8"}
        headers.update(self.extra_headers)
        return headers


@dataclass(slots=True)
class GeocodeSettings:
    """Settings used to geocode addresses and compute distances."""

    provider_url: str = "https://nominatim.openstreetmap.org/search"
    email: Optional[str] = None
    pause_seconds: float = 1.0
    timeout: int = 30

    def query_params(self, query: str) -> Dict[str, str]:
        params = {"format": "jsonv2", "q": query, "limit": "1"}
        if self.email:
            params["email"] = self.email
        return params


@dataclass(slots=True)
class PipelineSettings:
    """Composite settings structure for the pipeline."""

    fetch: FetchSettings = field(default_factory=FetchSettings)
    geocode: GeocodeSettings = field(default_factory=GeocodeSettings)
    output_csv: str = "dinercadeau_restaurants.csv"
    append: bool = False
    include_ratings: bool = True
    include_geocoding: bool = True


def build_listing_url(settings: FetchSettings, page: int) -> str:
    """Return the absolute URL for a listing page."""

    from urllib.parse import urlencode, urljoin

    query = {}
    if page > 1:
        query["page"] = page
    if settings.city:
        query["plaats"] = settings.city

    base = urljoin(settings.base_url, settings.list_path)
    if not query:
        return base
    return f"{base}?{urlencode(query)}"


def default_output_fields() -> Iterable[str]:
    """Return the column names used when exporting to CSV."""

    return [
        "name",
        "url",
        "city",
        "address",
        "postal_code",
        "country",
        "description",
        "tags",
        "price_range",
        "rating",
        "review_count",
        "latitude",
        "longitude",
        "distance_km_from_utrecht",
        "source",
        "scraped_at",
    ]
