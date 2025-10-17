"""Data models used throughout the scraping pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional


@dataclass(slots=True)
class Restaurant:
    """Normalized representation of a restaurant entry."""

    name: str
    url: str
    city: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    country: str = "Netherlands"
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    price_range: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_km_from_utrecht: Optional[float] = None
    source: str = "diner-cadeau"
    scraped_at: datetime = field(default_factory=datetime.utcnow)

    def as_row(self) -> List[str]:
        """Return the restaurant as a CSV row using primitive types."""

        return [
            self.name,
            self.url,
            self.city or "",
            self.address or "",
            self.postal_code or "",
            self.country,
            self.description or "",
            ";".join(self.tags),
            self.price_range or "",
            "" if self.rating is None else f"{self.rating:.2f}",
            "" if self.review_count is None else str(self.review_count),
            "" if self.latitude is None else f"{self.latitude:.6f}",
            "" if self.longitude is None else f"{self.longitude:.6f}",
            "" if self.distance_km_from_utrecht is None else f"{self.distance_km_from_utrecht:.3f}",
            self.source,
            self.scraped_at.isoformat(),
        ]


@dataclass(slots=True)
class ListingPage:
    """Metadata about a listing page that was scraped."""

    url: str
    page_number: int
    html: str
    fetched_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(slots=True)
class GeocodeResult:
    """Result returned by a geocoding provider."""

    address: str
    latitude: float
    longitude: float
    raw: dict


def merge_tags(*sources: Iterable[str]) -> List[str]:
    """Return a normalized list of unique tags preserving insertion order."""

    seen = set()
    result: List[str] = []
    for iterable in sources:
        for tag in iterable:
            normalized = tag.strip()
            if not normalized:
                continue
            lower = normalized.lower()
            if lower in seen:
                continue
            seen.add(lower)
            result.append(normalized)
    return result
