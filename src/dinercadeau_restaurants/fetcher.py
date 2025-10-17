"""HTTP client utilities for downloading DinerCadeau listing pages."""

from __future__ import annotations

import logging
import time
from typing import Iterable, Iterator, Optional

import requests

from .models import ListingPage
from .settings import FetchSettings, build_listing_url

logger = logging.getLogger(__name__)


class DinerCadeauFetcher:
    """Fetch listing pages using ``requests`` with minimal rate limiting."""

    def __init__(self, settings: Optional[FetchSettings] = None) -> None:
        self.settings = settings or FetchSettings()
        self._session = requests.Session()
        self._session.headers.update(self.settings.headers())

    @property
    def session(self) -> requests.Session:
        """Return the HTTP session used to fetch listing pages."""

        return self._session

    def fetch(self, page: int) -> ListingPage:
        """Fetch a single listing page and return metadata."""

        url = build_listing_url(self.settings, page)
        logger.debug("Fetching page %s", url)
        response = self._session.get(url, timeout=self.settings.request_timeout)
        response.raise_for_status()
        return ListingPage(url=url, page_number=page, html=response.text)

    def iter_pages(self) -> Iterator[ListingPage]:
        """Yield listing pages until ``max_pages`` is reached."""

        for page in range(1, self.settings.max_pages + 1):
            listing = self.fetch(page)
            yield listing
            if page < self.settings.max_pages:
                pause = max(0.0, self.settings.pause_seconds)
                if pause:
                    logger.debug("Sleeping %.2f seconds between requests", pause)
                    time.sleep(pause)

    def fetch_all(self) -> Iterable[ListingPage]:
        """Convenience wrapper returning a list of listing pages."""

        return list(self.iter_pages())
