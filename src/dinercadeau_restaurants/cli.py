"""Command line interface for building the restaurant index."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict

from .pipeline import run_pipeline
from .settings import FetchSettings, GeocodeSettings, PipelineSettings

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local DinerCadeau restaurant index")
    parser.add_argument("--city", help="Filter listings by city", default=None)
    parser.add_argument("--max-pages", type=int, default=None, help="Number of listing pages to crawl")
    parser.add_argument("--output", type=Path, default=Path("dinercadeau_restaurants.csv"), help="Output CSV path")
    parser.add_argument("--no-geocoding", action="store_true", help="Skip geocoding lookups")
    parser.add_argument("--config", type=Path, help="Optional JSON file overriding settings")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--email", type=str, default=None, help="Contact email passed to the geocoding provider")
    parser.add_argument(
        "--pause",
        type=float,
        default=None,
        help="Seconds to wait between HTTP requests (overrides config)",
    )
    return parser.parse_args(argv)


def load_config(path: Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _configure_logging(args.verbose)

    config = load_config(args.config)

    fetch_config = dict(config.get("fetch", {}))
    geocode_config = dict(config.get("geocode", {}))

    if args.city is not None:
        fetch_config["city"] = args.city
    if args.max_pages is not None:
        fetch_config["max_pages"] = args.max_pages
    if args.pause is not None:
        fetch_config["pause_seconds"] = args.pause

    if args.email is not None:
        geocode_config["email"] = args.email

    fetch_settings = FetchSettings(**fetch_config)
    geocode_settings = GeocodeSettings(**geocode_config)

    pipeline_settings = PipelineSettings(
        fetch=fetch_settings,
        geocode=geocode_settings,
        output_csv=str(args.output),
        include_geocoding=not args.no_geocoding,
    )

    run_pipeline(pipeline_settings)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
