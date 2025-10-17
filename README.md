# DinerCadeau Restaurant Index Builder

This repository contains a lightweight scraping toolkit for building a local
index of restaurants listed on [DinerCadeau](https://www.diner-cadeau.nl). It
fetches paginated restaurant listings, parses the available metadata (name,
address, description, tags, ratings, etc.), optionally geocodes missing
coordinates, and computes the distance to Utrecht city centre.

> **Important**: Before running the scraper against the live website, make sure
> you have permission to do so and that your usage complies with the platform's
> terms of service and the terms of any third-party APIs you invoke.

## Features

* Fetch DinerCadeau listing pages with configurable pagination and city
  filtering.
* Parse rich metadata from structured data (`ld+json`) or Nuxt bootstrap
  payloads.
* Optional geocoding (via OpenStreetMap Nominatim) for restaurants missing
  coordinates.
* Computes the geodesic distance from Utrecht city centre.
* Exports normalized data to a CSV file that you can further analyse or import
  into spreadsheets.

## Quick start

Create a Python virtual environment (Python 3.11+) and install the
requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the CLI to build your dataset:

```bash
python -m dinercadeau_restaurants.cli \
  --city Utrecht \
  --max-pages 3 \
  --output data/utrecht_restaurants.csv \
  --email you@example.com
```

The script prints progress to the console and writes the resulting CSV file to
`data/utrecht_restaurants.csv`.

### Configuration file

Instead of passing flags, you can provide a JSON configuration file:

```json
{
  "fetch": {
    "city": "Utrecht",
    "max_pages": 5,
    "pause_seconds": 1.5
  },
  "geocode": {
    "email": "you@example.com",
    "pause_seconds": 1.0
  }
}
```

Save the configuration (e.g. to `config.json`) and run:

```bash
python -m dinercadeau_restaurants.cli --config config.json
```

### Output schema

Each CSV row contains the following columns:

| Column | Description |
| --- | --- |
| `name` | Restaurant name |
| `url` | Direct link to the restaurant page on DinerCadeau |
| `city` | City where the restaurant is located |
| `address` | Street address |
| `postal_code` | Postal code |
| `country` | Country (defaults to Netherlands) |
| `description` | Short textual description |
| `tags` | Semicolon-separated list of cuisine/style tags |
| `price_range` | Price range when available |
| `rating` | Average rating parsed from the source data |
| `review_count` | Number of reviews contributing to the rating |
| `latitude` | Latitude in decimal degrees |
| `longitude` | Longitude in decimal degrees |
| `distance_km_from_utrecht` | Geodesic distance from Utrecht city centre |
| `source` | The data source identifier (`diner-cadeau`) |
| `scraped_at` | UTC timestamp when the data was scraped |

### Respect rate limits

The default configuration introduces a one-second pause between HTTP requests.
If you crawl many pages or perform geocoding, consider increasing this delay to
avoid overwhelming the upstream services. For Nominatim usage, provide a contact
email via `--email` or the configuration file, as required by their usage
policy.

## Development

* Lint: `ruff check .`
* Formatting: `ruff format .`
* Type checking: `mypy src`

These tools are not pinned as dependencies but recommended for contribution
workflows.
