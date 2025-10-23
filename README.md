# Transfermarkt web scraper

This script web scrapes [Transfermarkt](https://www.transfermarkt.com/) for club transfer data.
See the [data repository](https://github.com/eordo/transfermarkt-data) for CSVs of the scraped data.

## Setup

The scraper is most easily run with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run main.py
```

Follow the text prompts to select the desired leagues and seasons.
The scraped and cleaned data will be written to CSVs in a created `data` directory.

If not using uv, install the following dependencies by any method:

- BeautifulSoup
- httpx
- Pandas
- questionary

## Disclaimer

The scraper is purely for demonstration and should not be put into production.
