import pandas as pd
import questionary
from config import LEAGUE_CODES, LEAGUE_NAMES
from scraper import TransfermarktScraper


def main():
    def _parse_years(input_text, min_year=1992, max_year=2025):
        years = set()
        parts = input_text.replace(' ', '').split(',')
        for part in parts:
            if not part:
                continue
            if '-' in part:
                start, end = map(int, part.split('-'))
                years.update(
                    range(max(start, min_year), min(end, max_year) + 1)
                )
            elif part.isdigit():
                year = int(part)
                if min_year <= year <= max_year:
                    years.add(year)
        return sorted(years)

    def _validate_years_input(text):
        error_message = (
            "Enter years separated by a comma and space "
            "or a range of years"
        )
        try:
            years = _parse_years(text)
            return bool(years) or text.strip() == '' or error_message
        except ValueError:
            return error_message


    # Set data directory.
    data_dir = "data"

    # Select leagues to scrape.
    print(f"The scraper currently supports {len(LEAGUE_NAMES)} leagues.")
    while True:
        selected_leagues = questionary.checkbox(
            "Select leagues to scrape:",
            choices=[
                {'name': f'{LEAGUE_CODES[k]:<3}  {v}', 'value': k}
                for k, v in LEAGUE_NAMES.items()
            ]
        ).ask()
        if selected_leagues: break
        print("Please select at least one league.")

    # Select seasons to scrape.
    print((
        "\nThe scraper is supported from the 1992-93 season and onward. To "
        "select a season, enter the year in which the league season began."
    ))
    while True:
        selected_seasons = _parse_years(questionary.text(
            message="Select seasons to scrape, e.g. 1994, 2016, 2000-2009:",
            validate=_validate_years_input
        ).ask())
        if selected_seasons: break
        print("Please select at least one season.")

    print("\nThe following league(s) will be scraped:")
    print(', '.join([LEAGUE_NAMES[league] for league in selected_leagues]))
    print("\nFor the following season(s):")
    print(', '.join(map(str, selected_seasons)), '\n')
    proceed = questionary.confirm("Continue?").ask()
    if not proceed:
        print("Aborting.")
        return

    print((
        f"\nNow scraping {len(selected_seasons)} season(s) "
        f"for {len(selected_leagues)} league(s)."
    ))
    for league in selected_leagues:
        tm = TransfermarktScraper(league=league, enable_logging=True)
        for season in selected_seasons:
            df = pd.concat([tm.scrape(season, 's'), tm.scrape(season, 'w')])
            df = tm.clean(df)
            tm.save(df, filename=str(season), destination=data_dir)
    print("Done!")

if __name__ == "__main__":
    main()
