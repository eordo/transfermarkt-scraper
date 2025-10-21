import pandas as pd
from config import LEAGUE_CODES
from scraper import TransfermarktScraper


def main():
    data_dir = "data"
    for league in LEAGUE_CODES.keys():
        tm = TransfermarktScraper(league=league, enable_logging=True)
        for season in range(1992, 2025):
            df = pd.concat([tm.scrape(season, 's'), tm.scrape(season, 'w')])
            df = tm.clean(df)
            tm.save(df, filename=str(season), destination=data_dir)
    print("Done!")

if __name__ == "__main__":
    main()
