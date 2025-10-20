import pandas as pd
from scraper import TransfermarktScraper


def main():
    data_dir = "data"
    tm = TransfermarktScraper(league='premier-league', enable_logging=True)
    df = pd.concat([tm.scrape(2025, 's'), tm.scrape(2025, 'w')])
    df = tm.clean(df)
    tm.save(df, filename='2025', destination=data_dir)

if __name__ == "__main__":
    main()
