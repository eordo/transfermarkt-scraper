import random
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from config import BASE_URL, USER_AGENTS, LEAGUE_CODES


class TransfermarktScraper:
    """A web scraper for Transfermarkt transfers."""
    # URL query keys are in German.
    _Q_SEASON = 'saison_id'
    _Q_WINDOW = 's_w'
    _Q_LOANS = 'leihe'
    _Q_INTERNAL = 'intern'

    def __init__(self, league='premier-league', timeout=30.0):
        """
        Initialize a TransfermarktScraper.

        The league name must match how it appears in Transfermarkt URLs and
        is checked against the config file.

        Args:
            league (str): Name of the league.
            timeout (float): How long to wait for a response.
        """
        if league not in LEAGUE_CODES.keys():
            raise ValueError(f"{league} not found or is not supported.")
        self._league = league
        self._level = LEAGUE_CODES[league]
        self._origin = urljoin(
            BASE_URL,
            f"{self._league}/transfers/wettbewerb/{self._level}"
        )
        self._client = httpx.Client(timeout=timeout)

    def __str__(self):
        return f"League: {self._league}, Code: {self._level}"

    def save(self, df, filename, destination='.'):
        """
        Save data to a CSV.

        Args:
            df (pd.DataFrame): Data to save.
            filename (str): Name of CSV file without the extension.
            destination (pathlike): Where to save the data.
        """
        output_dir = Path(destination) / self._league.replace('-', '_')
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_dir / f"{filename}.csv", index=False, encoding='utf-8')

    def scrape(self, season, window, loans=True, internal=False, max_retries=5):
        """
        Scrape a Transfermarkt transfers page and return the parsed data.

        Loan transfers do not include players returning to their parent club
        at the end of a loan. Internal transfers are movements involving a
        club's youth or reserve teams.

        Args:
            season (int): Year in which the league season begins.
            window (str): One of 's' (summer) or 'w' (winter).
            loans (bool): Include loan transfers.
            internal (bool): Include transfers within clubs.
            max_retries (int): Maximum number of request retry attempts.

        Returns:
            pd.DataFrame: Transfermarkt transfers data.
        """
        url = self._build_url(season, window, loans, internal)
        soup = self._get_page_soup(url, max_retries=max_retries)
        df = self._soup_to_df(soup, window)
        return df

    def _build_url(self, season, window, loans=True, internal=False):
        """
        Build the URL for a Transfermarkt transfer window summary page.

        Args:
            season (int): Year in which the league season begins.
            window (str): One of 's' (summer) or 'w' (winter).
            loans (bool): Include loan transfers.
            internal (bool): Include transfers within clubs.

        Returns:
            str: Transfermarkt URL.
        """
        if not float(season).is_integer():
            raise ValueError("Season must be a year.")
        if window not in ('s', 'w'):
            raise ValueError("Window must be one of 's' or 'w'.")
        params = {
            self._Q_SEASON: season,
            self._Q_WINDOW: window,
            self._Q_LOANS: 3 if loans else 0,   # 3 to exclude ends of loans
            self._Q_INTERNAL: int(internal),    # 1 for True, 0 for False
        }
        query_string = urlencode(params)
        return f"{self._origin}/plus/?{query_string}"

    def _get_page_soup(self, url, max_retries=5):
        """
        Get the BeautifulSoup of a Transfermarkt transfer window summary page.

        Args:
            url (str): URL of the transfers page.
            max_retries (int): Maximum number of request retry attempts.

        Returns:
            BeautifulSoup: Soup of the transfers page.
        """
        for attempt in range(max_retries):
            try:
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                response = self._client.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                return soup
            except httpx.RequestError as e:
                print(f"An error occurred while requesting {e.request.url}")
            except httpx.HTTPStatusError as e:
                print(f"Code {e.response.status_code} on {e.request.url}")
            # Use exponential backoff with random delay to avoid soft blocking.
            sleep_time = random.uniform(2, 5) * (attempt + 1)
            time.sleep(sleep_time)
        else:
            raise RuntimeError(f"All {max_retries} attempts failed for {url}")

    def _soup_to_df(self, soup, window):
        """
        Parse the soup of a Transfermarkt transfer window summary page.

        Args:
            soup (BeautifulSoup): Soup of the transfers page.
            window (str): One of 's' (summer) or 'w' (winter).

        Returns:
            pd.DataFrame: Transfermarkt transfers data.
        """
        # Club names are h2 headers of class "content-box-headline--logo".
        clubs = [
            tag.text.strip()
            for tag in soup.find_all('h2', class_='content-box-headline--logo')
        ]
        # Transfers are in tables nested in "responsive-table"-class divs.
        tables = [
            tag.find('table')
            for tag in soup.find_all('div', class_='responsive-table')
        ]
        if not clubs or not tables:
            raise ValueError("Page structure not recognized.")

        # Player information is nested differently depending on the cell.
        def _parse_player_name_and_id(x):
            player_span = x.find('span', class_='hide-for-small')
            if player_span and player_span.a:
                player_name = player_span.a.text.strip()
                player_id = player_span.a['href'].split('/')[-1]
                return player_name, int(player_id)
            return None, None

        def _parse_text(x):
            return x.text.strip() if x else None

        def _parse_from_img(x):
            img = x.find('img') if x else None
            return img.get('title') if img else None

        parse_col_index = {
            0: _parse_player_name_and_id,
            1: _parse_text,
            2: _parse_from_img,
            3: _parse_text,
            4: _parse_text,
            5: _parse_text,
            6: _parse_from_img,
            7: _parse_from_img,
            8: _parse_text
        }

        # Parse the transfer data from the tables.
        dfs_in, dfs_out = [], []
        for i, table in enumerate(tables):
            col_headers = [th.text for th in table.find_all('th')]
            col_headers.insert(-1, 'Country')
            table_data = []
            for row in table.tbody.find_all('tr'):
                tds = row.find_all('td')
                # The row has one cell if there are no transfers.
                if len(tds) <= 1: break
                # Otherwise, there are nine cells to parse per transfer.
                transfer = [parse_col_index[j](td) for j, td in enumerate(tds)]
                table_data.append(transfer)

            df = pd.DataFrame(table_data, columns=col_headers)
            df.insert(loc=0, column='league', value=self._league)
            df.insert(loc=1, column='window', value=window)
            # Tables alternate between transfers in and out that have
            # different headers.
            if i % 2 == 0:
                dfs_in.append(df)
            else:
                dfs_out.append(df)

        # Make column names consistent.
        col_names = {
            'In': 'player',
            'Out': 'player',
            'Age': 'age',
            'Nat.': 'nationality',
            'Position': 'position',
            'Pos': 'pos',
            'Market value': 'market_value',
            'Left': 'dealing_club',
            'Joined': 'dealing_club',
            'Country': 'dealing_country',
            'Fee': 'fee'
        }
        # Merge the data.
        dfs = []
        for club, df_in, df_out in zip(clubs, dfs_in, dfs_out):
            for df, movement in ((df_in, 'in'), (df_out, 'out')):
                df.rename(columns=col_names, inplace=True)
                df.insert(loc=1, column='club', value=club)
                df.insert(loc=3, column='movement', value=movement)
                dfs.append(df)

        return pd.concat(dfs)
