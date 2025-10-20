BASE_URL = "https://www.transfermarkt.com"
USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15) Firefox/113.0",
        "Mozilla/5.0 (X11; Linux x86_64) Chrome/113.0.0.0"
]
LEAGUE_CODES = {
    'premier-league': 'GB1'
}
LEAGUE_NAMES = {k: k.replace('-', ' ').title() for k in LEAGUE_CODES.keys()}
