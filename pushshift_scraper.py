from requests import get
from datetime import datetime, date
from time import mktime

class PushshiftScraper:
    base_url = "https://api.pushshift.io/reddit/search/{0}/?"
    query_string = "q={0}&subreddit={1}&before={2}&after={3}"
    data = {'term': [], 'subreddit': [], 'before': [], 'after': [], 'hits': [], 'post': []}

    logs = []


    def __init__(self, keywords: list,  by_month: bool=True, subreddits: list=('uci')):
        self.qs = keywords
        self.frequencies = time_grans
        self.subreddit = subreddits
    

    def create_url(self, query: str, subreddit: str=None, before: str=None, after: str=None,
                   type='submission'):
        url = self.base_url.format(type)
        query_string = self.query_string.format(
            query,
            subreddit if subreddit is not None else '',
            before if before is not None else '',
            after if after is not None else '',
        )

        return url + query_string
    
    def execute_request(self, url: str) -> tuple(bool, dict):
        response = get(url)

        if response.status_code >= 300: # catch and log bad requests
            self.logs.append((response.status_code, url, response.text()))
            return (False, None)
        
        return True, response.json()
    
    def get_timestamps(self, by, start=datetime(2023, 2, 1), end=datetime(2010, 1, 1)):
        time_steps = []
        start = start.timetuple()
        end = end.timetuple()

        



        return timesteps


    def make_requests(self):
        raise NotImplementedError

