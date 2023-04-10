from pmaw import PushshiftAPI
import pandas as pd
import datetime
import time

class SubredditScraper():
    """
    Class to scrape data from a subreddit

    subreddit: string of subreddit name
    start_date: datetime.datetime for first day of the first month not being checked
    end_date: datetime.datetime for first day of the last month to be checked
    log_path: path to log file
    """

    def __init__(self, subreddit, start_date, end_date, log_path):
        self.subreddit = subreddit
        self.start_date = start_date
        self.end_date = end_date
        self.log_path = log_path
        self.api = PushshiftAPI()

    def log_overflow(self, comment_list, start, end, type=None, n=None):
        """
        Logs the start and end timestamps of a period where the number of comments
        returned by the API is 1000. This is used to check for periods where the
        API is not returning all comments.

        comment_list: list of comments returned by the API
        start: start timestamp of the period
        end: end timestamp of the period
        type: string of either 'comments' or 'posts'used by scraper
        n: number of days scraper incremented by
        """
        if len(comment_list) == 1000:
            with open(self.log_path, 'a+') as log:
                log.write(str(start))
                log.write(' ')
                log.write(str(end))
            if type is not None and n is not None:
                start = datetime.datetime.fromtimestamp(start)
                end = datetime.datetime.fromtimestamp(end)
                n = n // 2 or 1
                return self.scrape(type, n, start, end)

    
    def scrape(self, scrape_type: str, n=3, start=None, end=None):
        """
        Scrapes data from a subreddit.

        scrape_type: string of either 'comments' or 'posts'
        n: number of days to increment by
        start: datetime.datetime for first day of the first month not being checked,
        efaults to self.start_date
        end: datetime.datetime for first day of the last month to be checked,
        defaults to self.end_date
        """
        if start is None:
             start = self.start_date
        if end is None:
            end = self.end_date

        if scrape_type == 'comments':
            scraper_func = self.api.search_comments
        elif scrape_type == 'posts':
            scraper_func = self.api.search_submissions

        dates = self.get_timestamps(start, end, n)
        start = dates[0][0]
        end = dates[0][1]
        
        results = scraper_func(
                                subreddit=self.subreddit,
                                before = start,
                                after = end,
                                limit=1000
                            )
        result_list = [result for result in results]
        extra_results = self.log_overflow(result_list, start, end, scrape_type, n)
        df = pd.DataFrame(result_list)
        if extra_results is not None:
            df = pd.concat([df, extra_results])

        for date in dates[1:]:
            start = date[0]
            end = date[1]

            comments = scraper_func(
                                    subreddit=self.subreddit,
                                    before = start,
                                    after = end,
                                    limit=1000
                                )
            result_list = [result for result in results]
            extra_results = self.log_overflow(result_list, start, end, scrape_type, n)
            df = pd.concat([df, pd.DataFrame(result_list)])
            if extra_results is not None:
                df = pd.concat([df, extra_results])
        
        return df.drop_duplicates()
        
    @staticmethod
    def get_date_timestamps(start, end, n):
        """
        Generates all unix timestamps for each n day increment between start and end.

        start: datetime.datetime for first day of the first month not being checked
        end: datetime.datetime for first day of the last month to be checked
        """
        periods = []
        curr = start
        delta = datetime.timedelta(days=n)
        
        while curr > end:
            before = time.mktime(curr.timetuple()) 

            curr -= delta
            
            after = time.mktime(curr.timetuple()) 
            if after < time.mktime(end.timetuple()):
                after = time.mktime(end.timetuple())

            periods.append((int(before), int(after)))
            

        return periods
