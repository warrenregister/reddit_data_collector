from requests import get
from datetime import datetime, date
from time import mktime, sleep
from itertools import product
from csv import DictWriter

class PushshiftScraper:
    base_url = "https://api.pushshift.io/reddit/search/{0}/?"
    query_string = "q={0}&subreddit={1}&before={2}&after={3}"
    fields = ['term', 'subreddit', 'before', 'after', 'hits', 'submission', 'hit_type']



    def __init__(self, keywords: list, subreddits=('uci', ), custom_befores=None, custom_afters=None, log_path='./logs.txt', data_path='./reddit_data.csv'):
        """
        Initiate scraper with a list of keywords to query over another list of subreddits.
        By default queries are made for each month, but if different timestamps are necessary
        a custom list can be passed in.

        keywords: list of query strings
        subreddits: list of subreddits to check query strings on defauls to r/UCI
        custom_befores: list of unix before timestamps to query for
        custom_afters: list of unix after timestamps to query for
        log_path: path to file to save logs to
        data_path: path to file to save results to 
        """
        self.qs = keywords
        self.subreddits = subreddits
        self.log_path = log_path
        self.data_path = data_path
        self.befores = None
        self.afters = None

        if custom_afters is not None and custom_befores is not None:
            if len(custom_afters) == (custom_befores):
                self.befores = custom_befores
                self.afters = custom_afters
            else:
                raise ValueError('Custom lists for before and after must be the same length!')
        
        with open(self.data_path, 'w') as csvfile:
            writer = DictWriter(csvfile, fieldnames=self.fields)
            writer.writeheader()

    def create_urls(self, queries, subreddits, befores, afters, types=['submission', 'comment']):
        """
        Generator which given lists of query terms, subreddits, before/after unix timestamps, and query type(s) 
        yields urls for each entry in the product of these lists / all combinations of values in the lists.

        queries: list of query strings
        subreddits: list of subreddits to check query strings on
        befores: unix timestamps for before, e.g queries lt these values
        afters: unix timestamps for after, e.g queries gte these values
        types: list of types of content to check possible values are submission or comment defaults to both
        """

        for type in types:
            url = self.base_url.format(type)

            query_params = product(queries, subreddits, befores, afters)

            for param_list in query_params:
                query_string = self.query_string.format(*param_list)
                yield  url + query_string + "&size=0"

    
    def execute_request(self, url: str):
        """
        Executes http request given a url string

        url: string of url for a api request
        """
        response = get(url)

        if response.status_code >= 300: # catch and log bad requests
            with open(self.log_path, 'a') as file:
                file.write(str(response.status_code))
                file.write('\n')
                file.write(url)
                file.write('\n')
                file.write(str(response.text))
                file.write('\n')
            return (False, None)
        
        return True, response.json()
    
    def get_month_timestamps(self, start, end):
        """
        Generates all unix timestamps for each month between start and end.

        start: datetime.datetime for first day of the first month not being checked
        end: datetime.datetime for first day of the last month to be checked
        """
        befores = []
        afters = []
        curr = start

        while curr > end:
            before = mktime(curr.timetuple()) # midnight on first day of curr month
            
            try:
                curr = curr.replace(month=curr.month - 1)
            except ValueError:
                curr = curr.replace(year=curr.year - 1)
                curr = curr.replace(month=12)
            
            after = mktime(curr.timetuple()) # midnight on first day of prev month
            
            befores.append(int(before))
            afters.append(int(after))

        return befores, afters


    def make_requests(self, start=datetime(2023, 2, 1), end=datetime(2010, 1, 1), sleep_time=20):
        """
        Executes and stores results for all requests specifiec by constructor parameters

        start: datetime.datetime for first day of the first month not being checked
        end: datetime.datetime for first day of the last month to be checked
        sleep_time: if a request gets an http error code, how long to wait before trying again
        """

        if self.befores is None or self.afters is None:
            self.befores, self.afters =  self.get_month_timestamps(start, end)
        

        for url in self.create_urls(self.qs, self.subreddits, self.befores, self.afters):
            data = {}
            executed, results = self.execute_request(url)

            if not executed: # if not executed wait some amount of time and try again
                sleep(sleep_time)
                executed, results = self.execute_request(url)

                if not executed: # if it fails twice move on
                    continue

            
            data['term'] = results['metadata']['es_query']['query']['bool']['must'][0]['bool']['must'][0]['simple_query_string']['query']
            data['submission'] = 1 if url[39] == 's' else 0
            data['subreddit'] = results['metadata']['es_query']['query']['bool']['must'][1]['bool']['should'][0]['match']['subreddit']
            data['hits'] = results['metadata']['es']['hits']['total']['value']
            data['hit_type'] = results['metadata']['es']['hits']['total']['relation']
            data['after'] =  results['metadata']['es_query']['query']['bool']['must'][0]['bool']['must'][1]['range']['created_utc']['gte']
            data['before'] = results['metadata']['es_query']['query']['bool']['must'][0]['bool']['must'][2]['range']['created_utc']['lt']

            with open(self.data_path, 'a') as csvfile:
                 writer = DictWriter(csvfile, fieldnames=self.fields)
                 writer.writerow(data)
