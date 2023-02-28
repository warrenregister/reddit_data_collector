from requests import get
from datetime import datetime, date
from time import mktime, sleep
from itertools import product
from csv import DictWriter

class PushshiftScraper:
    base_url = "https://api.pushshift.io/reddit/search/{0}/?"
    query_string = "q={0}&subreddit={1}&before={2}&after={3}"
    fields = ['term', 'subreddit', 'before', 'after', 'hits', 'submission', 'hit_type']



    def __init__(self, keywords: list, subreddits=('uci', ),
                 custom_periods=None,
                 log_path='./logs.txt',
                 data_path='./reddit_data.csv',
                 append_data=True):
        """
        Initiate scraper with a list of keywords to query over another list
        of subreddits. By default queries are made for each month, but if
        different timestamps are necessary a custom list can be passed in.

        keywords: list of query strings
        subreddits: list, subreddits to check query strings on defaults to r/UCI
        custom_periods: list of tuples of unix timestamps (before, after) to
        aggregate over, if non is passed uses every month between Feb 1 2023
        and Jan 1 2010
        log_path: path to file to save logs to
        data_path: path to file to save results to
        append_data: bool, if true assumes that data_path file already
        exists, so it appends to the end
        """
        self.qs = keywords
        self.subreddits = subreddits
        self.log_path = log_path
        self.data_path = data_path
        self.periods = None

        if custom_periods is not None:
            self.periods = custom_periods

        
        if not append_data:
            with open(self.data_path, 'w') as csvfile:
                writer = DictWriter(csvfile, fieldnames=self.fields)
                writer.writeheader()

    def create_urls(self, queries, subreddits, periods, types=['submission', 'comment']):
        """
        Generator which given lists of query terms, subreddits, before/after unix timestamps, and query type(s) 
        yields urls for each entry in the product of these lists / all combinations of values in the lists.

        queries: list of query strings
        subreddits: list of subreddits to check query strings on
        periods: list of tuples of unix timestamps (before, after) to aggregate over
        types: list of types of content to check possible values are submission or comment defaults to both
        """

        for type in types:
            url = self.base_url.format(type)

            query_params = product(queries, subreddits, periods)

            for param_list in query_params:
                query_string = self.query_string.format(param_list[0],
                                                        param_list[1],
                                                        param_list[2][0],
                                                        param_list[2][1])
                yield  url + query_string + "&track_total_hits=true&limit=0"
                # docs show size=0 to return no actual posts, but only limit=0 works
                # without track_total_hits=true hits is capped at 10000 

    
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
        periods = []
        curr = start

        while curr > end:
            before = mktime(curr.timetuple()) # midnight on first day of curr month
            
            try:
                curr = curr.replace(month=curr.month - 1)
            except ValueError:
                curr = curr.replace(year=curr.year - 1)
                curr = curr.replace(month=12)
            
            after = mktime(curr.timetuple()) # midnight on first day of prev month
            
            periods.append((int(before), int(after)))


        return periods


    def make_requests(self, start=datetime(2023, 2, 1), end=datetime(2010, 1, 1), sleep_time=20, search_types=['submission', 'comment'],
                      alt_data_path=None, append_to_alt=False):
        """
        Executes and stores results for all requests specifiec by constructor parameters

        start: datetime.datetime for first day of the first month not being checked
        end: datetime.datetime for first day of the last month to be checked
        sleep_time: int, if a request gets an http error code, how long to wait before trying again
        search_types: list, types of search to try in ['subreddit', 'submission', 'comment']
        alt_data_path: string, alternative file path to store reqeusts to.
        append_to_alt: bool, if true assumes alt_data_path already has data in it and
        appends to the end.
        """
        periods = None
        if self.periods is None:
            periods =  self.get_month_timestamps(start, end)
        else:
            periods = self.periods
        

        for url in self.create_urls(self.qs, self.subreddits, periods, types=search_types):
            data = {}
            executed, results = self.execute_request(url)

            if not executed: # if not executed wait some amount of time and try again
                sleep(sleep_time)
                executed, results = self.execute_request(url)

                if not executed: # if it fails twice move on
                    continue

            query_list = results['metadata']['es_query']['query']['bool']['must'][0]['bool']['must']
            found = False
            for dictionary in query_list:
                if 'simple_query_string' in dictionary.keys():
                    data['term'] = query_list[0]['simple_query_string']['query']
                    found = True

            if not found:
                data['term'] = ''

            data['submission'] = 1 if url[39] == 's' else 0
            data['subreddit'] = results['metadata']['es_query']['query']['bool']['must'][1]['bool']['should'][0]['match']['subreddit']
            data['hits'] = results['metadata']['es']['hits']['total']['value']
            data['hit_type'] = results['metadata']['es']['hits']['total']['relation']

            range_list = results['metadata']['es_query']['query']['bool']['must'][0]['bool']['must']

            for elem in range_list:
                try:
                    range = elem['range']['created_utc']
                    if 'gte' in range:
                        data['after'] =  elem['range']['created_utc']['gte']
                    elif 'lt' in range:
                        data['before'] = elem['range']['created_utc']['lt']
                except KeyError:
                    continue

            if alt_data_path is None:
                with open(self.data_path, 'a') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=self.fields)
                    writer.writerow(data)
            else:
                with open(alt_data_path, 'a') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=self.fields)
                    if not append_to_alt:
                        writer.writeheader()
                        append_to_alt = True

                    writer.writerow(data)
