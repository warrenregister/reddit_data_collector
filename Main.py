import pushshift_scraper as ps
from datetime import datetime

if __name__ == '__main__':
    kwords = ["", ""]

    # submissions appear to only have data in pushshift going back 2 months
    # so best to only look at comments for now
    scraper = ps.PushshiftScraper(kwords,
                                  data_path='./example_data/reddit_comments.csv',
                                  log_path='./example_data/logs.txt')
    scraper.make_requests(search_types=['comment'])
    
    # to look at submissions with a smaller time window try this
    # can store to separate file with alt_data_path
    # scraper.make_requests(start=datetime(2023, 2, 1), end=datetime(2022, 11, 1),
    #                       search_types=['submission'],
    #                       alt_data_path='./reddit_submissions.csv')

