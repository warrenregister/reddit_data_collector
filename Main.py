import pushshift_scraper as ps
from datetime import datetime

if __name__ == '__main__':
    kwords = ["sports"]

    # submissions appear to only have data in pushshift going back 2 months
    scraper = ps.PushshiftScraper(kwords, data_path='./reddit_comments.csv', append_data=True)
    scraper.make_requests(search_types=['comment'])
    scraper.make_requests(start=datetime(2023, 2, 1), end=datetime(2022, 11, 1), search_types=['submission'], alt_data_path='./reddit_posts.csv')

