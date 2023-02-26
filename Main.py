import pushshift_scraper as ps

if __name__ == '__main__':
    log_path = "./logs.csv"
    data_path = "./reddit_keyword_data.csv"
    kwords = ['sports', "'student life", ""]
    
    scraper = ps.PushshiftScraper(kwords)

    scraper.make_requests()
    scraper.save_logs(log_path)
    scraper.save_data(data_path)

