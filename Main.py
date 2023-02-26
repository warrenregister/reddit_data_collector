import pushshift_scraper as ps

if __name__ == '__main__':
    #kwords = ["", 'sports', "'student life'", "club"]
    kwords = ["sports"]

    scraper = ps.PushshiftScraper(kwords)

    scraper.make_requests()

