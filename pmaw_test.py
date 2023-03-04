from pmaw import PushshiftAPI


api = PushshiftAPI()


comments = api.search_comments(after=1672560000000, before=1675238400000, subreddit='uci', limit=10)


print([x for x in comments])