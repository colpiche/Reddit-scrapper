import praw
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
USERNAME = os.getenv('USERNAME')
USER_PASSWORD = os.getenv('USER_PASSWORD')
USER_AGENT = os.getenv('USER_AGENT')

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    password=USER_PASSWORD,
    user_agent=USER_AGENT,
    username=USERNAME,
)

submissions = reddit.subreddit("AskFrance").stream.submissions()

for submission in submissions:
    print(submission.title)
