import praw
import os
from dotenv import load_dotenv
from praw.models import Submission, Comment, MoreComments
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

submissions: list[Submission] = reddit.subreddit("AskFrance").new(limit=30)
isOne: bool = False


for submission in submissions:

    submission.comments.replace_more(limit=None)
    # if isOne:
    #     break
    
    print(submission.title)

    comments = submission.comments.list()
    # Create a dictionary to store the number of parent comments for each comment
    comment_parent_count = {}

    # Create a mapping of comment IDs to comments for easy access
    comment_map = {comment.id: comment for comment in comments}

    for comment in comments:
        parent_id = comment.parent_id
        
        # Initialize the count of parent comments
        parent_count = 0

        # Traverse up the hierarchy to count parent comments
        while parent_id.startswith('t1_'):  # Check if it's a comment
            parent_comment = comment_map.get(parent_id[3:])  # Get the comment ID (remove 't1_')
            if parent_comment:
                parent_count += 1
                parent_id = parent_comment.parent_id  # Move to the parent comment
            else:
                break  # No more parents found
        
        comment_content: str = comment.body[:150].replace("\n", "    ")

        # Store the count of parent comments
        print(f"{'    ' * parent_count}- {comment_content}")

    print("------------------------------------------------------------------------------------------")
    pass