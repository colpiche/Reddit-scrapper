import praw
import os
from dotenv import load_dotenv
from praw.models import Submission, Comment, Redditor
from Database.Manager import DatabaseManager
from Database.Types import DbUser, DbSubmission, DbComment
from datetime import datetime
from praw import Reddit
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

database = DatabaseManager(name='askfrance_new')
database.create()

users_to_add: list[DbUser] = []
submissions_to_add: list[DbSubmission] = []
comments_to_add: list[DbComment] = []

submissions: list[Submission] = reddit.subreddit("AskFrance").new(limit=100)

for i, submission in enumerate(submissions):
    print(f"Submission-{i:03}: {submission.title}")
    submissions_to_add.append(
        DbSubmission(
        Id=submission.id,
        Author_id=submission.author.id,
        Created=datetime.fromtimestamp(submission.created_utc),  # Exemple de date et heure
        Sub_id=submission.subreddit.id,
        Url=submission.url,
        Title=submission.title,
        Body=submission.selftext,
        Keywords=[],  # Liste de mots-clés
        Topic=""
    ))

    users_to_add.append(DbUser(Id=submission.author.id, Name=submission.author.name))

    submission.comments.replace_more(limit=None)

    comments = submission.comments.list()

    for comment in comments:
        if isinstance(comment, Comment):
            if comment.author == None:
                comments_to_add.append(
                    DbComment(
                    Id=comment.id,
                    Author_id='[Removed]',
                    Created=datetime.fromtimestamp(comment.created_utc),
                    Parent_id=comment.parent_id,
                    Submission_id=comment.link_id,
                    Body='[Removed]'
                ))
            else :
                comments_to_add.append(
                    DbComment(
                    Id=comment.id,
                    Author_id=comment.author.id,
                    Created=datetime.fromtimestamp(comment.created_utc),
                    Parent_id=comment.parent_id,
                    Submission_id=comment.link_id,
                    Body=comment.body
                ))
            if comment.author != None:
                users_to_add.append(DbUser(Id=comment.author.id, Name=comment.author.name))


filtered_submissions: list[DbSubmission] = []
filtered_comments: list[DbComment] = []
filtered_users: list[DbUser] = []

submissions_ids = set()
comments_ids = set()
users_ids = set()

for submission in submissions_to_add:
    if submission['Id'] not in submissions_ids:
        submissions_ids.add(submission['Id'])
        filtered_submissions.append(submission)

for comment in comments_to_add:
    if comment['Id'] not in comments_ids:
        comments_ids.add(comment['Id'])
        filtered_comments.append(comment)

for user in users_to_add:
    if user['Id'] not in users_ids:
        users_ids.add(user['Id'])
        filtered_users.append(user)

database.add_submissions(filtered_submissions)
database.add_comments(filtered_comments)
database.add_users(filtered_users)

# # Récupérer tous les utilisateurs
# users = database.get_all_users()
# print(users)

# # Récupérer toutes les soumissions
# submissions = database.get_all_submissions()
# print(submissions)

# # Récupérer tous les commentaires
# comments = database.get_all_comments()
# print(comments)

# result = database.execute_command("SELECT * FROM User WHERE Age > ?", (25,))
# print(result)

# database.execute_command("INSERT INTO User (Id, Genre, Age) VALUES (?, ?, ?, ?)", 
#                            ("U123", "Alice", "F", 30))
