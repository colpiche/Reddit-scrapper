import praw
import os
from dotenv import load_dotenv
from praw.models import Submission
from Database.Manager import DatabaseManager
from Database.Types import DbUser, DbSubmission, DbComment
from datetime import datetime
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

submissions: list[Submission] = reddit.subreddit("AskFrance").stream.submissions()

for submission in submissions:
    print(submission.title)


database = DatabaseManager()
database.create()

users_to_add = [
    DbUser(Id="user001", Name="Alice", Genre="Femme", Age=30),
    DbUser(Id="user0012", Name="Graziella", Genre="F", Age=52),
    DbUser(Id="user007", Name="Michel", Genre="NB", Age=34),
    DbUser(Id="user002", Name="Bob", Genre="Homme", Age=25),
    DbUser(Id="user003", Name="Charlie"),
]

submissions_to_add = [
    DbSubmission(
        Id="sub123",
        Author_id="user456",
        Created=datetime.now(),  # Exemple de date et heure
        Sub_id="tech_news",
        Url="https://www.example.com/submission/123",
        Title="Nouvelle soumission de test",
        Body="Ceci est le corps de la soumission de test.",
        Keywords=["test", "python", "sqlite"],  # Liste de mots-clés
        Topic="Technologie"
    ),
    DbSubmission(
        Id="sub456",
        Author_id="user789",
        Created=datetime.now(),  # Exemple de date et heure
        Sub_id="science_news",
        Url="https://www.example.com/submission/456",
        Title="Nouvelle soumission de science",
        Body="Ceci est le corps de la soumission de science."
    )
]

comments_to_add = [
    DbComment(
        Id="comment123",
        Author_id="user456",
        Created=datetime.now(),
        Parent_id="sub123",  # Peut être l'ID de la soumission ou d'un commentaire parent
        Submission_id="sub123",
        Body="Ceci est un commentaire de test."
    ),
    DbComment(
        Id="comment456",
        Author_id="user789",
        Created=datetime.now(),
        Parent_id="comment123",  # Peut être l'ID de la soumission ou d'un commentaire parent
        Submission_id="sub123",
        Body="Ceci est un commentaire de test."
    )
]

database.add_users(users_to_add)
database.add_submissions(submissions_to_add)
database.add_comments(comments_to_add)

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