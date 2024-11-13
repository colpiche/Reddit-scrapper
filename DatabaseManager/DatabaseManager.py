from datetime import datetime
import sqlite3
from typing import TypedDict, NotRequired


class User(TypedDict):
    """
    This module defines the User TypedDict for representing user information in the database.

    Attributes:
        Id (str): The unique identifier for the user.
        Name (str): The name of the user.
        Genre (Optional[str]): The genre of the user. This field is optional and can be None.
        Age (Optional[int]): The age of the user. This field is optional and can be None.
    """
    Id: str
    Name: str
    Genre: NotRequired[str | None]
    Age: NotRequired[int | None]


class Submission(TypedDict):
    """
    This module defines the Submission TypedDict for representing submission information in the database.

    Attributes:
        Id (str): The unique identifier for the submission.
        Author_id (str): The unique identifier of the author (must correspond to an existing user Id).
        Created (datetime): The creation date of the submission.
        Sub_id (str): The identifier of the subreddit where the submission was posted.
        Url (str): The URL of the submission.
        Title (str): The title of the submission.
        Body (str): The body of the submission.
        Keywords (Optional[list[str]]): A list of keywords describing the submission. This field is optional and can be None.
        Topic (Optional[str]): The topic of the submission. This field is optional and can be None.
    """
    Id: str
    Author_id: str
    Created: datetime
    Sub_id: str
    Url: str
    Title: str
    Body: str
    Keywords: NotRequired[list[str] | None]
    Topic: NotRequired[str | None]


class Comment(TypedDict):
    """
    This module defines the Comment TypedDict for representing comment information in the database.

    Attributes:
        Id (str): The unique identifier for the comment.
        Author_id (str): The unique identifier of the author (must correspond to an existing user Id).
        Created (datetime): The creation date of the comment.
        Parent_id (str): The identifier of the parent comment (or submission if it's a first level comment).
        Submission_id (str): The identifier of the submission to which the comment belongs.
        Body (str): The body of the comment.
    """
    Id: str
    Author_id: str
    Created: datetime
    Parent_id: str
    Submission_id: str
    Body: str


class DatabaseManager:
    """Gestionnaire de base de données"""

    _file: str = "database.db"

    # Schéma de la table User
    _table_user: str = """
    CREATE TABLE IF NOT EXISTS User (
        Id TEXT PRIMARY KEY,
        Name TEXT NOT NULL,
        Genre TEXT,
        Age INTEGER
    );
    """

    # Schéma de la table Submission
    _table_submission: str = """
    CREATE TABLE IF NOT EXISTS Submission (
        Id TEXT PRIMARY KEY,
        Author_id TEXT NOT NULL,
        Created TEXT NOT NULL,
        Sub_id TEXT NOT NULL,
        Url TEXT NOT NULL,
        Title TEXT NOT NULL,
        Body TEXT NOT NULL,
        Keywords TEXT,
        Topic TEXT,
        FOREIGN KEY (Author_id) REFERENCES User(Id) ON DELETE CASCADE
    );
    """

    # Schéma de la table Comment
    _table_comment: str = """
    CREATE TABLE IF NOT EXISTS Comment (
        Id TEXT PRIMARY KEY,
        Author_id TEXT NOT NULL,
        Created TEXT NOT NULL,
        Parent_id TEXT,
        Submission_id TEXT NOT NULL,
        Body TEXT NOT NULL,
        FOREIGN KEY (Author_id) REFERENCES User(Id) ON DELETE SET NULL,
        FOREIGN KEY (Submission_id) REFERENCES Submission(Id) ON DELETE CASCADE
    );
    """

    def __init__(self) -> None:
        pass

    def _format_date(self, date: datetime) -> str:
        """Formatage d'une date au format SQLite"""

        return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # On enlève les derniers microsecondes non nécessaires

    def create(self):
        """Création de la base de données avec gestion des erreurs"""

        print("Création de la base de données...")

        try:
            # Connexion à la base de données (création du fichier si nécessaire)
            connexion: sqlite3.Connection = sqlite3.connect(self._file)

            # Création d'un curseur pour exécuter les commandes SQL
            curseur: sqlite3.Cursor = connexion.cursor()

            # Création des tables
            print("Création des tables...")
            curseur.execute(self._table_user)
            curseur.execute(self._table_submission)
            curseur.execute(self._table_comment)

            # Enregistrement des changements
            connexion.commit()
            print("Tables créées avec succès.")

        except sqlite3.DatabaseError as e:
            print(f"Erreur lors de la création de la base de données : {e}")

        except sqlite3.Error as e:
            print(f"Erreur générique SQLite : {e}")

        finally:
            # Fermeture de la connexion, même en cas d'erreur
            if 'connexion' in locals():
                connexion.close()
                print("Connexion fermée.")

    def add_users(self, users: list[User]):
        """
        Ajoute une liste d'utilisateurs dans la table User.

        :param users: list[User] - Une liste d'objets utilisateur
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._file)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Insertion de multiples utilisateurs dans la table User
            for user in users:
                try:
                    curseur.execute("""
                        INSERT INTO User (Id, Name, Genre, Age)
                        VALUES (?, ?, ?, ?)
                    """, (
                        user["Id"],
                        user["Name"],
                        user.get("Genre"),  # Si "Genre" n'est pas spécifié, cela renverra None
                        user.get("Age")     # Si "Age" n'est pas spécifié, cela renverra None
                    ))
                    # Enregistrement des changements pour chaque utilisateur
                    connexion.commit()
                    print(f"Utilisateur '{user['Id']}' ajouté avec succès.")

                except sqlite3.IntegrityError as e:
                    print(f"Utilisateur '{user['Id']}' - Erreur d'intégrité lors de l'ajout de l'utilisateur : {e}")

                except sqlite3.Error as e:
                    print(f"Utilisateur '{user['Id']}' - Une erreur est survenue lors de l'ajout de l'utilisateur : {e}")

        except sqlite3.Error as e:
            print(f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}")

        finally:
            # Fermeture de la connexion
            connexion.close()

    def add_submissions(self, submissions: list[Submission]):
        """
        Ajoute une liste de soumissions dans la table Submission.

        :param submissions: list[Submission] - Une liste d'objets Submission
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._file)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Insertion de multiples soumissions dans la table Submission
            for submission in submissions:
                try:
                    # Formatage de la liste de mots-clés en une chaîne de caractères séparée par des virgules
                    formatted_keywords: str | None = None
                    keywords = submission.get("Keywords")
                    if keywords:
                        formatted_keywords = ','.join(keywords)

                    # Formatage de la date 'Created' en texte au format SQLite
                    formatted_created = self._format_date(submission["Created"])

                    curseur.execute("""
                        INSERT INTO Submission (Id, Author_id, Created, Sub_id, Url, Title, Body, Keywords, Topic)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        submission["Id"],
                        submission["Author_id"],
                        formatted_created,
                        submission["Sub_id"],
                        submission["Url"],
                        submission["Title"],
                        submission["Body"],
                        formatted_keywords,
                        submission.get("Topic")  # Si 'Topic' n'est pas spécifié, cela renverra None
                    ))

                    # Enregistrement des changements pour chaque soumission
                    connexion.commit()
                    print(f"Soumission '{submission['Id']}' ajoutée avec succès.")

                except sqlite3.IntegrityError as e:
                    print(f"Soumission '{submission['Id']}' - Erreur d'intégrité lors de l'ajout de la soumission : {e}")

                except sqlite3.Error as e:
                    print(f"Soumission '{submission['Id']}' - Une erreur est survenue lors de l'ajout de la soumission : {e}")

        except sqlite3.Error as e:
            print(f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}")

        finally:
            # Fermeture de la connexion
            connexion.close()

    def add_comments(self, comments: list[Comment]):
        """
        Ajoute une liste de commentaires dans la table Comment.

        :param comments: list[Comment] - Une liste d'objets Comment
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._file)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Insertion de multiples commentaires dans la table Comment
            for comment in comments:
                try:
                    formatted_created = self._format_date(comment["Created"])

                    curseur.execute("""
                        INSERT INTO Comment (Id, Author_id, Created, Parent_id, Submission_id, Body)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        comment["Id"],
                        comment["Author_id"],
                        formatted_created,
                        comment["Parent_id"],
                        comment["Submission_id"],
                        comment["Body"]
                    ))

                    # Enregistrement des changements pour chaque commentaire
                    connexion.commit()
                    print(f"Commentaire '{comment['Id']}' ajouté avec succès.")

                except sqlite3.IntegrityError as e:
                    print(f"Commentaire '{comment['Id']}' - Erreur d'intégrité lors de l'ajout du commentaire : {e}")

                except sqlite3.Error as e:
                    print(f"Commentaire '{comment['Id']}' - Une erreur est survenue lors de l'ajout du commentaire : {e}")

        except sqlite3.Error as e:
            print(f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}")

        finally:
            # Fermeture de la connexion
            connexion.close()
