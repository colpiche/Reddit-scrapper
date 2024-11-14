from collections import Counter
from datetime import datetime
from LLM.Types import LLMResponseFormat
import os
import sqlite3
from .Types import DbUser, DbSubmission, DbComment


class DatabaseManager:
    """Gestionnaire de base de données"""

    # Définir le chemin vers database.db dans le dossier du module
    _filepath: str = os.path.join(os.path.dirname(__file__), "database.db")

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
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)

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

    def add_users(self, users: list[DbUser]):
        """
        Ajoute une liste d'utilisateurs dans la table User.

        :param users: list[User] - Une liste d'objets utilisateur
        """

        valid_genres = {"M", "F", "NB"}  # Ensemble des valeurs valides pour le champ Genre

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Insertion de multiples utilisateurs dans la table User
            for user in users:
                try:
                    # Validation de la valeur du champ Genre
                    genre = user.get("Genre")
                    if genre is not None and genre not in valid_genres:
                        print(f"Utilisateur '{user['Id']}' - Valeur de genre invalide : '{genre}' (doit être 'M', 'F' ou 'NB')")
                        continue  # Passe à l'utilisateur suivant sans insertion

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

    def add_submissions(self, submissions: list[DbSubmission]):
        """
        Ajoute une liste de soumissions dans la table Submission.

        :param submissions: list[Submission] - Une liste d'objets Submission
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
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

    def add_comments(self, comments: list[DbComment]):
        """
        Ajoute une liste de commentaires dans la table Comment.

        :param comments: list[Comment] - Une liste d'objets Comment
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
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
    
    def update_keywords_and_topic(self, dict: DbSubmission, LLMResponse: LLMResponseFormat):
        """
        Met à jour les mots-clés et le sujet d'une soumission.

        :param text: Submission - L'objet soumission à mettre à jour.
        :param LLMResponse: LLMResponseFormat - La réponse générée par le modèle LLM.
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Formatage de la liste de mots-clés en une chaîne de caractères séparée par des virgules
            formatted_keywords: str = ','.join(LLMResponse["keywords"])

            # Mise à jour des mots-clés et du sujet dans la table correspondante
            curseur.execute("""
                UPDATE Submission
                SET Keywords = ?, Topic = ?
                WHERE Id = ?
            """, (
                formatted_keywords,
                LLMResponse["topic"],
                dict["Id"]
            ))

            # Enregistrement des changements
            connexion.commit()
            print(f"Mots-clés et sujet mis à jour pour '{dict['Id']}' avec succès.")

        except sqlite3.Error as e:
            print(f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}")

        finally:
            # Fermeture de la connexion
            connexion.close()

    def get_all_users(self) -> list[DbUser]:
        """Récupère tous les utilisateurs de la table User."""
        users: list[DbUser] = []

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la requête pour récupérer tous les utilisateurs
            curseur.execute("SELECT Id, Name, Genre, Age FROM User")
            rows = curseur.fetchall()

            # Conversion des résultats en liste d'objets User
            for row in rows:
                user = DbUser(
                    Id=row[0],
                    Name=row[1],
                    Genre=row[2],
                    Age=row[3]
                )
                users.append(user)

        except sqlite3.Error as e:
            print(f"Erreur lors de la récupération des utilisateurs : {e}")

        finally:
            connexion.close()

        return users

    def get_all_submissions(self) -> list[DbSubmission]:
        """Récupère toutes les soumissions de la table Submission."""
        submissions: list[DbSubmission] = []

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la requête pour récupérer toutes les soumissions
            curseur.execute("SELECT Id, Author_id, Created, Sub_id, Url, Title, Body, Keywords, Topic FROM Submission")
            rows = curseur.fetchall()

            # Conversion des résultats en liste d'objets Submission
            for row in rows:
                submission = DbSubmission(
                    Id=row[0],
                    Author_id=row[1],
                    Created=datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S.%f'),
                    Sub_id=row[3],
                    Url=row[4],
                    Title=row[5],
                    Body=row[6],
                    Keywords=row[7].split(',') if row[7] else None,
                    Topic=row[8]
                )
                submissions.append(submission)

        except sqlite3.Error as e:
            print(f"Erreur lors de la récupération des soumissions : {e}")

        finally:
            connexion.close()

        return submissions

    def get_all_comments(self) -> list[DbComment]:
        """Récupère tous les commentaires de la table Comment."""
        comments: list[DbComment] = []

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la requête pour récupérer tous les commentaires
            curseur.execute("SELECT Id, Author_id, Created, Parent_id, Submission_id, Body FROM Comment")
            rows = curseur.fetchall()

            # Conversion des résultats en liste d'objets Comment
            for row in rows:
                comment = DbComment(
                    Id=row[0],
                    Author_id=row[1],
                    Created=datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S.%f'),
                    Parent_id=row[3],
                    Submission_id=row[4],
                    Body=row[5]
                )
                comments.append(comment)

        except sqlite3.Error as e:
            print(f"Erreur lors de la récupération des commentaires : {e}")

        finally:
            connexion.close()

        return comments

    def execute_command(self, command: str, params: tuple = ()) -> list[tuple] | None:
        """
        Exécute une commande SQLite arbitraire et retourne le résultat.

        :param command: str - La commande SQL à exécuter.
        :param params: tuple - Les paramètres à insérer dans la commande SQL (optionnel).
        :return: list[tuple] | None - Le résultat de la commande si c'est une requête SELECT,
                sinon None.
        """
        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la commande avec les paramètres
            curseur.execute(command, params)

            # Si la commande est une requête de type SELECT, retourner les résultats
            if command.strip().upper().startswith("SELECT"):
                result = curseur.fetchall()
                return result

            # Si la commande est autre qu'un SELECT, on commit les changements
            connexion.commit()
            print("Commande exécutée avec succès.")

        except sqlite3.Error as e:
            print(f"Erreur lors de l'exécution de la commande : {e}")

        finally:
            # Fermeture de la connexion
            connexion.close()

        # Retourner None si la commande n'est pas un SELECT
        return None
    
    def calculate_keyword_occurrences(self):
        """
        Calcule les occurrences de chaque mot-clé dans la table Submission,
        et enregistre les résultats dans une nouvelle table KeywordWeight.
        """
        
        # Connexion à la base de données
        connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
        curseur: sqlite3.Cursor = connexion.cursor()

        try:
            # Création de la table KeywordWeight si elle n'existe pas
            curseur.execute("""
                CREATE TABLE IF NOT EXISTS KeywordWeight (
                    Keyword TEXT PRIMARY KEY,
                    Weight INTEGER NOT NULL
                );
            """)

            # Vider la table KeywordWeight si elle existe déjà
            curseur.execute("DELETE FROM KeywordWeight")

            # Récupération de tous les mots-clés dans la table Submission
            curseur.execute("SELECT Keywords FROM Submission")
            rows = curseur.fetchall()

            # Compter les occurrences des mots-clés
            keyword_counter = Counter()
            for row in rows:
                if row[0]:  # S'assurer que Keywords n'est pas NULL
                    keywords = row[0].split(',')  # Supposer que les mots-clés sont séparés par des virgules
                    keyword_counter.update(keywords)

            # Insérer les mots-clés et leurs occurrences dans la table KeywordWeight
            for keyword, weight in keyword_counter.items():
                curseur.execute("""
                    INSERT INTO KeywordWeight (Keyword, Weight)
                    VALUES (?, ?)
                """, (keyword, weight))

            # Sauvegarder les modifications
            connexion.commit()
            print("Table KeywordWeight mise à jour avec les occurrences des mots-clés.")

        except sqlite3.Error as e:
            print(f"Erreur lors du calcul des occurrences des mots-clés : {e}")

        finally:
            connexion.close()
