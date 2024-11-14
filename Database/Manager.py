from collections import Counter, defaultdict
from datetime import datetime
from LLM.Agent import LLMAgent
from LLM.Types import LLMCategoryRequestFormat, LLMKeywordsTopicResponseFormat
import os
import sqlite3
from .Types import (
    DbComment,
    DbSubmission,
    DbUser,
    DbWeightedCategory,
    DbWeightedKeyword,
)


class DatabaseManager:
    """Gestionnaire de base de données"""

    _filepath: str = ""

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

    def __init__(self, name: str) -> None:
        # Définir le chemin vers database.db dans le dossier du module
        self._filepath = os.path.join(os.path.dirname(__file__), f"{name}.db")
        pass

    def _format_date(self, date: datetime) -> str:
        """Formatage d'une date au format SQLite"""

        return date.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]  # On enlève les derniers microsecondes non nécessaires

    def _has_values_for_keywords_and_topic(self, submission: DbSubmission) -> bool:
        """ "Vérifie si les valeurs de Keywords et Topic sont présentes dans une soumission."""

        # Vérifie que Keywords n'est pas None ou une liste vide
        keywords_present = submission.get("Keywords") is not None and bool(
            submission.get("Keywords")
        )

        # Vérifie que Topic n'est pas None ou une chaîne vide
        topic_present = (
            submission.get("Topic") is not None and submission.get("Topic") != ""
        )

        return keywords_present and topic_present

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
            if "connexion" in locals():
                connexion.close()
                print("Connexion fermée.")

    def add_users(self, users: list[DbUser]):
        """
        Ajoute une liste d'utilisateurs dans la table User.

        :param users: list[User] - Une liste d'objets utilisateur
        """

        valid_genres = {
            "M",
            "F",
            "NB",
        }  # Ensemble des valeurs valides pour le champ Genre

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
                        print(
                            f"Utilisateur '{user['Id']}' - Valeur de genre invalide : '{genre}' (doit être 'M', 'F' ou 'NB')"
                        )
                        continue  # Passe à l'utilisateur suivant sans insertion

                    curseur.execute(
                        """
                        INSERT INTO User (Id, Name, Genre, Age)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            user["Id"],
                            user["Name"],
                            user.get(
                                "Genre"
                            ),  # Si "Genre" n'est pas spécifié, cela renverra None
                            user.get(
                                "Age"
                            ),  # Si "Age" n'est pas spécifié, cela renverra None
                        ),
                    )
                    # Enregistrement des changements pour chaque utilisateur
                    connexion.commit()
                    print(f"Utilisateur '{user['Id']}' ajouté avec succès.")

                except sqlite3.IntegrityError as e:
                    print(
                        f"Utilisateur '{user['Id']}' - Erreur d'intégrité lors de l'ajout de l'utilisateur : {e}"
                    )

                except sqlite3.Error as e:
                    print(
                        f"Utilisateur '{user['Id']}' - Une erreur est survenue lors de l'ajout de l'utilisateur : {e}"
                    )

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

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
                        formatted_keywords = ",".join(keywords)

                    # Formatage de la date 'Created' en texte au format SQLite
                    formatted_created = self._format_date(submission["Created"])

                    curseur.execute(
                        """
                        INSERT INTO Submission (Id, Author_id, Created, Sub_id, Url, Title, Body, Keywords, Topic)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            submission["Id"],
                            submission["Author_id"],
                            formatted_created,
                            submission["Sub_id"],
                            submission["Url"],
                            submission["Title"],
                            submission["Body"],
                            formatted_keywords,
                            submission.get(
                                "Topic"
                            ),  # Si 'Topic' n'est pas spécifié, cela renverra None
                        ),
                    )

                    # Enregistrement des changements pour chaque soumission
                    connexion.commit()
                    print(f"Soumission '{submission['Id']}' ajoutée avec succès.")

                except sqlite3.IntegrityError as e:
                    print(
                        f"Soumission '{submission['Id']}' - Erreur d'intégrité lors de l'ajout de la soumission : {e}"
                    )

                except sqlite3.Error as e:
                    print(
                        f"Soumission '{submission['Id']}' - Une erreur est survenue lors de l'ajout de la soumission : {e}"
                    )

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

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

                    curseur.execute(
                        """
                        INSERT INTO Comment (Id, Author_id, Created, Parent_id, Submission_id, Body)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            comment["Id"],
                            comment["Author_id"],
                            formatted_created,
                            comment["Parent_id"],
                            comment["Submission_id"],
                            comment["Body"],
                        ),
                    )

                    # Enregistrement des changements pour chaque commentaire
                    connexion.commit()
                    print(f"Commentaire '{comment['Id']}' ajouté avec succès.")

                except sqlite3.IntegrityError as e:
                    print(
                        f"Commentaire '{comment['Id']}' - Erreur d'intégrité lors de l'ajout du commentaire : {e}"
                    )

                except sqlite3.Error as e:
                    print(
                        f"Commentaire '{comment['Id']}' - Une erreur est survenue lors de l'ajout du commentaire : {e}"
                    )

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

        finally:
            # Fermeture de la connexion
            connexion.close()

    def update_keywords_and_topic(
        self, dict: DbSubmission, LLMResponse: LLMKeywordsTopicResponseFormat
    ):
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
            formatted_keywords: str = ",".join(LLMResponse["keywords"])

            # Mise à jour des mots-clés et du sujet dans la table correspondante
            curseur.execute(
                """
                UPDATE Submission
                SET Keywords = ?, Topic = ?
                WHERE Id = ?
            """,
                (formatted_keywords, LLMResponse["topic"], dict["Id"]),
            )

            # Enregistrement des changements
            connexion.commit()
            print(f"Mots-clés et sujet mis à jour pour '{dict['Id']}' avec succès.")

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

        finally:
            # Fermeture de la connexion
            connexion.close()

    def update_all_keywords_and_topic(
        self, LLMAgent: LLMAgent, force_update: bool = False
    ):
        """
        Met à jour les mots-clés et le sujet de toutes les soumissions dans la table Submission.

        :param LLMAgent: LLMAgent - L'agent LLM utilisé pour générer les mots-clés et le sujet.
        :param force_update: bool - Indique si on doit écraser les valeurs existantes (par défaut False).
        """

        # Récupération de toutes les soumissions
        submissions = self.get_all_submissions()

        for submission in submissions:
            if not force_update and self._has_values_for_keywords_and_topic(submission):
                print(f"Soumission '{submission['Id']}' déjà traitée.")
                continue

            # Génération des mots-clés et du sujet pour chaque soumission
            LLMResponse = LLMAgent.request_keywords_and_topic(submission)
            self.update_keywords_and_topic(submission, LLMResponse)

    def get_all_users(self) -> list[DbUser]:
        """
        Récupère tous les utilisateurs de la table User.

        :return: list[User] - La liste de tous les utilisateurs.
        """

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
                user = DbUser(Id=row[0], Name=row[1], Genre=row[2], Age=row[3])
                users.append(user)

        except sqlite3.Error as e:
            print(f"Erreur lors de la récupération des utilisateurs : {e}")

        finally:
            connexion.close()

        return users

    def get_all_submissions(self) -> list[DbSubmission]:
        """
        Récupère toutes les soumissions de la table Submission.

        :return: list[Submission] - La liste de toutes les soumissions.
        """

        submissions: list[DbSubmission] = []

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la requête pour récupérer toutes les soumissions
            curseur.execute(
                "SELECT Id, Author_id, Created, Sub_id, Url, Title, Body, Keywords, Topic FROM Submission"
            )
            rows = curseur.fetchall()

            # Conversion des résultats en liste d'objets Submission
            for row in rows:
                submission = DbSubmission(
                    Id=row[0],
                    Author_id=row[1],
                    Created=datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S.%f"),
                    Sub_id=row[3],
                    Url=row[4],
                    Title=row[5],
                    Body=row[6],
                    Keywords=row[7].split(",") if row[7] else None,
                    Topic=row[8],
                )
                submissions.append(submission)

        except sqlite3.Error as e:
            print(f"Erreur lors de la récupération des soumissions : {e}")

        finally:
            connexion.close()

        return submissions

    def get_all_comments(self) -> list[DbComment]:
        """
        Récupère tous les commentaires de la table Comment.

        :return: list[Comment] - La liste de tous les commentaires.
        """

        comments: list[DbComment] = []

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Exécution de la requête pour récupérer tous les commentaires
            curseur.execute(
                "SELECT Id, Author_id, Created, Parent_id, Submission_id, Body FROM Comment"
            )
            rows = curseur.fetchall()

            # Conversion des résultats en liste d'objets Comment
            for row in rows:
                comment = DbComment(
                    Id=row[0],
                    Author_id=row[1],
                    Created=datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S.%f"),
                    Parent_id=row[3],
                    Submission_id=row[4],
                    Body=row[5],
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
                    keywords = row[0].split(
                        ","
                    )  # Supposer que les mots-clés sont séparés par des virgules
                    keyword_counter.update(keywords)

            # Insérer les mots-clés et leurs occurrences dans la table KeywordWeight
            for keyword, weight in keyword_counter.items():
                curseur.execute(
                    """
                    INSERT INTO KeywordWeight (Keyword, Weight)
                    VALUES (?, ?)
                """,
                    (keyword, weight),
                )

            # Sauvegarder les modifications
            connexion.commit()
            print("Table KeywordWeight mise à jour avec les occurrences des mots-clés.")

        except sqlite3.Error as e:
            print(f"Erreur lors du calcul des occurrences des mots-clés : {e}")

        finally:
            connexion.close()

    def categorize_keywords(self, chatgpt: LLMAgent, category_number: int):
        """
        Récupère les mots-clés et leur fréquence depuis la table KeywordWeight
        et les envoie à la fonction request_keyword_categorization de LLM pour les catégoriser.

        :param LLMAgent: LLMAgent - L'agent LLM utilisé pour catégoriser les mots-clés.
        :return: list[DbCategoryWeight] - Liste des catégories des mots-clés obtenus de la réponse de LLM.
        """

        try:
            # Connexion à la base de données
            connexion: sqlite3.Connection = sqlite3.connect(self._filepath)
            curseur: sqlite3.Cursor = connexion.cursor()

            # Récupération des mots-clés et de leur fréquence depuis KeywordWeight
            curseur.execute("SELECT Keyword, Weight FROM KeywordWeight")
            rows = curseur.fetchall()

            # Formatage des mots-clés pour la requête LLM
            # keywords = {row[0]: row[1] for row in rows}
            keywords: list[DbWeightedKeyword] = [
                {"Keyword": row[0], "Weight": row[1]} for row in rows
            ]

            # Préparation des données pour la requête LLM
            keyword_request = LLMCategoryRequestFormat(
                weighted_objects=keywords, category_number=category_number
            )

            # Envoi à la méthode request_keyword_categorization de l'agent LLM
            categories_response: list[DbWeightedCategory] = chatgpt.categorize_keywords(
                keyword_request
            )

            # Création de la table CategoryWeight si elle n'existe pas
            curseur.execute("""
                CREATE TABLE IF NOT EXISTS CategoryWeight (
                    Category TEXT PRIMARY KEY,
                    Weight INTEGER NOT NULL
                );
            """)

            # Vider la table CategoryWeight si elle existe déjà
            curseur.execute("DELETE FROM CategoryWeight")

            # Insérer les mots-clés et leurs occurrences dans la table CategoryWeight
            for category in categories_response:
                curseur.execute(
                    """
                    INSERT INTO CategoryWeight (Category, Weight)
                    VALUES (?, ?)
                """,
                    (category["Category"], category["Weight"]),
                )

            # Sauvegarder les modifications
            connexion.commit()
            print("Table CategoryWeight mise à jour avec les catégories des mots-clés.")

        except sqlite3.Error as e:
            print(
                f"Erreur lors de la récupération ou de la catégorisation des mots-clés : {e}"
            )

        finally:
            connexion.close()

    def calculate_submissions_count_by_date(self):
        """
        Cette fonction récupère les dates de soumission, compte les soumissions par jour
        et insère ou met à jour les résultats dans une table SubmissionDate.
        """

        try:
            # Connexion à la base de données
            connexion = sqlite3.connect(self._filepath)
            curseur = connexion.cursor()

            # Récupérer toutes les soumissions
            submissions: list[DbSubmission] = self.get_all_submissions()

            # Créer un dictionnaire pour compter les soumissions par jour
            date_count = defaultdict(int)

            # Parcourir les soumissions et compter les soumissions par date
            for submission in submissions:
                # Extraire uniquement la date (en omettant l'heure)
                date_only = submission[
                    "Created"
                ].date()  # Utilisation de .date() pour extraire la partie date

                # Compter la soumission pour cette date
                date_count[date_only] += 1

            # Création de la table SubmissionDate si elle n'existe pas
            curseur.execute("""
            CREATE TABLE IF NOT EXISTS SubmissionDate (
                Date TEXT PRIMARY KEY,
                NbSubmissions INTEGER NOT NULL
            )
            """)

            # Insérer ou mettre à jour les résultats dans la table SubmissionDate
            for date, count in date_count.items():
                try:
                    curseur.execute(
                        """
                    INSERT INTO SubmissionDate (Date, NbSubmissions) 
                    VALUES (?, ?)
                    ON CONFLICT(Date) 
                    DO UPDATE SET NbSubmissions = excluded.NbSubmissions
                    """,
                        (date, count),
                    )
                except sqlite3.IntegrityError as e:
                    print(
                        f"Erreur d'intégrité lors de l'insertion des données pour la date {date}: {e}"
                    )
                except sqlite3.Error as e:
                    print(
                        f"Une erreur est survenue lors de l'insertion des données pour la date {date}: {e}"
                    )

            # Validation des changements
            connexion.commit()

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

        finally:
            # Fermeture de la connexion
            if connexion:
                connexion.close()

    def calculate_submissions_count_by_weekday(self):
        """
        Cette fonction récupère les dates de soumission, compte les soumissions par jour de la semaine,
        et insère ou met à jour les résultats dans une table SubmissionWeekdayCount avec le jour de la semaine,
        son ordre et le nombre de soumissions.
        """

        try:
            # Connexion à la base de données
            connexion = sqlite3.connect(self._filepath)
            curseur = connexion.cursor()

            # Récupérer toutes les soumissions
            submissions: list[DbSubmission] = self.get_all_submissions()

            # Créer un dictionnaire pour compter les soumissions par jour de la semaine
            weekday_count = defaultdict(int)

            # Parcourir les soumissions et compter les soumissions par jour de la semaine
            for submission in submissions:
                # Extraire le jour de la semaine (lundi = 0, dimanche = 6)
                weekday = submission["Created"].weekday()

                # Compter la soumission pour ce jour de la semaine
                weekday_count[weekday] += 1

            # Dictionnaire pour correspondre l'ordre et le nom du jour de la semaine
            weekdays = {
                0: "Lundi",
                1: "Mardi",
                2: "Mercredi",
                3: "Jeudi",
                4: "Vendredi",
                5: "Samedi",
                6: "Dimanche",
            }

            # Création de la table SubmissionWeekdayCount si elle n'existe pas
            curseur.execute("""
            CREATE TABLE IF NOT EXISTS SubmissionWeekdayCount (
                Weekday TEXT PRIMARY KEY,
                Id INTEGER NOT NULL,
                NbSubmissions INTEGER NOT NULL
            )
            """)

            # Insérer ou mettre à jour les résultats dans la table SubmissionWeekdayCount
            for weekday, count in weekday_count.items():
                try:
                    curseur.execute(
                        """
                    INSERT INTO SubmissionWeekdayCount (Weekday, Id, NbSubmissions) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(Weekday) 
                    DO UPDATE SET NbSubmissions = excluded.NbSubmissions
                    """,
                        (weekdays[weekday], weekday, count),
                    )
                except sqlite3.IntegrityError as e:
                    print(
                        f"Erreur d'intégrité lors de l'insertion des données pour le jour {weekdays[weekday]}: {e}"
                    )
                except sqlite3.Error as e:
                    print(
                        f"Une erreur est survenue lors de l'insertion des données pour le jour {weekdays[weekday]}: {e}"
                    )

            # Validation des changements
            connexion.commit()

        except sqlite3.Error as e:
            print(
                f"Une erreur est survenue lors de la connexion ou de l'exécution des requêtes : {e}"
            )

        finally:
            # Fermeture de la connexion
            if connexion:
                connexion.close()
