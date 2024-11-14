from datetime import datetime
from typing import NotRequired, TypedDict

"""
This module defines the TypedDicts used to represent the different types of data stored in the database.
"""


class DbUser(TypedDict):
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


class DbSubmission(TypedDict):
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


class DbComment(TypedDict):
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


class DbWeightedKeyword(TypedDict):
    """
    This module defines the TypedDict for representing keyword weight information in the database.

    Attributes:
        Keyword (str): The keyword.
        Weight (int): The weight of the keyword.
    """

    Keyword: str
    Weight: int


class DbWeightedCategory(TypedDict):
    """
    This module defines the TypedDict for representing category weight information in the database.

    Attributes:
        Category (str): The category.
        Weight (int): The weight of the category.
    """

    Category: str
    Weight: int
