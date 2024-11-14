from Database.Types import DbSubmission
import json
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from .Types import LLMResponseFormat
import os


class LLMAgent:
    _model: AzureChatOpenAI
    _keywords_and_topic_system_prompt: str = """
        Tu es un assistant de recherche qui aide un chercheur à trouver des
        mots-clés pour un article, dans une démarche de recherche documentaire
        et d'archivage bibliographique. Extrait seulement 3 mots-clés (pas plus,
        pas moins) du texte suivant ainsi que la thématique générale de l'article,
        qui doit être un concept très général, rien de spécifique (par exemple,
        garder seulement "Accident de voiture" pour "Accident de voiture et
        dommages potentiels" ou "Accident de voiture sans dommage apparent").
        La thématique doit être formulée ou 3 ou 4 mots environ. La réponse
        doit être formattée en JSON avec les clés "keywords", qui est une liste
        de string et "topic" qui est une string.
    """

    def __init__(self):
        self._model: AzureChatOpenAI = AzureChatOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        )

    def request_keywords_and_topic(self, text: DbSubmission) -> LLMResponseFormat:
        prompt: dict[str, str] = {
            "system": self._keywords_and_topic_system_prompt,
            "payload": text["Body"],
        }

        messages: list[SystemMessage | HumanMessage] = [
            SystemMessage(content=prompt["system"]),
            HumanMessage(content=prompt["payload"]),
        ]

        response: BaseMessage = self._model.invoke(messages)
        return json.loads(str(response.content))
