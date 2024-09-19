import spacy
from pydantic import BaseModel
from typing import List


def clean_data(text: str) -> str:
    """
    Clean the text data by removing newlines and tabs.

    :param text: raw text

    :return: cleaned text
    """
    text = text.replace("\n", "")
    text = text.replace("\t", "")
    return text.strip()


class NamedEntity(BaseModel):
    """
    Named entity extracted from a text.
    """
    text: str
    label: str


class EntityData(BaseModel):
    """
    Extracted entities, noun phrases, and verbs from a text.
    """
    named_entities: List[NamedEntity]
    noun_phrases: List[str]
    verbs: List[str]


class EntityExtractor:
    """
    Extracts various linguistic features such as named entities, noun phrases,
    and verbs from text.
    """

    def __init__(self):
        """
        Initializes the NLP model for entity extraction.
        """
        self.nlp = spacy.load('en_core_web_sm')

    def extract(self, text: str) -> EntityData:
        """
        Extracts named entities, noun phrases, and verbs from the input text.

        :param text: Input text to process.

        :return: A model containing lists of named entities, noun phrases,
                    and verbs.
        """
        named_entities = self._extract_named_entities(text)
        noun_phrases = self._extract_noun_phrases(text)
        verbs = self._extract_verbs(text)
        return EntityData(
            named_entities=named_entities,
            noun_phrases=noun_phrases,
            verbs=verbs
        )

    def _extract_named_entities(self, text: str) -> List[NamedEntity]:
        """
        Extracts named entities from the input text.

        :param text: Input text to process.
        :return: A list of tuples where each tuple contains the entity text
        """
        doc = self.nlp(text)
        return [NamedEntity(text=ent.text, label=ent.label_) for ent in
                doc.ents]

    def _extract_noun_phrases(self, text: str) -> List[str]:
        """
        Extracts noun phrases from the input text.

        :param text: Input text to process.

        :return: A list of noun phrases found in the text.
        """
        doc = self.nlp(text)
        return [clean_data(chunk.text) for chunk in doc.noun_chunks]

    def _extract_verbs(self, text: str) -> List[str]:
        """
        Extracts verbs from the input text.

        :param text: Input text to process.

        :return: A list of verbs found in the text.
        """
        doc = self.nlp(text)
        return [token.lemma_ for token in doc if token.pos_ == 'VERB']


if __name__ == '__main__':
    # Example usage
    entity_extractor = EntityExtractor()
    text_to_analyze = ("3 different themes to collect Naptime Nursery, Preschool, Play Room.")

    extracted_data = entity_extractor.extract(text_to_analyze)
    print(extracted_data.model_dump_json(indent=2))
