import spacy
import pandas as pd
from neo4j import GraphDatabase

data = pd.read_parquet("../data/eval-00000-of-00001.parquet", engine='pyarrow')

# Load the English language model
nlp = spacy.load('en_core_web_sm')


def extract_named_entities(description):
    doc = nlp(description)
    entities = [(ent.text, ent.label_) for ent in doc.ents]
    return entities


def extract_noun_phrases(description):
    doc = nlp(description)
    noun_phrases = [chunk.text for chunk in doc.noun_chunks]
    return noun_phrases


def extract_verbs(description):
    doc = nlp(description)
    verbs = [token.lemma_ for token in doc if token.pos_ == 'VERB']
    return verbs


description = data['Description'][3]  # Second product's description
print(description)
# Extract entities
entities = extract_named_entities(description)
print("Named Entities:", entities)

# Extract noun phrases
noun_phrases = extract_noun_phrases(description)
print("Noun Phrases:", noun_phrases)

# Extract verbs
verbs = extract_verbs(description)
print("Verbs:", verbs)
