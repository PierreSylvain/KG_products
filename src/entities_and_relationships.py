from langchain import PromptTemplate, LLMChain
from langchain.llms import Ollama

# Initialize the Ollama LLM with the desired model
llm = Ollama(model='llama3.1:latest')

# Define the prompt template
template = """
You are an expert in knowledge graphs and Neo4j.

Given the following extracted data:
- Named Entities: {named_entities}
- Noun Phrases: {noun_phrases}
- Verbs: {verbs}

Please perform the following tasks:

1. Identify unique entities from the named entities and noun phrases.
2. Assign appropriate name to these entities (e.g., 'Event', 'Season', 'Item').
3. Determine possible relationships between these entities based on the verbs provided.
4. Generate Neo4j Cypher commands to create the nodes and relationships.

Instructions:

- Use the MERGE command to create nodes to avoid duplicates.
- Use uppercase letters for labels and relationship types.
- For relationships, use the verbs as relationship types in uppercase (e.g., 'CELEBRATES', 'ENJOYS').
- Ensure that the Cypher commands are syntactically correct.

Output only the Cypher commands without any explanations.
"""

prompt = PromptTemplate(
    input_variables=["named_entities", "noun_phrases", "verbs"],
    template=template,
)

# Create the LLMChain with the LLM and prompt
chain = LLMChain(llm=llm, prompt=prompt)

# Define the data
named_entities = [('Christmas', 'DATE')]
noun_phrases = ['The Fall', 'Winter Season', 'The Perfect Cuddly', 'Plush', 'Christmas']
verbs = ['celebrate', 'enjoy']

# Run the chain to get the output
output = chain.run({
    'named_entities': named_entities,
    'noun_phrases': noun_phrases,
    'verbs': verbs,
})

for line in output.split('\n'):
    if line.startswith('```') or line.startswith('**'):
        continue
    if line.contains('['):
        continue
    print(line)

"""
MERGE (p:Product {name: "Ben&Jonah Throw Blankets, Perfect for The Fall & Winter. Fun Designs for Kids/Teens Great Present Friends and Family! 100% Soft Polyester. Camping/Travel Size (Rainbow Unicorn)"})
MERGE (ws:SEASON {name: "Winter Season"})
MERGE (p)-[:CELEBRATES]->(c)

MATCH (p:Product), (ws:SEASON) WHERE p.name = "Ben&Jonah Throw Blankets, Perfect for The Fall & Winter. Fun Designs for Kids/Teens Great Present Friends and Family! 100% Soft Polyester. Camping/Travel Size (Rainbow Unicorn)" AND ws.name = "Winter Season"   
CREATE (p)-[r: CELEBRATES]->(ws)   
RETURN p,ws   

DELETE ALL ----
MATCH (n) DETACH DELETE n
"""