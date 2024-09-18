import pandas as pd
from neo4j import GraphDatabase

# FROM https://huggingface.co/datasets/EmbeddingStudio/amazon-products-with-images

data = pd.read_parquet("../data/eval-00000-of-00001.parquet", engine='pyarrow')
data = data.drop(columns=["Image", "Raw Image"])


# Function to parse product specifications
def parse_specifications(spec_str):
    if pd.isna(spec_str) or spec_str == '':
        return {}
    specs = spec_str.split('|')
    spec_dict = {}
    for spec in specs:
        if ':' in spec:
            key, value = spec.split(':', 1)
            spec_dict[key.strip()] = value.strip()
        else:
            # Handle cases where there is no colon
            spec_dict[spec.strip()] = ''
    return spec_dict

def create_product(tx, product_id, name, description, price):
    tx.run(
        """
        MERGE (p:Product {id: $product_id})
        SET p.name = $name, p.description = $description, p.price = $price
        """,
        product_id=product_id, name=name, description=description, price=price
    )

def create_category(tx, category_name):
    tx.run(
        """
        MERGE (c:Category {name: $name})
        """,
        name=category_name
    )

def create_product_category_relationship(tx, product_id, category_name):
    tx.run(
        """
        MATCH (p:Product {id: $product_id}), (c:Category {name: $category_name})
        MERGE (p)-[:BELONGS_TO]->(c)
        """,
        product_id=product_id, category_name=category_name
    )

def create_specification(tx, spec_key, spec_value):
    tx.run(
        """
        MERGE (s:Specification {key: $key, value: $value})
        """,
        key=spec_key, value=spec_value
    )

def create_product_specification_relationship(tx, product_id, spec_key, spec_value):
    tx.run(
        """
        MATCH (p:Product {id: $product_id}), (s:Specification {key: $spec_key, value: $spec_value})
        MERGE (p)-[:HAS_SPECIFICATION]->(s)
        """,
        product_id=product_id, spec_key=spec_key, spec_value=spec_value
    )


# Apply the function to create a new column with specifications as a dictionary
data['Parsed Specifications'] = data['Product Specification'].apply(
    parse_specifications)

# Replace the following with your Neo4j credentials
uri = "bolt://localhost:7687"
user = "neo4j"
password = "myPassword"  # Replace with your Neo4j password

driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session() as session:
    for index, row in data.iterrows():
        product_id = f"product_{index}"
        name = row['Product Name']
        description = row['Description'] if pd.notna(
            row['Description']) else ''
        price = row['Selling Price'] if pd.notna(row['Selling Price']) else ''

        # Create Product node
        session.execute_write(create_product, product_id, name,
                                  description, price)

        # Create Category nodes and relationships
        if 'Category' not in row or pd.isna(row['Category']):
            continue
        categories = [cat.strip() for cat in row['Category'].split('|')]
        for category in categories:
            session.execute_write(create_category, category)
            session.execute_write(create_product_category_relationship,
                                      product_id, category)

        # Create Specification nodes and relationships
        specs = row['Parsed Specifications']
        for spec_key, spec_value in specs.items():
            session.execute_write(create_specification, spec_key,
                                      spec_value)
            session.execute_write(
                create_product_specification_relationship, product_id,
                spec_key, spec_value)
