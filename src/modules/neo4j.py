import os
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
import logging

_ = load_dotenv()
logging.basicConfig(level=logging.INFO)


def handle_transaction_errors(func) -> None:
    """
    Decorator to handle transaction errors and rollback if necessary.
    """
    def wrapper(*args, **kwargs) -> None:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if hasattr(args[0], 'tx') and args[0].tx:
                args[0].tx.rollback()
            logging.error(f"Transaction failed: {e}")
            raise e

    return wrapper


class Neo4j:
    """
    A class to interact with a Neo4j database for managing products, categories, and specifications.
    Attributes:
        driver (neo4j.GraphDatabase.driver): A Neo4j driver for interacting with the database.
        tx (neo4j.Transaction): The current transaction session.
    """
    def __init__(self) -> None:
        """
        Initializes the Neo4j class by loading the Neo4j credentials from environment variables
        and establishing a connection to the database.
        """
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        if not uri or not user or not password:
            raise ValueError("Please ensure that NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD are set in the environment.")
        self.driver = GraphDatabase.driver(uri,
                                           auth=(user, password),
                                           max_connection_lifetime=1000,
                                           max_connection_pool_size=50)
        self.tx = None

    def close(self) -> None:
        """
        Closes the Neo4j driver connection.
        """
        if self.driver:
            self.driver.close()

    @handle_transaction_errors
    def create(self, data: pd.DataFrame) -> None:
        """
        Processes the provided data, creating nodes and relationships in the Neo4j database for
        each product, category, and specification.

        :param data: The data containing products, categories, and specifications.
        """
        self.validate_data(data)
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                self.tx = tx

                logging.info("Starting data insertion...")

                for index, row in data.iterrows():
                    product_id = f"product_{index}"
                    name = row['Product Name']
                    description = row.get('Description', '') or ''
                    price = row.get('Selling Price', '') or ''

                    # Create Product node
                    self.create_product(product_id, name, description, price)

                    # Create Category nodes and relationships
                    self._process_categories(product_id, row.get('Category', ''))

                    # Create Specification nodes and relationships
                    self._process_specifications(product_id, row.get('Parsed Specifications', {}))

                self.tx.commit()
                logging.info("Data inserted successfully.")

    def _process_categories(self, product_id: str, categories: str):
        """
        Helper function to process and create category nodes and relationships.

        :param product_id: The product identifier.
        :param categories: A string of categories separated by a pipe ('|').
        """
        if not categories:
            return

        for category in [cat.strip() for cat in categories.split('|')]:
            self.create_category(category)
            self.create_product_category_relationship(product_id, category)

    def _process_specifications(self, product_id: str, specs: dict):
        """
        Helper function to process and create specification nodes and relationships.

        :param product_id: The product identifier.
        :param specs: A dictionary of specifications where keys are specification names and values are the
                        specification values.
        """
        for spec_key, spec_value in specs.items():
            self.create_specification(spec_key, spec_value)
            self.create_product_specification_relationship(product_id, spec_key, spec_value)

    def create_product(self, product_id: str, name: str, description: str, price: str) -> None:
        """
        Creates or updates a Product node in the Neo4j database.
        :param product_id: The product identifier.
        :param name: The product name.
        :param description: The product description.
        :param price: The product price.
        """
        self.tx.run(
            """
            MERGE (p:Product {id: $product_id})
            SET p.name = $name, p.description = $description, p.price = $price
            """,
            product_id=product_id, name=name, description=description, price=price
        )

    def create_category(self, category_name: str) -> None:
        """
        Creates or updates a Category node in the Neo4j database.

        :param category_name: The name of the category.
        """
        self.tx.run(
            """
            MERGE (c:Category {name: $name})
            """,
            name=category_name
        )

    def create_product_category_relationship(self, product_id: str, category_name: str) -> None:
        """
        Creates a BELONGS_TO relationship between a Product and a Category in the Neo4j database.

        :param product_id: The product identifier.
        :param category_name: The name of the category.
        """
        self.tx.run(
            """
            MATCH (p:Product {id: $product_id}), (c:Category {name: $category_name})
            MERGE (p)-[:BELONGS_TO]->(c)
            """,
            product_id=product_id, category_name=category_name
        )

    def create_specification(self, spec_key: str, spec_value: str) -> None:
        """
        Creates or updates a Specification node in the Neo4j database.

        :param spec_key: The key of the specification.
        :param spec_value: The value of the specification.
        """
        self.tx.run(
            """
            MERGE (s:Specification {key: $key, value: $value})
            """,
            key=spec_key, value=spec_value
        )

    def create_product_specification_relationship(self, product_id: str, spec_key: str, spec_value: str) -> None:
        """
        Creates a HAS_SPECIFICATION relationship between a Product and a Specification in the Neo4j database.

        :param product_id: The product identifier.
        :param spec_key: The key of the specification.
        :param spec_value: The value of the specification.
        """
        self.tx.run(
            """
            MATCH (p:Product {id: $product_id}), (s:Specification {key: $spec_key, value: $spec_value})
            MERGE (p)-[:HAS_SPECIFICATION]->(s)
            """,
            product_id=product_id, spec_key=spec_key, spec_value=spec_value
        )

    def validate_data(self, data: pd.DataFrame) -> None:
        """
        Validates the provided data to ensure that all required columns exist.

        :param data: The data to be validated.

        :raises ValueError: If any required column is missing.
        """
        required_columns = ['Product Name', 'Parsed Specifications']
        for column in required_columns:
            if column not in data.columns:
                raise ValueError(f"Missing required column: {column}")

    def create_products_in_batch(self, products) -> None:
        """
        Create products in a batch operation using UNWIND for better performance.

        :param products: List of product dictionaries with fields `id`, `name`, `description`, `price`.
        """
        query = """
        UNWIND $products AS product
        MERGE (p:Product {id: product.id})
        SET p.name = product.name, p.description = product.description, p.price = product.price
        """
        self.tx.run(query, products=products)
