import polars as pl
from src.modules.neo4j import Neo4j

def create_main_entities():
    data = pl.read_parquet("../data/product_data.parquet")
    neo4j = Neo4j()
    neo4j.create(data)

if __name__ == "__main__":
    create_main_entities()
