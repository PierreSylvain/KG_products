"""
Data are get from the following dataset:
https://huggingface.co/datasets/EmbeddingStudio/amazon-products-with-images
"""
import pandas as pd

from src.modules.neo4j import Neo4j
from src.modules.parse_specifications import parse_specifications


def load_data():

    eval_data = pd.read_parquet("../data/eval-00000-of-00001.parquet",
                                engine='pyarrow')
    eval_data = eval_data.drop(columns=["Image", "Raw Image"])
    # Create a new column with specifications as a dictionary
    eval_data['Parsed Specifications'] = eval_data[
        'Product Specification'].apply(parse_specifications)
    eval_data.to_json("../data/eval_data.json",
                      orient="records",
                      lines=True)
    return data


if __name__ == "__main__":
    data = load_data()
    neo4j = Neo4j()
    neo4j.create(data)
