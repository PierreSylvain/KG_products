"""
Data are get from the following dataset:
https://huggingface.co/datasets/EmbeddingStudio/amazon-products-with-images
"""
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from src.modules.neo4j import Neo4j
from src.modules.parse_specifications import parse_specifications


def parallel_parse_specifications(specifications_list, func, max_workers=4):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(func, specifications_list))
    return results


def load_data():
    eval_data = pl.read_parquet("../data/eval-00000-of-00001.parquet")

    # Extract "Product Specification" column as a Python list
    spec_list = eval_data["Product Specification"].to_list()

    # Apply the custom parsing function in parallel
    parsed_specs = parallel_parse_specifications(spec_list,
                                                 parse_specifications,
                                                 max_workers=4)

    # Drop the specified columns
    eval_data = eval_data.drop(["Image", "Raw Image"])

    # Add the processed specifications back as a new column
    eval_data = eval_data.with_columns(
        pl.Series("Parsed Specifications", parsed_specs))

    # Save the resulting DataFrame back to a parquet file
    eval_data.write_parquet("../data/product_data.parquet")
    return eval_data


if __name__ == "__main__":
    data = load_data()
    neo4j = Neo4j()
    neo4j.create(data)
