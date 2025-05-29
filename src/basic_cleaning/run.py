#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info('Artifact %s was retrieved locally', args.input_artifact)
    df = pd.read_csv(artifact_local_path, low_memory=False)

    # Remove outliers
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # hook for sample 2.csv
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_dataset.csv", index=False)
    logger.info("Dataset was cleaned and ouliers removed.")

    output_artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    output_artifact.add_file("clean_dataset.csv")
    run.log_artifact(output_artifact)

    run.finish()
    logger.info('Artifact %s was uploaded', args.output_artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Path to the raw input artifact in WandB",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Path to the output artifact in WandB",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum acceptable price of the housing, used for cleaning",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Minimum acceptable price of the housing, used for cleaning",
        required=True
    )


    args = parser.parse_args()

    go(args)
