import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CustomerETL").getOrCreate()

    # Read raw transaction data
    df = spark.read.parquet(args.input_path)

    # Your ETL transformations here
    processed_df = df.groupBy("customer_id").agg(
        F.countDistinct("transaction_id").alias("unique_transactions"),
        F.countDistinct("category").alias("distinct_categories"),
        F.countDistinct("payment_method").alias("distinct_payment_methods"),
        F.avg("amount").alias("avg_transaction_amount"),
    )

    # Write to output
    processed_df.write.mode("overwrite").parquet(args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
