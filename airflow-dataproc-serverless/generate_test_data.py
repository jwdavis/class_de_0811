#!/usr/bin/env python3
"""
Test Data Generator for Customer Analytics ETL Pipeline

This script generates realistic customer transaction data in Parquet format
for testing the Airflow DAG that processes daily customer analytics.

Usage:
    python generate_test_data.py --date 2024-01-15 --output gs://bucket/path/transactions.parquet
    python generate_test_data.py --date 2024-01-15 --output local_transactions.parquet --num-customers 1000
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd


def generate_customer_transactions(
    date_str: str, num_customers: int = 500, num_transactions: int = 5000
) -> pd.DataFrame:
    """
    Generate realistic customer transaction data for a specific date.

    Args:
        date_str: Date in YYYY-MM-DD format
        num_customers: Number of unique customers
        num_transactions: Total number of transactions to generate

    Returns:
        DataFrame with transaction data
    """

    # Parse the target date
    target_date = datetime.strptime(date_str, "%Y-%m-%d")

    # Generate customer IDs (mix of new and returning customers)
    customer_base = [
        f"CUST_{str(uuid.uuid4())[:8].upper()}" for _ in range(num_customers)
    ]

    # Create realistic customer segments
    segments = {
        "premium": {"weight": 0.15, "avg_amount": 250, "frequency": 3.5},
        "regular": {"weight": 0.60, "avg_amount": 85, "frequency": 1.8},
        "occasional": {"weight": 0.25, "avg_amount": 45, "frequency": 0.8},
    }

    # Assign customers to segments
    customer_segments = {}
    for customer_id in customer_base:
        segment = np.random.choice(
            list(segments.keys()), p=[segments[s]["weight"] for s in segments.keys()]
        )
        customer_segments[customer_id] = segment

    # Product categories with different price ranges
    categories = {
        "Electronics": {"min": 50, "max": 2000, "weight": 0.25},
        "Clothing": {"min": 15, "max": 300, "weight": 0.30},
        "Home & Garden": {"min": 20, "max": 500, "weight": 0.20},
        "Books & Media": {"min": 5, "max": 100, "weight": 0.15},
        "Sports & Outdoors": {"min": 25, "max": 800, "weight": 0.10},
    }

    # Payment methods with realistic distribution
    payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
    payment_weights = [0.45, 0.25, 0.15, 0.08, 0.07]

    # Store locations
    store_locations = [
        "New York, NY",
        "Los Angeles, CA",
        "Chicago, IL",
        "Houston, TX",
        "Phoenix, AZ",
        "Philadelphia, PA",
        "San Antonio, TX",
        "San Diego, CA",
        "Dallas, TX",
        "San Jose, CA",
        "Austin, TX",
        "Jacksonville, FL",
        "San Francisco, CA",
        "Columbus, OH",
        "Charlotte, NC",
        "Online",
    ]

    transactions = []

    for _ in range(num_transactions):
        # Select customer (some customers have multiple transactions)
        customer_id = np.random.choice(customer_base)
        segment = customer_segments[customer_id]

        # Generate transaction timestamp throughout the day
        hour = np.random.choice(range(24), p=get_hourly_distribution())
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        timestamp = target_date.replace(hour=hour, minute=minute, second=second)

        # Select product category
        category = np.random.choice(
            list(categories.keys()),
            p=[categories[c]["weight"] for c in categories.keys()],
        )

        # Generate transaction amount based on customer segment and category
        base_amount = np.random.uniform(
            categories[category]["min"], categories[category]["max"]
        )

        # Apply segment multiplier
        segment_multiplier = {
            "premium": np.random.uniform(1.2, 2.5),
            "regular": np.random.uniform(0.8, 1.3),
            "occasional": np.random.uniform(0.5, 1.1),
        }[segment]

        amount = round(base_amount * segment_multiplier, 2)

        # Generate other fields
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "timestamp": timestamp,
            "amount": amount,
            "category": category,
            "payment_method": np.random.choice(payment_methods, p=payment_weights),
            "store_location": random.choice(store_locations),
            "customer_segment": segment,
            "discount_applied": (
                random.choice([0, 5, 10, 15, 20]) if random.random() < 0.3 else 0
            ),
            "is_return": random.random() < 0.05,  # 5% chance of return
            "currency": "USD",
        }

        # Apply return logic (negative amount for returns)
        if transaction["is_return"]:
            transaction["amount"] = -abs(transaction["amount"])

        transactions.append(transaction)

    # Convert to DataFrame
    df = pd.DataFrame(transactions)

    # Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Add some derived fields that might be useful for analytics
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["is_weekend"] = df["timestamp"].dt.weekday >= 5

    return df


def get_hourly_distribution() -> List[float]:
    """
    Return realistic hourly distribution for transactions.
    Higher activity during business hours and evening.
    """
    # 24-hour distribution (probabilities sum to 1.0)
    distribution = [
        0.01,
        0.01,
        0.01,
        0.01,
        0.01,
        0.02,  # 0-5 AM: Very low
        0.03,
        0.04,
        0.05,
        0.06,
        0.07,
        0.08,  # 6-11 AM: Morning increase
        0.09,
        0.08,
        0.07,
        0.06,
        0.07,
        0.08,  # 12-5 PM: Afternoon peak
        0.09,
        0.08,
        0.06,
        0.04,
        0.03,
        0.02,  # 6-11 PM: Evening decline
    ]

    # Normalize to ensure sum equals exactly 1.0
    total = sum(distribution)
    return [p / total for p in distribution]


def save_to_gcs(df: pd.DataFrame, gcs_path: str) -> None:
    """Save DataFrame to Google Cloud Storage as Parquet."""
    try:
        # Try to use gcsfs for direct GCS upload
        import gcsfs

        fs = gcsfs.GCSFileSystem()

        with fs.open(gcs_path, "wb") as f:
            df.to_parquet(f, engine="pyarrow", index=False, coerce_timestamps="us")

        print(f"✅ Data successfully uploaded to {gcs_path}")

    except ImportError:
        # Fallback: save locally then upload with gsutil
        local_filename = "temp_transactions.parquet"
        df.to_parquet(
            local_filename, engine="pyarrow", index=False, coerce_timestamps="us"
        )

        # Upload using gsutil
        os.system(f"gsutil cp {local_filename} {gcs_path}")
        os.remove(local_filename)

        print(f"✅ Data uploaded to {gcs_path} via gsutil")


def save_locally(df: pd.DataFrame, filepath: str) -> None:
    """Save DataFrame locally as Parquet."""
    df.to_parquet(filepath, engine="pyarrow", index=False, coerce_timestamps="us")
    print(f"✅ Data saved locally to {filepath}")


def print_data_summary(df: pd.DataFrame, date_str: str) -> None:
    """Print summary statistics of the generated data."""
    print(f"\n📊 Data Summary for {date_str}")
    print("=" * 50)
    print(f"Total Transactions: {len(df):,}")
    print(f"Unique Customers: {df['customer_id'].nunique():,}")
    print(f"Total Revenue: ${df[df['amount'] > 0]['amount'].sum():,.2f}")
    print(f"Total Returns: ${abs(df[df['amount'] < 0]['amount'].sum()):,.2f}")
    print(f"Average Transaction: ${df['amount'].mean():.2f}")
    print(f"Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    print("\n📈 Top Categories by Revenue:")
    category_revenue = (
        df[df["amount"] > 0]
        .groupby("category")["amount"]
        .sum()
        .sort_values(ascending=False)
    )
    for category, revenue in category_revenue.head().items():
        print(f"  {category}: ${revenue:,.2f}")

    print("\n👥 Customer Segments:")
    segment_stats = (
        df.groupby("customer_segment")
        .agg({"customer_id": "nunique", "amount": ["count", "mean", "sum"]})
        .round(2)
    )
    print(segment_stats)


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data for Customer Analytics ETL"
    )
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--output", required=True, help="Output path (local file or gs:// GCS path)"
    )
    parser.add_argument(
        "--num-customers", type=int, default=500, help="Number of unique customers"
    )
    parser.add_argument(
        "--num-transactions",
        type=int,
        default=5000,
        help="Total number of transactions",
    )
    parser.add_argument("--summary", action="store_true", help="Print data summary")

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("❌ Error: Date must be in YYYY-MM-DD format")
        return 1

    print(f"🚀 Generating test data for {args.date}...")
    print(f"   Customers: {args.num_customers:,}")
    print(f"   Transactions: {args.num_transactions:,}")

    # Generate the data
    df = generate_customer_transactions(
        date_str=args.date,
        num_customers=args.num_customers,
        num_transactions=args.num_transactions,
    )

    # Save the data
    if args.output.startswith("gs://"):
        save_to_gcs(df, args.output)
    else:
        save_locally(df, args.output)

    # Print summary if requested
    if args.summary:
        print_data_summary(df, args.date)

    print(f"\n✨ Test data generation complete!")
    return 0


if __name__ == "__main__":
    exit(main())
