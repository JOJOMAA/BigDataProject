"""
MongoDB Data Reader - Wien Demografie
======================================

Authors: Johannes Mantler, Johannes Reitterer, Nicolas Nemeth

Usage:
    from read_mongo_data import WienDemografieReader

    reader = WienDemografieReader()
    df_pop = reader.get_population_data()
    df_births = reader.get_births_data()
    df_merged = reader.get_merged_data()
    reader.close()
"""

import pandas as pd
from pymongo import MongoClient
import sys

MONGO_CONFIG = {
    'uri': "mongodb://admin:admin123@localhost:27017/",
    'auth_source': "admin",
    'database': "wien_demografie_db",
    'use_docker': True
}


class WienDemografieReader:
    """
    Reader class for accessing Wien demographic data from MongoDB.
    """

    def __init__(self):
        """Initialize connection to MongoDB database."""
        try:
            if MONGO_CONFIG['use_docker']:
                self.client = MongoClient(
                    MONGO_CONFIG['uri'],
                    serverSelectionTimeoutMS=5000,
                    authSource=MONGO_CONFIG['auth_source']
                )
            else:
                self.client = MongoClient(
                    MONGO_CONFIG['uri'],
                    serverSelectionTimeoutMS=5000
                )

            self.client.server_info()
            self.db = self.client[MONGO_CONFIG['database']]

            print(f"Connected to MongoDB: {MONGO_CONFIG['database']}")

        except Exception as e:
            print(f"ERROR: MongoDB connection failed - {e}")
            print("\nTroubleshooting:")
            print("1. Ensure MongoDB is running: docker ps")
            print("2. Start MongoDB: docker-compose up -d")
            sys.exit(1)

    def get_population_data(self):
        """
        Retrieve all population data from MongoDB.

        Returns:
            DataFrame: Complete population dataset
        """
        cursor = self.db.population.find()
        data = list(cursor)

        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        print(f"Loaded population data: {len(df):,} records")
        return df

    def get_births_data(self):
        """
        Retrieve all birth data from MongoDB.

        Returns:
            DataFrame: Complete birth dataset
        """
        cursor = self.db.births.find()
        data = list(cursor)

        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        print(f"Loaded births data: {len(df):,} records")
        return df

    def get_merged_data(self):
        """
        Retrieve merged analysis data from MongoDB.

        Returns:
            DataFrame: Complete merged dataset (recommended for ML/Analysis)
        """
        cursor = self.db.merged_analysis.find()
        data = list(cursor)

        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        print(f"Loaded merged data: {len(df):,} records")
        return df

    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        print("MongoDB connection closed")


def main():
    """
    Example usage of WienDemografieReader.
    """
    print("=" * 70)
    print("WIEN DEMOGRAFIE DATA READER")
    print("=" * 70)

    reader = WienDemografieReader()

    print("\nLoading all datasets...")

    df_population = reader.get_population_data()
    df_births = reader.get_births_data()
    df_merged = reader.get_merged_data()

    print("\n" + "=" * 70)
    print("DATA LOADED SUCCESSFULLY")
    print("=" * 70)

    print(f"\nPopulation data:")
    print(f"  Shape: {df_population.shape}")
    print(f"  Columns: {list(df_population.columns)[:5]}...")

    print(f"\nBirths data:")
    print(f"  Shape: {df_births.shape}")
    print(f"  Columns: {list(df_births.columns)}")

    print(f"\nMerged data (for ML/Analysis):")
    print(f"  Shape: {df_merged.shape}")
    print(f"  Columns: {list(df_merged.columns)}")

    print(f"\nFirst 3 rows of merged data:")
    print(df_merged.head(3))

    reader.close()


if __name__ == "__main__":
    main()