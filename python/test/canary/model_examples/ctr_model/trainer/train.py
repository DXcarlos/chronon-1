"""
Test XGBoost model example to test model training + deployment
"""
import argparse
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from typing import Dict, List, Tuple
from pathlib import Path

from google.cloud import bigquery
import os

GCP_PROJECT_ID = "canary-443022"

def get_training_data_df(input_table: str, start_ds: str, end_ds: str) -> Tuple[pd.DataFrame, List[int]]:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # 2. Define your Query
    sql_query = f"""
        SELECT coalesce(user_id_click_event_average_7d,0), coalesce(listing_id_price_cents,0) AS listing_price_cents, coalesce(price_log,0), price_bucket, label
        FROM {input_table} where ds BETWEEN '{start_ds}' AND '{end_ds}'
    """

    print(f"Running query to fetch training data from {input_table} between {start_ds} and {end_ds}:")
    print(sql_query)

    # 3. Run the query and convert to DataFrame
    df = client.query(sql_query).to_dataframe()

    # Split to X_train_df, y_train
    x_train_df = df.drop(columns=['label'])
    y_train = df['label'].tolist()
    return x_train_df, y_train

def train_model(
    X_train: pd.DataFrame,
    y_train: List[int],
    max_depth: int = 4,
    eta: float = 0.1,
    num_boost_round: int = 50,
    seed: int = 42
) -> xgb.Booster:
    """
    Train XGBoost model

    Args:
        X_train: DataFrame with training features
        y_train: List of binary labels
        max_depth: Maximum tree depth
        eta: Learning rate
        num_boost_round: Number of boosting rounds
        seed: Random seed

    Returns:
        Trained XGBoost model
    """
    print(f"Training with {len(X_train.columns)} features: {list(X_train.columns)}")

    # Train XGBoost model
    params = {
        'max_depth': max_depth,
        'eta': eta,
        'objective': 'binary:logistic',
        'eval_metric': 'logloss',
        'seed': seed
    }

    dtrain = xgb.DMatrix(X_train.values, label=y_train)
    print(f"Training model with params: {params}")

    model = xgb.train(params, dtrain, num_boost_round=num_boost_round)

    print(f"Training complete!")

    return model


def save_model(model: xgb.Booster, output_path: str):
    """Save model in Vertex AI compatible format"""
    import joblib

    # GCSFuse conversion for Vertex AI
    # When running in Vertex AI, gs:// paths are mounted at /gcs/
    gs_prefix = 'gs://'
    gcsfuse_prefix = '/gcs/'
    if output_path.startswith(gs_prefix):
        output_path = output_path.replace(gs_prefix, gcsfuse_prefix)

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save XGBoost model using joblib format
    model_file = output_dir / "model.joblib"
    joblib.dump(model, str(model_file))
    print(f"Model saved to {model_file}")

    # Save model metadata
    metadata = {
        'num_features': 4,
        'feature_names': ['user_id_click_event_average_7d', 'listing_price_cents', 'price_log', 'price_bucket'],
        'model_type': 'xgboost',
        'version': '1.0'
    }

    metadata_file = output_dir / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"Metadata saved to {metadata_file}")

class TrainingCLIParser(argparse.ArgumentParser):
    """
    A base argparse parser for training scripts to inherit from.
    Automatically adds common arguments.
    """

    def __init__(self, description: str = "Model Training Script", *args, **kwargs):
        INPUT_TABLE_KEYWORD = "--input-table"
        START_DS_KEYWORD = "--start-ds"
        END_DS_KEYWORD = "--end-ds"

        super().__init__(description=description, *args, **kwargs)
        self.add_argument(
            INPUT_TABLE_KEYWORD,
            type=str,
            help="Table containing features",
        )

        self.add_argument(
            START_DS_KEYWORD,
            type=str,
            help="Start partition date (yyyy-MM-dd) for training data",
        )

        self.add_argument(
            END_DS_KEYWORD,
            type=str,
            help="End partition date (yyyy-MM-dd) for training data",
        )


def main():

    parser = TrainingCLIParser(description='Train XGBoost CTR model')

    # Model hyperparameters
    parser.add_argument('--max-depth', type=int, default=4,
                        help='Maximum tree depth')
    parser.add_argument('--eta', type=float, default=0.1,
                        help='Learning rate')
    parser.add_argument('--num-boost-round', type=int, default=50,
                        help='Number of boosting rounds')

    # Output parameters
    parser.add_argument('--model-dir', type=str,
                        default=os.environ.get('AIP_MODEL_DIR', './model_output'),
                        help='Directory to save the model (defaults to AIP_MODEL_DIR for Vertex AI)')

    args = parser.parse_args()

    print("="*80)
    print("XGBoost CTR Model Training")
    print("="*80)
    print(f"Configuration:")
    print(f"  Max depth: {args.max_depth}")
    print(f"  Learning rate: {args.eta}")
    print(f"  Boosting rounds: {args.num_boost_round}")
    print(f"  Model output: {args.model_dir}")
    print("="*80)

    X_train, y_train = get_training_data_df(args.input_table, args.start_ds, args.end_ds)
    # Train model
    print("\n2. Training model...")
    model = train_model(
        X_train,
        y_train,
        max_depth=args.max_depth,
        eta=args.eta,
        num_boost_round=args.num_boost_round,
    )

    # Save model
    print("\n3. Saving model...")
    save_model(model, args.model_dir)

    # Quick validation
    print("\n4. Validation...")
    X_test = X_train.iloc[[0]]
    dmatrix = xgb.DMatrix(X_test)
    pred = model.predict(dmatrix)[0]
    print(f"   Sample prediction: {pred:.4f}")
    print(f"   Actual label: {y_train[0]}")

    print("\n" + "="*80)
    print("Training complete!")
    print("="*80)


if __name__ == "__main__":
    main()
