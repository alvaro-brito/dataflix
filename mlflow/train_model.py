#!/usr/bin/env python3
"""
MLflow Model Training Script - Collaborative Filtering
Trains a matrix factorization model using data from ClickHouse
"""

import os
import logging
import numpy as np
import pandas as pd
from sklearn.decomposition import NMF
from clickhouse_driver import Client
import mlflow
import mlflow.sklearn
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurations
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse-server')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')


def fetch_data_from_clickhouse():
    """Fetch user-movie matrix data from ClickHouse"""
    logger.info("Fetching data from ClickHouse...")

    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=9000,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )

        # Query to fetch interaction matrix
        query = """
            SELECT user_id, movie_id, interaction_score
            FROM analytics.mart_user_movie_matrix
            ORDER BY user_id, movie_id
        """

        results = client.execute(query)

        if not results:
            logger.warning("No data found in ClickHouse")
            return None

        logger.info(f"Fetched {len(results)} interactions")

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=['user_id', 'movie_id', 'interaction_score'])

        return df

    except Exception as e:
        logger.error(f"Error fetching data from ClickHouse: {str(e)}")
        raise


def create_user_movie_matrix(df):
    """Create user-movie matrix from data"""
    logger.info("Creating user-movie matrix...")

    # Create pivot table
    matrix = df.pivot_table(
        index='user_id',
        columns='movie_id',
        values='interaction_score',
        fill_value=0
    )

    logger.info(f"Matrix shape: {matrix.shape}")
    sparsity = (matrix == 0).sum().sum() / (matrix.shape[0] * matrix.shape[1]) * 100
    logger.info(f"Matrix sparsity: {sparsity:.2f}%")

    return matrix


def train_model(matrix):
    """Train matrix factorization model"""
    logger.info("Training collaborative filtering model...")

    # Configure MLflow
    logger.info(f"MLflow Tracking URI: {MLFLOW_TRACKING_URI}")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Create/select experiment
    experiment_name = 'dataflix-collaborative-filtering'
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = mlflow.create_experiment(experiment_name)
        logger.info(f"Created experiment: {experiment_name} (ID: {experiment_id})")
    else:
        experiment_id = experiment.experiment_id
        logger.info(f"Using existing experiment: {experiment_name} (ID: {experiment_id})")

    mlflow.set_experiment(experiment_name)

    # Start MLflow run
    with mlflow.start_run(run_name=f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        run_id = mlflow.active_run().info.run_id
        logger.info(f"Started MLflow run: {run_id}")

        # Hyperparameters
        n_components = min(10, min(matrix.shape) - 1)  # Ensure it doesn't exceed dimensions
        max_iter = 200
        random_state = 42

        # Log parameters
        mlflow.log_param('n_components', n_components)
        mlflow.log_param('init', 'random')
        mlflow.log_param('max_iter', max_iter)
        mlflow.log_param('algorithm', 'NMF')
        mlflow.log_param('matrix_shape', f"{matrix.shape[0]}x{matrix.shape[1]}")
        mlflow.log_param('num_users', matrix.shape[0])
        mlflow.log_param('num_movies', matrix.shape[1])

        # Train NMF model
        logger.info(f"Training NMF model with {n_components} components...")
        model = NMF(
            n_components=n_components,
            init='random',
            max_iter=max_iter,
            random_state=random_state
        )

        W = model.fit_transform(matrix.values)  # User-feature matrix
        H = model.components_  # Feature-movie matrix

        # Calculate metrics
        reconstructed = W @ H
        rmse = np.sqrt(np.mean((matrix.values - reconstructed) ** 2))
        mae = np.mean(np.abs(matrix.values - reconstructed))
        sparsity = (matrix == 0).sum().sum() / (matrix.shape[0] * matrix.shape[1])

        logger.info(f"RMSE: {rmse:.4f}")
        logger.info(f"MAE: {mae:.4f}")

        # Log metrics
        mlflow.log_metric('rmse', rmse)
        mlflow.log_metric('mae', mae)
        mlflow.log_metric('sparsity', sparsity)
        mlflow.log_metric('reconstruction_error', model.reconstruction_err_)

        # Save model
        mlflow.sklearn.log_model(model, 'nmf_model')
        logger.info("Model logged to MLflow")

        # Save matrices as artifacts
        np.save('/tmp/user_features.npy', W)
        np.save('/tmp/movie_features.npy', H)
        mlflow.log_artifact('/tmp/user_features.npy')
        mlflow.log_artifact('/tmp/movie_features.npy')
        os.remove('/tmp/user_features.npy')
        os.remove('/tmp/movie_features.npy')

        # Save metadata
        metadata = {
            'n_components': n_components,
            'user_ids': [int(x) for x in matrix.index.tolist()],
            'movie_ids': [int(x) for x in matrix.columns.tolist()],
            'training_date': datetime.now().isoformat(),
            'rmse': float(rmse),
            'mae': float(mae),
            'num_users': int(matrix.shape[0]),
            'num_movies': int(matrix.shape[1])
        }

        mlflow.log_dict(metadata, 'metadata.json')

        logger.info(f"Model training completed successfully. Run ID: {run_id}")

        return model, W, H, matrix, run_id


def generate_recommendations(model, W, H, matrix, user_id, n_recommendations=5):
    """Generate recommendations for a user"""

    try:
        # Get user index
        user_idx = matrix.index.get_loc(user_id)

        # Get user features
        user_features = W[user_idx]

        # Calculate scores for all movies
        scores = user_features @ H

        # Movies already watched
        watched_movies = set(matrix.columns[matrix.iloc[user_idx] > 0])

        # Sort by score and filter unwatched movies
        recommendations = []
        for movie_idx, score in enumerate(scores):
            movie_id = matrix.columns[movie_idx]
            if movie_id not in watched_movies:
                recommendations.append((int(movie_id), float(score)))

        # Sort by descending score
        recommendations.sort(key=lambda x: x[1], reverse=True)

        return recommendations[:n_recommendations]

    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        return []


def main():
    """Main function"""
    try:
        logger.info("=" * 60)
        logger.info("Starting model training pipeline...")
        logger.info("=" * 60)

        # Fetch data
        df = fetch_data_from_clickhouse()

        if df is None or df.empty:
            logger.warning("No data available for training")
            return {"status": "error", "message": "No data available"}

        # Create matrix
        matrix = create_user_movie_matrix(df)

        # Train model
        model, W, H, matrix, run_id = train_model(matrix)

        # Generate some sample recommendations
        logger.info("Generating sample recommendations...")

        sample_users = matrix.index[:3].tolist()  # First 3 users

        for user_id in sample_users:
            recommendations = generate_recommendations(model, W, H, matrix, user_id, n_recommendations=5)
            logger.info(f"Recommendations for user {user_id}: {recommendations}")

        logger.info("=" * 60)
        logger.info("Model training pipeline completed successfully")
        logger.info(f"MLflow Run ID: {run_id}")
        logger.info("=" * 60)

        return {"status": "success", "run_id": run_id}

    except Exception as e:
        logger.error(f"Error in training pipeline: {str(e)}")
        raise


if __name__ == '__main__':
    main()
