#!/usr/bin/env python3
"""
Dataflix Backend API - Flask
Manages users, movies, ratings, and recommendations
"""

import os
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc
import mlflow
from mlflow.tracking import MlflowClient
import numpy as np
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache for model and artifacts
MODEL_CACHE = {
    'run_id': None,
    'model': None,
    'W': None,
    'metadata': None,
    'last_checked': 0
}

# Flask app
app = Flask(__name__)
CORS(app)

# Configurations
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-source')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'dataflix')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'dataflix123')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'dataflix_db')
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow-server:5000')

# SQLAlchemy Config
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# ============================================================================
# Models
# ============================================================================

class User(db.Model):
    __tablename__ = 'users'
    user_id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    city = db.Column(db.String(50))
    state = db.Column(db.String(2))
    country = db.Column(db.String(50), default='Brazil')
    age = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'username': self.username,
            'email': self.email,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'city': self.city,
            'state': self.state,
            'country': self.country,
            'age': self.age
        }

class Movie(db.Model):
    __tablename__ = 'movies'
    movie_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    genre = db.Column(db.String(100))
    release_year = db.Column(db.Integer)
    director = db.Column(db.String(255))
    duration_minutes = db.Column(db.Integer)
    imdb_rating = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'movie_id': self.movie_id,
            'title': self.title,
            'description': self.description,
            'genre': self.genre,
            'release_year': self.release_year,
            'director': self.director,
            'duration_minutes': self.duration_minutes,
            'imdb_rating': self.imdb_rating
        }

class WatchedMovie(db.Model):
    __tablename__ = 'watched_movies'
    watched_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)
    movie_id = db.Column(db.Integer, db.ForeignKey('movies.movie_id'), nullable=False)
    watched_at = db.Column(db.DateTime, default=datetime.utcnow)

class Rating(db.Model):
    __tablename__ = 'ratings'
    rating_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)
    movie_id = db.Column(db.Integer, db.ForeignKey('movies.movie_id'), nullable=False)
    rating = db.Column(db.Float)
    liked = db.Column(db.Boolean, default=False)
    rated_at = db.Column(db.DateTime, default=datetime.utcnow)

# ============================================================================
# Health Check
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'dataflix-backend',
        'timestamp': datetime.now().isoformat()
    }), 200

# ============================================================================
# Users Endpoints
# ============================================================================

@app.route('/users', methods=['GET'])
def get_users():
    """List all users"""
    try:
        users = User.query.order_by(User.user_id).all()
        return jsonify({
            'status': 'success',
            'data': [u.to_dict() for u in users],
            'count': len(users)
        }), 200
    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    """Get user by ID"""
    try:
        user = User.query.get(user_id)
        if not user:
            return jsonify({'status': 'error', 'message': 'User not found'}), 404
        
        return jsonify({
            'status': 'success',
            'data': user.to_dict()
        }), 200
    except Exception as e:
        logger.error(f"Error fetching user: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/users', methods=['POST'])
def create_user():
    """Create new user"""
    try:
        data = request.get_json()
        
        required_fields = ['username', 'email', 'first_name', 'last_name', 'city', 'state']
        if not all(field in data for field in required_fields):
            return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400
        
        # Check if exists
        if User.query.filter((User.username == data['username']) | (User.email == data['email'])).first():
            return jsonify({'status': 'error', 'message': 'Username or email already exists'}), 400

        user = User(
            username=data['username'],
            email=data['email'],
            first_name=data['first_name'],
            last_name=data['last_name'],
            city=data['city'],
            state=data['state'],
            country=data.get('country', 'Brazil'),
            age=data.get('age', 0)
        )
        
        db.session.add(user)
        db.session.commit()
        
        logger.info(f"User created: {user.user_id}")
        
        return jsonify({
            'status': 'success',
            'message': 'User created successfully',
            'data': user.to_dict()
        }), 201
    
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ============================================================================
# Movies Endpoints
# ============================================================================

@app.route('/movies', methods=['GET'])
def get_movies():
    """List movies with optional filters"""
    try:
        genre = request.args.get('genre')
        limit = request.args.get('limit', 20, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        query = Movie.query
        if genre:
            query = query.filter_by(genre=genre)
            
        movies = query.order_by(Movie.imdb_rating.desc()).limit(limit).offset(offset).all()
        
        return jsonify({
            'status': 'success',
            'data': [m.to_dict() for m in movies],
            'count': len(movies)
        }), 200
    
    except Exception as e:
        logger.error(f"Error fetching movies: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie(movie_id):
    """Get movie by ID"""
    try:
        movie = Movie.query.get(movie_id)
        if not movie:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        
        return jsonify({
            'status': 'success',
            'data': movie.to_dict()
        }), 200
    except Exception as e:
        logger.error(f"Error fetching movie: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ============================================================================
# Watched Movies Endpoints
# ============================================================================

@app.route('/watched', methods=['POST'])
def mark_watched():
    """Mark movie as watched"""
    try:
        data = request.get_json()
        
        if 'user_id' not in data or 'movie_id' not in data:
            return jsonify({'status': 'error', 'message': 'Missing user_id or movie_id'}), 400
        
        # Check if already watched
        watched = WatchedMovie.query.filter_by(user_id=data['user_id'], movie_id=data['movie_id']).first()
        if watched:
            return jsonify({
                'status': 'success',
                'message': 'Movie already marked as watched'
            }), 200
            
        watched = WatchedMovie(user_id=data['user_id'], movie_id=data['movie_id'])
        db.session.add(watched)
        db.session.commit()
        
        logger.info(f"Movie {data['movie_id']} marked as watched by user {data['user_id']}")
        return jsonify({
            'status': 'success',
            'message': 'Movie marked as watched'
        }), 201
    
    except Exception as e:
        logger.error(f"Error marking watched: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/watched/<int:user_id>', methods=['GET'])
def get_watched_movies(user_id):
    """Get movies watched by a user"""
    try:
        # Join WatchedMovie and Movie
        results = db.session.query(Movie).join(WatchedMovie).filter(WatchedMovie.user_id == user_id).order_by(WatchedMovie.watched_at.desc()).all()
        
        return jsonify({
            'status': 'success',
            'data': [m.to_dict() for m in results],
            'count': len(results)
        }), 200
    except Exception as e:
        logger.error(f"Error fetching watched movies: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ============================================================================
# Ratings Endpoints
# ============================================================================

@app.route('/ratings', methods=['POST'])
def rate_movie():
    """Register movie rating/like"""
    try:
        data = request.get_json()
        
        required_fields = ['user_id', 'movie_id', 'rating']
        if not all(field in data for field in required_fields):
            return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400
        
        rating_val = data['rating']
        if not (1 <= rating_val <= 5):
            return jsonify({'status': 'error', 'message': 'Rating must be between 1 and 5'}), 400
        
        # Upsert logic manually
        existing = Rating.query.filter_by(user_id=data['user_id'], movie_id=data['movie_id']).first()
        liked = data.get('liked', rating_val >= 4)
        
        if existing:
            existing.rating = rating_val
            existing.liked = liked
            existing.rated_at = datetime.utcnow()
        else:
            rating = Rating(
                user_id=data['user_id'],
                movie_id=data['movie_id'],
                rating=rating_val,
                liked=liked
            )
            db.session.add(rating)
            
        db.session.commit()
        
        logger.info(f"Rating {rating_val} registered for movie {data['movie_id']} by user {data['user_id']}")
        
        return jsonify({
            'status': 'success',
            'message': 'Rating registered successfully'
        }), 201
    
    except Exception as e:
        logger.error(f"Error registering rating: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/ratings/<int:user_id>', methods=['GET'])
def get_user_ratings(user_id):
    """Get user ratings"""
    try:
        # Join Rating and Movie
        results = db.session.query(Rating, Movie).join(Movie).filter(Rating.user_id == user_id).order_by(Rating.rated_at.desc()).all()
        
        data = []
        for r, m in results:
            item = {
                'user_id': r.user_id,
                'movie_id': r.movie_id,
                'rating': r.rating,
                'liked': r.liked,
                'rated_at': r.rated_at,
                'title': m.title,
                'genre': m.genre
            }
            data.append(item)
            
        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data)
        }), 200
    except Exception as e:
        logger.error(f"Error fetching ratings: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ============================================================================
# Recommendations Endpoint
# ============================================================================

def get_latest_model():
    """Load latest model from MLflow with cache"""
    global MODEL_CACHE
    
    # Check if we need to update (5 minutes cache)
    current_time = datetime.now().timestamp()
    if MODEL_CACHE['model'] and (current_time - MODEL_CACHE['last_checked'] < 300):
        return MODEL_CACHE

    try:
        client = MlflowClient(MLFLOW_TRACKING_URI)
        
        # Search experiment
        experiments = client.search_experiments()
        experiment_id = None
        for exp in experiments:
            if 'dataflix' in exp.name.lower():
                experiment_id = exp.experiment_id
                break
        
        if not experiment_id:
            logger.warning("Experiment not found")
            return None
            
        # Search finished runs
        runs = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string="status = 'FINISHED'",
            order_by=["start_time DESC"],
            max_results=1
        )
        
        if not runs:
            logger.warning("No finished runs found")
            return None
            
        latest_run = runs[0]
        run_id = latest_run.info.run_id
        
        # If run changed, reload everything
        if run_id != MODEL_CACHE['run_id']:
            logger.info(f"Loading new model version from run {run_id}")
            
            # 1. Load Model (H matrix is in model.components_)
            model_uri = f"runs:/{run_id}/nmf_model"
            model = mlflow.sklearn.load_model(model_uri)
            
            # 2. Download and Load W matrix (User Features)
            local_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="user_features.npy")
            W = np.load(local_path)
            
            # 3. Download and Load Metadata
            local_meta_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="metadata.json")
            with open(local_meta_path, 'r') as f:
                metadata = json.load(f)
            
            # Update cache
            MODEL_CACHE = {
                'run_id': run_id,
                'model': model,
                'W': W,
                'metadata': metadata,
                'last_checked': current_time
            }
        else:
            MODEL_CACHE['last_checked'] = current_time
            
        return MODEL_CACHE

    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        return None

@app.route('/recommendations/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    """Get recommendations for a user using Collaborative Filtering"""
    try:
        n_recommendations = request.args.get('limit', 5, type=int)
        
        # Try to load model
        model_data = get_latest_model()
        
        # Recommendation Strategy
        recommendations = []
        source = "fallback_sql"
        
        if model_data and model_data['model']:
            # Try ML inference
            try:
                metadata = model_data['metadata']
                user_ids = metadata['user_ids']
                
                # Check if user exists in model (not cold start)
                if user_id in user_ids:
                    logger.info(f"Generating ML recommendations for user {user_id}")
                    
                    # Get indices
                    user_idx = user_ids.index(user_id)
                    movie_ids = metadata['movie_ids']
                    
                    # Get matrices
                    W = model_data['W']
                    H = model_data['model'].components_
                    
                    # Calculate scores: user_features (1xK) dot product movie_features (KxM)
                    user_features = W[user_idx]
                    scores = np.dot(user_features, H)
                    
                    # Identify watched movies to filter
                    watched_db = set(r.movie_id for r in WatchedMovie.query.filter_by(user_id=user_id).all())
                    
                    # Create list of (movie_id, score)
                    scored_movies = []
                    for idx, score in enumerate(scores):
                        m_id = movie_ids[idx]
                        if m_id not in watched_db:
                            scored_movies.append((m_id, float(score)))

                    # Sort and get top N
                    scored_movies.sort(key=lambda x: x[1], reverse=True)
                    top_movies = scored_movies[:n_recommendations]

                    # Normalize scores to 0-5 scale (better visualization)
                    if top_movies:
                        max_score = max(s for _, s in top_movies) if top_movies else 1
                        if max_score > 0:
                            top_movies = [(m_id, (score / max_score) * 5.0) for m_id, score in top_movies]
                    
                    # Fetch movie details from DB
                    if top_movies:
                        top_ids = [m[0] for m in top_movies]
                        
                        # SQLAlchemy IN clause
                        movies_list = Movie.query.filter(Movie.movie_id.in_(top_ids)).all()
                        movies_details = {m.movie_id: m.to_dict() for m in movies_list}
                        
                        # Build final response sorted
                        for m_id, score in top_movies:
                            if m_id in movies_details:
                                movie = movies_details[m_id]
                                movie['score'] = score  # Add prediction score
                                recommendations.append(movie)
                        
                        source = "ml_model_nmf"
            
            except Exception as ml_error:
                logger.error(f"ML inference failed, falling back to SQL: {str(ml_error)}")
                recommendations = []
        
        # Fallback: If ML failed or returned nothing (cold start), use SQL
        if not recommendations:
            logger.info(f"Using SQL fallback for user {user_id}")
            
            # Subquery for watched movies
            watched_subquery = db.session.query(WatchedMovie.movie_id).filter_by(user_id=user_id).subquery()
            
            # Query best rated movies
            results = db.session.query(Movie, func.coalesce(func.avg(Rating.rating), 0).label('avg_rating'))\
                .outerjoin(Rating, Movie.movie_id == Rating.movie_id)\
                .filter(~Movie.movie_id.in_(watched_subquery))\
                .group_by(Movie.movie_id)\
                .order_by(desc('avg_rating'), Movie.imdb_rating.desc())\
                .limit(n_recommendations)\
                .all()
                
            for m, avg_rating in results:
                movie_dict = m.to_dict()
                movie_dict['score'] = float(avg_rating)
                recommendations.append(movie_dict)
                
        return jsonify({
            'status': 'success',
            'data': recommendations,
            'source': source,
            'count': len(recommendations)
        }), 200

    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
