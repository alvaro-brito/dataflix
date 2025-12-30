#!/usr/bin/env python3
"""
Training Server - Webhook to trigger model training
Exposes /train endpoint that executes train_model.py
"""

import logging
import subprocess
import sys
from flask import Flask, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})


@app.route('/train', methods=['POST'])
def train():
    """Endpoint to trigger model training"""
    logger.info("Received training request - executing train_model.py...")

    try:
        # Execute train_model.py script
        result = subprocess.run(
            [sys.executable, '/app/train_model.py'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )

        if result.returncode == 0:
            logger.info("Training completed successfully")
            logger.info(f"Output: {result.stdout[-1000:]}")  # Last 1000 chars
            return jsonify({
                "status": "success",
                "message": "Model trained successfully",
                "output": result.stdout[-500:]
            }), 200
        else:
            logger.error(f"Training failed: {result.stderr}")
            return jsonify({
                "status": "error",
                "message": "Training failed",
                "error": result.stderr[-500:]
            }), 500

    except subprocess.TimeoutExpired:
        logger.error("Training timeout exceeded")
        return jsonify({
            "status": "error",
            "message": "Training timeout exceeded (5 minutes)"
        }), 500
    except Exception as e:
        logger.error(f"Error executing training: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == '__main__':
    logger.info("Starting Training Server on port 5001...")
    app.run(host='0.0.0.0', port=5001, debug=False)
