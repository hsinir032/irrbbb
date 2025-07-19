#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# This script is executed by Render to start your FastAPI application.

# Run database migrations (optional, but good practice for production)
# For now, we rely on Base.metadata.create_all(bind=engine) in main.py
# In a real production app, you'd use Alembic or similar for migrations.

# Start Uvicorn server
# --host 0.0.0.0 is important for Render, as it listens on all interfaces
# --port $PORT uses the port provided by Render's environment variable
# Redirect stderr to stdout so all output goes to Render logs
uvicorn main:app --host 0.0.0.0 --port "$PORT" --log-level debug 2>&1
