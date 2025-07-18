#!/usr/bin/env bash

# This script is executed by Render to start your FastAPI application.

# Run database migrations (optional, but good practice for production)
# For now, we rely on Base.metadata.create_all(bind=engine) in main.py
# In a real production app, you'd use Alembic or similar for migrations.

# Start Uvicorn server
# --host 0.0.0.0 is important for Render, as it listens on all interfaces
# --port $PORT uses the port provided by Render's environment variable
uvicorn main:app --host 0.0.0.0 --port $PORT
