#!/bin/bash

# Run database migrations/table creation on startup
# This will execute the on_startup event in main.py
# which calls Base.metadata.create_all()

# Start Uvicorn with your FastAPI app
# main:app means "look for the 'app' object in the 'main.py' file"
uvicorn main:app --host 0.0.0.0 --port $PORT
