# Use an official Python runtime as a parent image
# Changed from buster to bullseye as buster repositories are archived
FROM python:3.10-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Install system dependencies needed for psycopg2-binary
# Ensure apt-get update is successful and use --no-install-recommends for smaller image
RUN apt-get update --fix-missing && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the working directory
COPY . .

# Make the start script executable
RUN chmod +x ./start.sh

# Command to run the application when the container starts
# This explicitly tells Docker to execute start.sh
CMD ["./start.sh"]
