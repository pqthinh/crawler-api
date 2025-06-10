# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install system dependencies if any
# RUN apt-get update && apt-get install -y ... && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY ./src /app/src

# The command to run Celery Worker will be set in docker-compose.yml
# Example: CMD ["celery", "-A", "src.tasks", "worker", "-l", "info"] 