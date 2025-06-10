# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install system dependencies if any (e.g., for mysql-connector)
# RUN apt-get update && apt-get install -y default-libmysqlclient-dev gcc && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY ./src /app/src

# The command to run Celery Beat will be set in docker-compose.yml
# or defined here if it's static.
# Example: CMD ["celery", "-A", "src.app", "beat", "-l", "info", "--scheduler", "celery.beat.PersistentScheduler"] 