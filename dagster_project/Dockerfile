FROM python:3.11-slim

# Install system dependencies if needed (e.g., git for dbt)
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables to ensure Python output is logged immediately
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/app
