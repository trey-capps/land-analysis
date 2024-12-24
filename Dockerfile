# Use Python slim base image
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    libpq-dev \
    && apt-get clean

# Upgrade pip
RUN pip install --upgrade pip

# Set working directory
WORKDIR /workspace

# Copy requirements file and install dependencies
COPY requirements.txt /workspace/requirements.txt
RUN pip install -r /workspace/requirements.txt