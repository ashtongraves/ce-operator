FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the controller code
COPY controller.py .

# Create non-root user
RUN useradd --create-home --shell /bin/bash controller
USER controller

# Run the controller
CMD ["kopf", "run", "/app/controller.py", "--verbose"]