FROM python:3.11-slim

# Set environment variable to avoid debconf errors
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8080

# Run the application
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:8080 --workers 4 --timeout ${GUNICORN_TIMEOUT:-300} --keep-alive 2 --max-requests 1000 --max-requests-jitter 50 wsgi:app"]