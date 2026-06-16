FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

# Create data directory
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# Install system dependencies for asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/

# Set permissions
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

VOLUME ["/app/data"]

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=3).read()" || exit 1

CMD ["python", "-m", "src"]
