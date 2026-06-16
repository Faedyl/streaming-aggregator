FROM python:3.11-slim
WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY publisher.py ./
CMD ["python", "publisher.py"]
