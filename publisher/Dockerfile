FROM python:3.11-slim

RUN useradd -m -u 1000 app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
USER app

CMD ["python", "app.py"]
