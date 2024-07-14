FROM python:3.11-slim

WORKDIR /app

# Install build tools
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    build-essential \
    libpq-dev \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
