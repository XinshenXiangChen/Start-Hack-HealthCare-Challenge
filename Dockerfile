FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Build deps for packages with native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    grep \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Root requirements includes: -r pipeline/requirements.txt
# Copy it before `pip install` so the relative include resolves correctly.
COPY pipeline/requirements.txt ./pipeline/requirements.txt

# Install dependencies (skip Windows-only pywin32)
RUN python -m pip install --no-cache-dir --upgrade pip \
    && grep -vE '^(pywin32|pywin32==)' requirements.txt > reqs.linux.txt \
    && python -m pip install --no-cache-dir -r reqs.linux.txt

COPY . .

RUN mkdir -p \
    runtime/incoming \
    runtime/processed \
    runtime/standardized \
    runtime/linked \
    && python -m py_compile pipeline.py dashboard/app.py pipeline/standardize.py

EXPOSE 8000

# Run the FastAPI dashboard server (it also starts the watcher/worker loops on startup).
# Bind inside the container to all interfaces; the compose port mapping controls exposure on your host.
CMD ["python", "pipeline.py", "dashboard", "--host", "0.0.0.0", "--port", "8000"]