FROM python:3.10-slim

WORKDIR /app


COPY . /app
COPY requirements.txt .
COPY test_integration_heartbeat.py /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Entry point for running tests
CMD ["pytest", "test_integration_heartbeat.py", "-v"]
